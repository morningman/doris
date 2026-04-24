// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.service;

import org.apache.doris.connector.credential.BeDispatchableCredential;
import org.apache.doris.connector.credential.Credential;
import org.apache.doris.connector.credential.DefaultCredentialBroker;
import org.apache.doris.connector.credential.DefaultCredentialBrokerFactory;
import org.apache.doris.thrift.TConnectorCredential;
import org.apache.doris.thrift.TRefreshCredentialRequest;
import org.apache.doris.thrift.TRefreshCredentialResult;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * Process-wide handler for the BE -&gt; FE {@code refreshCredential} RPC
 * (M1-02). Owns one {@link DefaultCredentialBroker} keyed by an opaque
 * "global" name (or by the request's optional catalog hint) and applies an
 * additional FE-side single-flight fan-out so multiple BEs hammering the same
 * credential within a short window only trigger one resolver call.
 *
 * <p>The single-flight is keyed on {@code (catalog, scheme, ref, scope)};
 * concurrent callers attach to the same {@link CompletableFuture}. Once that
 * future completes (success or failure) it is removed so the next request
 * starts a fresh resolution.</p>
 *
 * <p>Audit logging redacts the secret. The raw credential bytes never appear
 * in any log line emitted by this class.</p>
 */
public final class RefreshCredentialService {

    private static final Logger LOG = LogManager.getLogger(RefreshCredentialService.class);
    private static final String DEFAULT_CATALOG = "_global_";

    private static final RefreshCredentialService INSTANCE = new RefreshCredentialService();

    public static RefreshCredentialService getInstance() {
        return INSTANCE;
    }

    private final ConcurrentHashMap<String, DefaultCredentialBroker> brokersByCatalog
            = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<InflightKey, CompletableFuture<Credential>> inflight
            = new ConcurrentHashMap<>();

    private RefreshCredentialService() {
    }

    /** Test/diagnostics injection: replace the broker for a given catalog. */
    public void registerBrokerForTest(String catalog, DefaultCredentialBroker broker) {
        brokersByCatalog.put(catalog == null || catalog.isEmpty() ? DEFAULT_CATALOG : catalog,
                Objects.requireNonNull(broker, "broker"));
    }

    /** Test/diagnostics: drop all cached brokers and inflight singleflights. */
    public void resetForTest() {
        brokersByCatalog.clear();
        inflight.clear();
    }

    public TRefreshCredentialResult refresh(TRefreshCredentialRequest request) {
        TRefreshCredentialResult result = new TRefreshCredentialResult();
        if (request == null || !request.isSetScheme() || !request.isSetRef()) {
            result.setStatus(status(TStatusCode.INVALID_ARGUMENT,
                    "scheme and ref are required"));
            return result;
        }
        String scheme = request.getScheme();
        String ref = request.getRef();
        String scopeStr = request.isSetScope() ? request.getScope() : BeDispatchableCredential.Scope.CATALOG.name();
        String catalog = request.isSetCatalog() && !request.getCatalog().isEmpty()
                ? request.getCatalog() : DEFAULT_CATALOG;
        String requestor = request.isSetRequestorId() ? request.getRequestorId() : "unknown";

        URI uri;
        try {
            uri = new URI(ref);
        } catch (Exception e) {
            result.setStatus(status(TStatusCode.INVALID_ARGUMENT,
                    "malformed credential ref: " + e.getMessage()));
            return result;
        }
        if (uri.getScheme() == null || !uri.getScheme().equalsIgnoreCase(scheme)) {
            result.setStatus(status(TStatusCode.INVALID_ARGUMENT,
                    "scheme '" + scheme + "' does not match ref '" + ref + "'"));
            return result;
        }
        BeDispatchableCredential.Scope scope;
        try {
            scope = BeDispatchableCredential.Scope.valueOf(scopeStr);
        } catch (IllegalArgumentException e) {
            result.setStatus(status(TStatusCode.INVALID_ARGUMENT,
                    "unknown scope: " + scopeStr));
            return result;
        }

        // Audit log: secret is never resolved yet, so cannot leak. Still avoid
        // logging the ref query if the scheme is opaque (we log only scheme + ref host).
        LOG.info("refreshCredential rpc: requestor={}, catalog={}, scheme={}, scope={}, ref_host={}",
                requestor, catalog, scheme, scope, uri.getHost());

        InflightKey key = new InflightKey(catalog, ref, scope);
        // Two-phase single-flight: the first caller registers an incomplete
        // future and then resolves outside the bucket lock; concurrent callers
        // attach to the same future and await it. The future is removed from
        // the map only after the registering thread completes the resolution.
        boolean[] isOwner = new boolean[1];
        CompletableFuture<Credential> future = inflight.computeIfAbsent(key, k -> {
            isOwner[0] = true;
            return new CompletableFuture<>();
        });
        if (isOwner[0]) {
            try {
                DefaultCredentialBroker broker = brokerFor(catalog);
                Credential c = broker.refresh(uri);
                future.complete(c);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            } finally {
                inflight.remove(key, future);
            }
        }

        Credential resolved;
        try {
            resolved = future.get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            result.setStatus(status(TStatusCode.CANCELLED, "interrupted"));
            return result;
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause() != null ? ee.getCause() : ee;
            LOG.warn("refreshCredential failed: scheme={}, ref_host={}, err={}",
                    scheme, uri.getHost(), cause.toString());
            result.setStatus(status(TStatusCode.INTERNAL_ERROR,
                    "credential resolve failed: " + cause.getMessage()));
            return result;
        }

        BeDispatchableCredential dispatch = new BeDispatchableCredential(scheme, resolved, scope);
        TConnectorCredential thrift = dispatch.toThrift(uri);
        result.setStatus(status(TStatusCode.OK, null));
        result.setCredential(thrift);
        return result;
    }

    private DefaultCredentialBroker brokerFor(String catalog) {
        return brokersByCatalog.computeIfAbsent(catalog,
                DefaultCredentialBrokerFactory::forCatalog);
    }

    private static TStatus status(TStatusCode code, String msg) {
        TStatus s = new TStatus(code);
        if (msg != null) {
            s.setErrorMsgs(Collections.singletonList(msg));
        }
        return s;
    }

    private static final class InflightKey {
        final String catalog;
        final String ref;
        final BeDispatchableCredential.Scope scope;

        InflightKey(String catalog, String ref, BeDispatchableCredential.Scope scope) {
            this.catalog = catalog;
            this.ref = ref;
            this.scope = scope;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof InflightKey)) {
                return false;
            }
            InflightKey that = (InflightKey) o;
            return catalog.equals(that.catalog) && ref.equals(that.ref) && scope == that.scope;
        }

        @Override
        public int hashCode() {
            return Objects.hash(catalog, ref, scope);
        }
    }
}
