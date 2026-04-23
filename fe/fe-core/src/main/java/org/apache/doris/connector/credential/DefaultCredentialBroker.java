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

package org.apache.doris.connector.credential;

import org.apache.doris.connector.api.credential.CredentialBroker;
import org.apache.doris.connector.api.credential.CredentialEnvelope;
import org.apache.doris.connector.api.credential.CredentialScope;
import org.apache.doris.connector.api.credential.HttpCredentialOps;
import org.apache.doris.connector.api.credential.HttpEndpoint;
import org.apache.doris.connector.api.credential.HttpRequestSpec;
import org.apache.doris.connector.api.credential.JdbcCredentialOps;
import org.apache.doris.connector.api.credential.JdbcRequest;
import org.apache.doris.connector.api.credential.MetastoreCredentialOps;
import org.apache.doris.connector.api.credential.MetastorePrincipal;
import org.apache.doris.connector.api.credential.RuntimeImpersonationOps;
import org.apache.doris.connector.api.credential.StorageCredentialOps;
import org.apache.doris.connector.api.credential.StoragePath;
import org.apache.doris.connector.api.credential.StorageRequest;
import org.apache.doris.connector.api.credential.ThrowingSupplier;
import org.apache.doris.connector.api.credential.UserContext;

import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * fe-core default {@link CredentialBroker}.
 *
 * <p>Holds an ordered chain of {@link CredentialResolver}s keyed by URI scheme
 * (e.g. {@code env}, {@code file}, {@code kms}, {@code vault}). Resolved
 * {@link Credential}s are cached per-URI; repeated lookups are served from the
 * cache until either {@link Credential#expiresAt()} elapses or the per-broker
 * default TTL ({@link ResolverContext#defaultTtlSeconds()}) expires. Concurrent
 * callers asking for the same URI share a single load (single-flight via
 * {@code computeIfAbsent} on a per-key lock).</p>
 *
 * <p>The five sub-ops accessors required by {@link CredentialBroker} are
 * provided; storage / metastore / jdbc / http remain as stubs in M1-01 (their
 * full wiring lands in M1-03 / M3). Impersonation is wired as a passthrough
 * so existing call sites preserve their current behaviour. URI resolution via
 * the resolver chain is the M1-01 primary deliverable and is exposed as
 * {@link #resolve(URI)} / {@link #resolve(URI, String, BeDispatchableCredential.Scope)}.
 * </p>
 */
public final class DefaultCredentialBroker implements CredentialBroker {

    private static final long MIN_TTL_SECONDS = 1L;

    private final String catalogName;
    private final Map<String, CredentialResolver> resolversByScheme;
    private final List<String> chainOrder;
    private final long defaultTtlSeconds;
    private final Clock clock;

    private final ConcurrentHashMap<URI, CacheEntry> cache = new ConcurrentHashMap<>();

    public DefaultCredentialBroker(String catalogName,
                                   List<CredentialResolver> chain,
                                   long defaultTtlSeconds) {
        this(catalogName, chain, defaultTtlSeconds, Clock.systemUTC());
    }

    public DefaultCredentialBroker(String catalogName,
                                   List<CredentialResolver> chain,
                                   long defaultTtlSeconds,
                                   Clock clock) {
        this.catalogName = Objects.requireNonNull(catalogName, "catalogName");
        if (chain == null || chain.isEmpty()) {
            throw new IllegalArgumentException("resolver chain must not be empty");
        }
        if (defaultTtlSeconds < MIN_TTL_SECONDS) {
            throw new IllegalArgumentException(
                    "defaultTtlSeconds must be >= " + MIN_TTL_SECONDS);
        }
        this.clock = Objects.requireNonNull(clock, "clock");
        this.defaultTtlSeconds = defaultTtlSeconds;

        Map<String, CredentialResolver> bySchemeOrdered = new LinkedHashMap<>();
        for (CredentialResolver r : chain) {
            if (r == null) {
                throw new IllegalArgumentException("resolver chain entry is null");
            }
            String scheme = r.scheme();
            if (scheme == null || scheme.isEmpty()) {
                throw new IllegalArgumentException("resolver scheme is empty: " + r);
            }
            String s = scheme.toLowerCase(Locale.ROOT);
            CredentialResolver prev = bySchemeOrdered.putIfAbsent(s, r);
            if (prev != null) {
                throw new IllegalArgumentException(
                        "duplicate resolver for scheme '" + s + "'");
            }
        }
        this.resolversByScheme = Collections.unmodifiableMap(bySchemeOrdered);
        this.chainOrder = Collections.unmodifiableList(new ArrayList<>(bySchemeOrdered.keySet()));
    }

    public String catalogName() {
        return catalogName;
    }

    /** Schemes wired in chain order (primarily for diagnostics / tests). */
    public List<String> chainOrder() {
        return chainOrder;
    }

    /**
     * Resolve a credential reference URI through the chain.
     *
     * @throws CredentialResolutionException if the URI is null/blank, the
     *         scheme has no registered resolver, or the resolver itself fails
     */
    public Credential resolve(URI ref) {
        if (ref == null) {
            throw new CredentialResolutionException("credential reference is null");
        }
        String scheme = ref.getScheme();
        if (scheme == null || scheme.trim().isEmpty()) {
            throw new CredentialResolutionException(
                    "credential reference has no scheme: " + ref);
        }
        CredentialResolver resolver = resolversByScheme.get(scheme.toLowerCase(Locale.ROOT));
        if (resolver == null) {
            throw new CredentialResolutionException(
                    "no resolver registered for scheme '" + scheme + "'");
        }

        Instant now = clock.instant();
        CacheEntry hit = cache.get(ref);
        if (hit != null && !hit.isExpired(now)) {
            return hit.credential;
        }

        // Single-flight: the per-URI CacheEntry is itself the lock.
        CacheEntry entry = cache.compute(ref, (k, existing) -> {
            if (existing != null && !existing.isExpired(clock.instant())) {
                return existing;
            }
            ResolverContext rctx = new ResolverContext(catalogName, defaultTtlSeconds);
            Credential resolved = resolver.resolve(k, rctx);
            if (resolved == null) {
                throw new CredentialResolutionException(
                        "resolver for '" + k.getScheme() + "' returned null");
            }
            Instant expiresAt = resolved.expiresAt()
                    .orElseGet(() -> clock.instant().plusSeconds(defaultTtlSeconds));
            return new CacheEntry(resolved, expiresAt);
        });
        return entry.credential;
    }

    /**
     * Convenience: resolve and wrap as a {@link BeDispatchableCredential} ready
     * for thrift dispatch.
     */
    public BeDispatchableCredential resolve(URI ref,
                                            String beType,
                                            BeDispatchableCredential.Scope scope) {
        return new BeDispatchableCredential(beType, resolve(ref), scope);
    }

    /** Drop a cached credential for {@code ref}. */
    public void invalidate(URI ref) {
        if (ref != null) {
            cache.remove(ref);
        }
    }

    /** Drop every cached credential. */
    public void invalidateAll() {
        cache.clear();
    }

    /** Test/diagnostics: current cache size. */
    int cacheSize() {
        return cache.size();
    }

    private static final class CacheEntry {
        final Credential credential;
        final Instant expiresAt;

        CacheEntry(Credential credential, Instant expiresAt) {
            this.credential = credential;
            this.expiresAt = expiresAt;
        }

        boolean isExpired(Instant now) {
            return !now.isBefore(expiresAt);
        }
    }

    // --- CredentialBroker sub-ops ------------------------------------------------
    //
    // Storage / metastore / jdbc / http are intentionally stubbed for M1-01 and
    // throw UOE: full wiring lands in M1-03 (jdbc) and M3 (storage / metastore /
    // http). Impersonation is a passthrough so existing executeAuthenticated
    // call-sites preserve their behaviour after the deprecation switchover.

    @Override
    public StorageCredentialOps storage() {
        return STORAGE_NOT_WIRED;
    }

    @Override
    public MetastoreCredentialOps metastore() {
        return METASTORE_NOT_WIRED;
    }

    @Override
    public JdbcCredentialOps jdbc() {
        return JDBC_NOT_WIRED;
    }

    @Override
    public HttpCredentialOps http() {
        return HTTP_NOT_WIRED;
    }

    @Override
    public RuntimeImpersonationOps impersonation() {
        return PASSTHROUGH_IMPERSONATION;
    }

    private static final StorageCredentialOps STORAGE_NOT_WIRED = new StorageCredentialOps() {
        @Override
        public CredentialEnvelope resolve(StorageRequest req, Mode mode) {
            throw new UnsupportedOperationException("storage credential ops not wired (M3)");
        }

        @Override
        public Map<StoragePath, CredentialEnvelope> resolveAll(List<StorageRequest> reqs, Mode mode) {
            throw new UnsupportedOperationException("storage credential ops not wired (M3)");
        }

        @Override
        public Map<String, String> toBackendProperties(CredentialEnvelope env) {
            throw new UnsupportedOperationException("storage credential ops not wired (M3)");
        }

        @Override
        public void invalidate(CredentialScope scope) {
            throw new UnsupportedOperationException("storage credential ops not wired (M3)");
        }
    };

    private static final MetastoreCredentialOps METASTORE_NOT_WIRED = new MetastoreCredentialOps() {
        @Override
        public <T> T runAs(MetastorePrincipal principal, ThrowingSupplier<T> action) throws Exception {
            throw new UnsupportedOperationException("metastore credential ops not wired (M3)");
        }

        @Override
        public CredentialEnvelope resolve(MetastorePrincipal principal) {
            throw new UnsupportedOperationException("metastore credential ops not wired (M3)");
        }

        @Override
        public void invalidate(MetastorePrincipal principal) {
            throw new UnsupportedOperationException("metastore credential ops not wired (M3)");
        }
    };

    private static final JdbcCredentialOps JDBC_NOT_WIRED = new JdbcCredentialOps() {
        @Override
        public Properties getConnectionProperties(JdbcRequest req) {
            throw new UnsupportedOperationException("jdbc credential ops not wired (M1-03)");
        }

        @Override
        public boolean rotateIfNeeded(JdbcRequest req) {
            throw new UnsupportedOperationException("jdbc credential ops not wired (M1-03)");
        }
    };

    private static final HttpCredentialOps HTTP_NOT_WIRED = new HttpCredentialOps() {
        @Override
        public void sign(HttpRequestSpec spec, CredentialEnvelope env) {
            throw new UnsupportedOperationException("http credential ops not wired (M3)");
        }

        @Override
        public CredentialEnvelope acquire(HttpEndpoint endpoint) {
            throw new UnsupportedOperationException("http credential ops not wired (M3)");
        }
    };

    private static final RuntimeImpersonationOps PASSTHROUGH_IMPERSONATION = new RuntimeImpersonationOps() {
        @Override
        public <T> T runAs(UserContext user, ThrowingSupplier<T> action) throws Exception {
            Objects.requireNonNull(user, "user");
            Objects.requireNonNull(action, "action");
            return action.get();
        }
    };

    /** Suppresses the unused-cache-size warning until invalidate APIs are exercised externally. */
    @SuppressWarnings("unused")
    private Optional<Long> debugTtlSeconds() {
        return Optional.of(defaultTtlSeconds);
    }
}
