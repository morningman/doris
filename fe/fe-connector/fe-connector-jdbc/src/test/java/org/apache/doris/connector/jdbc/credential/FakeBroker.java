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

package org.apache.doris.connector.jdbc.credential;

import org.apache.doris.connector.api.credential.CredentialBroker;
import org.apache.doris.connector.api.credential.CredentialEnvelope;
import org.apache.doris.connector.api.credential.CredentialScope;
import org.apache.doris.connector.api.credential.HttpCredentialOps;
import org.apache.doris.connector.api.credential.JdbcCredentialOps;
import org.apache.doris.connector.api.credential.JdbcRequest;
import org.apache.doris.connector.api.credential.MetastoreCredentialOps;
import org.apache.doris.connector.api.credential.RuntimeImpersonationOps;
import org.apache.doris.connector.api.credential.StorageCredentialOps;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

/**
 * Test broker that resolves the {@code env://} and {@code vault://} schemes
 * via injected lookup functions. {@link #resolveCalls()} records every call
 * to {@link JdbcCredentialOps#resolve(JdbcRequest)} for cache assertions.
 */
final class FakeBroker implements CredentialBroker {

    private final BiFunction<String, JdbcRequest, ResolvedValue> envLookup;
    private final BiFunction<String, JdbcRequest, ResolvedValue> vaultLookup;
    private final AtomicInteger resolveCalls = new AtomicInteger(0);
    private final AtomicInteger rotateCalls = new AtomicInteger(0);

    FakeBroker(BiFunction<String, JdbcRequest, ResolvedValue> envLookup,
               BiFunction<String, JdbcRequest, ResolvedValue> vaultLookup) {
        this.envLookup = envLookup;
        this.vaultLookup = vaultLookup;
    }

    int resolveCalls() {
        return resolveCalls.get();
    }

    int rotateCalls() {
        return rotateCalls.get();
    }

    @Override
    public StorageCredentialOps storage() {
        throw new UnsupportedOperationException();
    }

    @Override
    public MetastoreCredentialOps metastore() {
        throw new UnsupportedOperationException();
    }

    @Override
    public JdbcCredentialOps jdbc() {
        return new JdbcCredentialOps() {
            @Override
            public Properties getConnectionProperties(JdbcRequest req) {
                CredentialEnvelope env = resolve(req);
                Properties p = new Properties();
                env.payload().forEach(p::setProperty);
                return p;
            }

            @Override
            public boolean rotateIfNeeded(JdbcRequest req) {
                rotateCalls.incrementAndGet();
                return true;
            }

            @Override
            public CredentialEnvelope resolve(JdbcRequest req) {
                resolveCalls.incrementAndGet();
                Map<String, String> payload = new LinkedHashMap<>();
                Instant earliest = null;
                String hint = null;
                for (Map.Entry<String, String> e : req.attrs().entrySet()) {
                    String raw = e.getValue();
                    ResolvedValue r;
                    if (raw == null || raw.isEmpty()) {
                        payload.put(e.getKey(), "");
                        continue;
                    }
                    if (raw.startsWith("env://")) {
                        r = envLookup.apply(raw.substring("env://".length()), req);
                    } else if (raw.startsWith("vault://")) {
                        r = vaultLookup.apply(raw.substring("vault://".length()), req);
                    } else {
                        payload.put(e.getKey(), raw);
                        continue;
                    }
                    payload.put(e.getKey(), r.value);
                    if (r.expiresAt != null && (earliest == null || r.expiresAt.isBefore(earliest))) {
                        earliest = r.expiresAt;
                    }
                    if (hint == null && r.refreshHint != null) {
                        hint = r.refreshHint;
                    }
                }
                CredentialEnvelope.Builder b = CredentialEnvelope.builder()
                        .type("jdbc")
                        .scope(CredentialScope.CATALOG)
                        .payload(payload);
                if (earliest != null) {
                    b.expiresAt(earliest);
                }
                if (hint != null) {
                    b.refreshHint(hint);
                }
                return b.build();
            }
        };
    }

    @Override
    public HttpCredentialOps http() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RuntimeImpersonationOps impersonation() {
        throw new UnsupportedOperationException();
    }

    static final class ResolvedValue {
        final String value;
        final Instant expiresAt;
        final String refreshHint;

        ResolvedValue(String value, Instant expiresAt, String refreshHint) {
            this.value = value;
            this.expiresAt = expiresAt;
            this.refreshHint = refreshHint;
        }

        static ResolvedValue of(String v) {
            return new ResolvedValue(v, null, null);
        }

        static ResolvedValue withExpiry(String v, Instant exp) {
            return new ResolvedValue(v, exp, null);
        }

        static ResolvedValue withHint(String v, String hint) {
            return new ResolvedValue(v, null, hint);
        }
    }
}
