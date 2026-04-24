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
import org.apache.doris.connector.api.credential.JdbcCredentialOps;
import org.apache.doris.connector.api.credential.JdbcRequest;
import org.apache.doris.connector.jdbc.JdbcConnectorProperties;

import java.time.Clock;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Per-connector JDBC credential helper.
 *
 * <p>Holds the raw {@code user} / {@code password} catalog property values and
 * resolves them through {@link CredentialBroker#jdbc()}. Plain literals are
 * passed through as-is (backward compatibility); URI references with the
 * {@code env://}, {@code file://}, {@code kms://}, or {@code vault://} schemes
 * are routed through the broker's resolver chain.</p>
 *
 * <p>Resolved credentials are cached in this instance and re-used until
 * either the underlying {@link CredentialEnvelope#expiresAt()} elapses or a
 * caller invokes {@link #invalidate()} (e.g. on a 401-style auth failure).
 * Concurrent callers share a single resolution via {@code synchronized}
 * single-flight; the broker's own per-URI cache deduplicates further.</p>
 *
 * <p>Secrets are never logged. The cached {@link JdbcConnectionCredential}
 * redacts user/password in its {@code toString()}.</p>
 */
public final class JdbcCredentialContext {

    /**
     * Plugin-side fallback TTL applied when the resolved envelope carries no
     * {@code expiresAt}. Bounded so a stuck broker cache doesn't pin an
     * outdated credential indefinitely.
     */
    static final long DEFAULT_PLUGIN_TTL_SECONDS = 60L;

    private final String catalog;
    private final String url;
    private final String rawUser;
    private final String rawPassword;
    private final Map<String, String> attrs;
    private final CredentialBroker broker;
    private final Clock clock;
    private final long fallbackTtlSeconds;

    private final Object lock = new Object();
    private volatile Cached cached;

    public JdbcCredentialContext(String catalog,
                                 String url,
                                 Map<String, String> properties,
                                 CredentialBroker broker) {
        this(catalog, url, properties, broker, Clock.systemUTC(), DEFAULT_PLUGIN_TTL_SECONDS);
    }

    JdbcCredentialContext(String catalog,
                          String url,
                          Map<String, String> properties,
                          CredentialBroker broker,
                          Clock clock,
                          long fallbackTtlSeconds) {
        if (catalog == null || catalog.isEmpty()) {
            throw new IllegalArgumentException("catalog is required");
        }
        if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("url is required");
        }
        if (properties == null) {
            throw new IllegalArgumentException("properties is required");
        }
        if (broker == null) {
            throw new IllegalArgumentException("broker is required");
        }
        if (fallbackTtlSeconds < 1L) {
            throw new IllegalArgumentException("fallbackTtlSeconds must be >= 1");
        }
        this.catalog = catalog;
        this.url = url;
        this.rawUser = properties.getOrDefault(JdbcConnectorProperties.USER, "");
        this.rawPassword = properties.getOrDefault(JdbcConnectorProperties.PASSWORD, "");
        Map<String, String> attrsMap = new LinkedHashMap<>();
        attrsMap.put(JdbcConnectorProperties.USER, rawUser);
        attrsMap.put(JdbcConnectorProperties.PASSWORD, rawPassword);
        this.attrs = Collections.unmodifiableMap(attrsMap);
        this.broker = broker;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.fallbackTtlSeconds = fallbackTtlSeconds;
    }

    /**
     * Returns the resolved JDBC user/password pair, served from the in-memory
     * cache when the previous resolution is still fresh.
     */
    public JdbcConnectionCredential getDriverCredential() {
        Instant now = clock.instant();
        Cached snap = cached;
        if (snap != null && !snap.isExpired(now)) {
            return snap.credential;
        }
        synchronized (lock) {
            snap = cached;
            if (snap != null && !snap.isExpired(clock.instant())) {
                return snap.credential;
            }
            JdbcConnectionCredential resolved = doResolve();
            Instant expiry = resolved.expiresAt()
                    .orElseGet(() -> clock.instant().plusSeconds(fallbackTtlSeconds));
            cached = new Cached(resolved, expiry);
            return resolved;
        }
    }

    /**
     * Returns a {@link BeDispatchableJdbcCredential} wrapping the current
     * resolved value, suitable for FE→BE dispatch (M1-02 thrift envelope).
     */
    public BeDispatchableJdbcCredential getBeCredential() {
        return new BeDispatchableJdbcCredential(getDriverCredential());
    }

    /**
     * Drop the cached credential and ask the broker to invalidate any URI
     * entries it cached on our behalf. Call this on auth failures.
     */
    public void invalidate() {
        synchronized (lock) {
            cached = null;
            JdbcCredentialOps ops = broker.jdbc();
            ops.rotateIfNeeded(buildRequest());
        }
    }

    /** Diagnostic: whether a cached credential is currently held. */
    boolean hasCached() {
        return cached != null;
    }

    private JdbcConnectionCredential doResolve() {
        JdbcCredentialOps ops = broker.jdbc();
        CredentialEnvelope env = ops.resolve(buildRequest());
        Map<String, String> payload = env.payload();
        String resolvedUser = payload.getOrDefault(JdbcConnectorProperties.USER, rawUser);
        String resolvedPassword = payload.getOrDefault(JdbcConnectorProperties.PASSWORD, rawPassword);
        Instant expiresAt = env.expiresAt().orElse(null);
        String refreshHint = env.refreshHint().orElse(null);
        return new JdbcConnectionCredential(
                resolvedUser, resolvedPassword, expiresAt, refreshHint);
    }

    private JdbcRequest buildRequest() {
        JdbcRequest.Builder b = JdbcRequest.builder()
                .catalog(catalog)
                .url(url)
                .attrs(attrs);
        if (!rawUser.isEmpty()) {
            b.username(rawUser);
        }
        return b.build();
    }

    Optional<Instant> currentExpiry() {
        Cached snap = cached;
        return snap == null ? Optional.empty() : Optional.of(snap.expiresAt);
    }

    private static final class Cached {
        final JdbcConnectionCredential credential;
        final Instant expiresAt;

        Cached(JdbcConnectionCredential credential, Instant expiresAt) {
            this.credential = credential;
            this.expiresAt = expiresAt;
        }

        boolean isExpired(Instant now) {
            return !now.isBefore(expiresAt);
        }
    }
}
