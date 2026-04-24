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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

class JdbcCredentialContextTest {

    private static final String CATALOG = "test_jdbc";
    private static final String URL = "jdbc:mysql://h:3306/db";

    private static Map<String, String> props(String user, String password) {
        Map<String, String> m = new HashMap<>();
        m.put("jdbc_url", URL);
        m.put("user", user);
        m.put("password", password);
        return m;
    }

    private static FakeBroker brokerEnv(Map<String, FakeBroker.ResolvedValue> env) {
        return new FakeBroker(
                (k, r) -> env.getOrDefault(k, FakeBroker.ResolvedValue.of("")),
                (k, r) -> {
                    throw new IllegalStateException("vault not stubbed for " + k);
                });
    }

    private static FakeBroker brokerVault(Map<String, FakeBroker.ResolvedValue> vault) {
        return new FakeBroker(
                (k, r) -> {
                    throw new IllegalStateException("env not stubbed for " + k);
                },
                (k, r) -> vault.getOrDefault(k, FakeBroker.ResolvedValue.of("")));
    }

    @Test
    void plainStringUserPassword_returnsInline() {
        FakeBroker b = brokerEnv(new HashMap<>());
        JdbcCredentialContext ctx = new JdbcCredentialContext(
                CATALOG, URL, props("alice", "s3cret"), b);
        JdbcConnectionCredential c = ctx.getDriverCredential();
        Assertions.assertEquals("alice", c.user());
        Assertions.assertEquals("s3cret", c.password());
        Assertions.assertEquals(1, b.resolveCalls(),
                "broker.resolve called once even for inline (centralised path)");
    }

    @Test
    void envScheme_callsBroker_returnsEnvValue() {
        Map<String, FakeBroker.ResolvedValue> env = new HashMap<>();
        env.put("MYSQL_PASS", FakeBroker.ResolvedValue.of("from-env"));
        FakeBroker b = brokerEnv(env);
        JdbcCredentialContext ctx = new JdbcCredentialContext(
                CATALOG, URL, props("alice", "env://MYSQL_PASS"), b);
        JdbcConnectionCredential c = ctx.getDriverCredential();
        Assertions.assertEquals("alice", c.user());
        Assertions.assertEquals("from-env", c.password());
    }

    @Test
    void vaultScheme_callsBroker_returnsVaultValue() {
        Map<String, FakeBroker.ResolvedValue> vault = new HashMap<>();
        vault.put("kv/data/jdbc#password", FakeBroker.ResolvedValue.of("vault-secret"));
        FakeBroker b = brokerVault(vault);
        JdbcCredentialContext ctx = new JdbcCredentialContext(
                CATALOG, URL, props("alice", "vault://kv/data/jdbc#password"), b);
        Assertions.assertEquals("vault-secret", ctx.getDriverCredential().password());
    }

    @Test
    void cachingWithinTtl_brokerCalledOnce() {
        Instant start = Instant.parse("2026-01-01T00:00:00Z");
        MutableClock clock = new MutableClock(start);
        FakeBroker b = brokerEnv(new HashMap<>());
        JdbcCredentialContext ctx = new JdbcCredentialContext(
                CATALOG, URL, props("alice", "s3cret"), b, clock, 60L);
        ctx.getDriverCredential();
        ctx.getDriverCredential();
        ctx.getDriverCredential();
        Assertions.assertEquals(1, b.resolveCalls());
    }

    @Test
    void ttlExpired_brokerReinvoked() {
        Instant start = Instant.parse("2026-01-01T00:00:00Z");
        MutableClock clock = new MutableClock(start);
        FakeBroker b = brokerEnv(new HashMap<>());
        JdbcCredentialContext ctx = new JdbcCredentialContext(
                CATALOG, URL, props("alice", "s3cret"), b, clock, 5L);
        ctx.getDriverCredential();
        clock.advance(Duration.ofSeconds(6));
        ctx.getDriverCredential();
        Assertions.assertEquals(2, b.resolveCalls());
    }

    @Test
    void envelopeExpiry_respectedOverFallbackTtl() {
        Instant start = Instant.parse("2026-01-01T00:00:00Z");
        MutableClock clock = new MutableClock(start);
        Map<String, FakeBroker.ResolvedValue> env = new HashMap<>();
        env.put("X", FakeBroker.ResolvedValue.withExpiry("v", start.plusSeconds(2)));
        FakeBroker b = brokerEnv(env);
        // fallback TTL=600 but envelope expiry=+2s should win.
        JdbcCredentialContext ctx = new JdbcCredentialContext(
                CATALOG, URL, props("alice", "env://X"), b, clock, 600L);
        ctx.getDriverCredential();
        clock.advance(Duration.ofSeconds(3));
        ctx.getDriverCredential();
        Assertions.assertEquals(2, b.resolveCalls(),
                "envelope expiresAt must override the plugin fallback TTL");
    }

    @Test
    void refreshHint_propagatedToBeCredential() {
        Map<String, FakeBroker.ResolvedValue> env = new HashMap<>();
        env.put("X", FakeBroker.ResolvedValue.withHint("v", "vault://kv/refresh"));
        FakeBroker b = brokerEnv(env);
        JdbcCredentialContext ctx = new JdbcCredentialContext(
                CATALOG, URL, props("alice", "env://X"), b);
        BeDispatchableJdbcCredential be = ctx.getBeCredential();
        Assertions.assertEquals("JDBC", be.beType());
        Assertions.assertEquals("v", be.credential().password());
        Assertions.assertTrue(be.credential().refreshHint().isPresent());
        Assertions.assertEquals("vault://kv/refresh", be.credential().refreshHint().get());
        Assertions.assertEquals("vault://kv/refresh", be.serialize().get("refresh_hint"));
    }

    @Test
    void invalidate_dropsCacheAndCallsRotate() {
        FakeBroker b = brokerEnv(new HashMap<>());
        JdbcCredentialContext ctx = new JdbcCredentialContext(
                CATALOG, URL, props("alice", "env://X"), b);
        ctx.getDriverCredential();
        Assertions.assertTrue(ctx.hasCached());
        ctx.invalidate();
        Assertions.assertFalse(ctx.hasCached());
        Assertions.assertEquals(1, b.rotateCalls());
        // next call re-resolves.
        ctx.getDriverCredential();
        Assertions.assertEquals(2, b.resolveCalls());
    }

    @Test
    void sensitiveFlag_redactionInToString() {
        Map<String, FakeBroker.ResolvedValue> env = new HashMap<>();
        env.put("X", FakeBroker.ResolvedValue.withHint("very-secret-value", "vault://kv/refresh-host"));
        FakeBroker b = brokerEnv(env);
        JdbcCredentialContext ctx = new JdbcCredentialContext(
                CATALOG, URL, props("alice", "env://X"), b);
        JdbcConnectionCredential c = ctx.getDriverCredential();
        String s1 = c.toString();
        Assertions.assertFalse(s1.contains("very-secret-value"), s1);
        Assertions.assertFalse(s1.contains("alice"), s1);
        Assertions.assertFalse(s1.contains("vault://kv/refresh-host"), s1);
        BeDispatchableJdbcCredential be = ctx.getBeCredential();
        String s2 = be.toString();
        Assertions.assertFalse(s2.contains("very-secret-value"), s2);
        Assertions.assertFalse(s2.contains("alice"), s2);
        Assertions.assertFalse(s2.contains("vault://kv/refresh-host"), s2);
    }

    @Test
    void concurrentSingleFlight_oneResolution() throws Exception {
        Instant start = Instant.parse("2026-01-01T00:00:00Z");
        MutableClock clock = new MutableClock(start);
        FakeBroker b = brokerEnv(new HashMap<>());
        JdbcCredentialContext ctx = new JdbcCredentialContext(
                CATALOG, URL, props("alice", "s3cret"), b, clock, 60L);
        int n = 16;
        Thread[] ts = new Thread[n];
        for (int i = 0; i < n; i++) {
            ts[i] = new Thread(ctx::getDriverCredential);
        }
        for (Thread t : ts) {
            t.start();
        }
        for (Thread t : ts) {
            t.join(2000);
        }
        Assertions.assertEquals(1, b.resolveCalls());
    }

    @Test
    void emptyPasswordProperty_resolvesToEmpty() {
        FakeBroker b = brokerEnv(new HashMap<>());
        Map<String, String> p = props("alice", "");
        JdbcCredentialContext ctx = new JdbcCredentialContext(CATALOG, URL, p, b);
        Assertions.assertEquals("", ctx.getDriverCredential().password());
    }

    @Test
    void nullChecks_throwIAE() {
        FakeBroker b = brokerEnv(new HashMap<>());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new JdbcCredentialContext(null, URL, props("u", "p"), b));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new JdbcCredentialContext(CATALOG, "", props("u", "p"), b));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new JdbcCredentialContext(CATALOG, URL, null, b));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new JdbcCredentialContext(CATALOG, URL, props("u", "p"), null));
    }

    private static final class MutableClock extends Clock {
        private Instant now;

        MutableClock(Instant start) {
            this.now = start;
        }

        void advance(Duration d) {
            now = now.plus(d);
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
