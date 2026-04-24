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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultCredentialBrokerTest {

    private static List<CredentialResolver> envChain() {
        return Collections.singletonList(new EnvCredentialResolver(name -> "v-" + name));
    }

    @Test
    public void resolve_unknownScheme_throws() {
        DefaultCredentialBroker b = new DefaultCredentialBroker("c", envChain(), 60L);
        Assertions.assertThrows(CredentialResolutionException.class,
                () -> b.resolve(URI.create("kms://aws/key")));
    }

    @Test
    public void resolve_nullRef_throws() {
        DefaultCredentialBroker b = new DefaultCredentialBroker("c", envChain(), 60L);
        Assertions.assertThrows(CredentialResolutionException.class, () -> b.resolve(null));
    }

    @Test
    public void resolve_blankScheme_throws() {
        DefaultCredentialBroker b = new DefaultCredentialBroker("c", envChain(), 60L);
        Assertions.assertThrows(CredentialResolutionException.class,
                () -> b.resolve(URI.create("/no-scheme")));
    }

    @Test
    public void resolve_cacheHit_doesNotReload() {
        AtomicInteger calls = new AtomicInteger(0);
        CredentialResolver counting = new CredentialResolver() {
            @Override
            public String scheme() {
                return "env";
            }

            @Override
            public Credential resolve(URI ref, ResolverContext ctx) {
                calls.incrementAndGet();
                return Credential.ofSecret("v");
            }
        };
        DefaultCredentialBroker b = new DefaultCredentialBroker("c", Collections.singletonList(counting), 60L);
        b.resolve(URI.create("env://A"));
        b.resolve(URI.create("env://A"));
        b.resolve(URI.create("env://A"));
        Assertions.assertEquals(1, calls.get());
        Assertions.assertEquals(1, b.cacheSize());
    }

    @Test
    public void resolve_expiry_triggersReload() {
        AtomicInteger calls = new AtomicInteger(0);
        Instant start = Instant.parse("2025-01-01T00:00:00Z");
        MutableClock clock = new MutableClock(start);
        CredentialResolver shortLived = new CredentialResolver() {
            @Override
            public String scheme() {
                return "env";
            }

            @Override
            public Credential resolve(URI ref, ResolverContext ctx) {
                calls.incrementAndGet();
                return Credential.ofSecret("v", clock.instant().plusSeconds(10), null);
            }
        };
        DefaultCredentialBroker b = new DefaultCredentialBroker(
                "c", Collections.singletonList(shortLived), 60L, clock);
        b.resolve(URI.create("env://A"));
        b.resolve(URI.create("env://A"));
        Assertions.assertEquals(1, calls.get());
        clock.advance(Duration.ofSeconds(11));
        b.resolve(URI.create("env://A"));
        Assertions.assertEquals(2, calls.get());
    }

    @Test
    public void resolve_singleFlight_concurrentNoDoubleLoad() throws Exception {
        AtomicInteger calls = new AtomicInteger(0);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch inResolver = new CountDownLatch(1);
        CredentialResolver slow = new CredentialResolver() {
            @Override
            public String scheme() {
                return "env";
            }

            @Override
            public Credential resolve(URI ref, ResolverContext ctx) {
                calls.incrementAndGet();
                inResolver.countDown();
                try {
                    start.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return Credential.ofSecret("v");
            }
        };
        DefaultCredentialBroker b = new DefaultCredentialBroker("c", Collections.singletonList(slow), 60L);
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            URI ref = URI.create("env://A");
            Future<Credential> f1 = pool.submit(() -> b.resolve(ref));
            Assertions.assertTrue(inResolver.await(5, TimeUnit.SECONDS));
            Future<Credential> f2 = pool.submit(() -> b.resolve(ref));
            Future<Credential> f3 = pool.submit(() -> b.resolve(ref));
            // Give the second/third callers a moment to enter compute().
            Thread.sleep(50);
            start.countDown();
            f1.get(5, TimeUnit.SECONDS);
            f2.get(5, TimeUnit.SECONDS);
            f3.get(5, TimeUnit.SECONDS);
            Assertions.assertEquals(1, calls.get());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    public void chainOrder_honoredAndDeduped() {
        DefaultCredentialBroker b = new DefaultCredentialBroker("c",
                Arrays.asList(new EnvCredentialResolver(),
                        new FileCredentialResolver(),
                        new KmsCredentialResolver()),
                60L);
        Assertions.assertEquals(Arrays.asList("env", "file", "kms"), b.chainOrder());
    }

    @Test
    public void duplicateScheme_inChain_throws() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new DefaultCredentialBroker("c",
                        Arrays.asList(new EnvCredentialResolver(), new EnvCredentialResolver()),
                        60L));
    }

    @Test
    public void emptyChain_throws() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new DefaultCredentialBroker("c", Collections.emptyList(), 60L));
    }

    @Test
    public void invalidate_dropsCache() {
        DefaultCredentialBroker b = new DefaultCredentialBroker("c", envChain(), 60L);
        b.resolve(URI.create("env://A"));
        Assertions.assertEquals(1, b.cacheSize());
        b.invalidate(URI.create("env://A"));
        Assertions.assertEquals(0, b.cacheSize());
    }

    @Test
    public void invalidateAll_clearsCache() {
        DefaultCredentialBroker b = new DefaultCredentialBroker("c", envChain(), 60L);
        b.resolve(URI.create("env://A"));
        b.resolve(URI.create("env://B"));
        b.invalidateAll();
        Assertions.assertEquals(0, b.cacheSize());
    }

    @Test
    public void credentialToString_doesNotContainSecret() {
        DefaultCredentialBroker b = new DefaultCredentialBroker("c", envChain(), 60L);
        Credential c = b.resolve(URI.create("env://TOPSECRETKEY"));
        // env lookup returns "v-" + name = "v-TOPSECRETKEY"
        Assertions.assertEquals("v-TOPSECRETKEY", c.secretAsString());
        String s = c.toString();
        Assertions.assertFalse(s.contains("v-TOPSECRETKEY"), "toString leaks secret: " + s);
        Assertions.assertTrue(s.contains("***REDACTED***"), "toString must redact: " + s);
    }

    @Test
    public void impersonation_isPassthrough() throws Exception {
        DefaultCredentialBroker b = new DefaultCredentialBroker("c", envChain(), 60L);
        Integer out = b.impersonation().runAs(
                org.apache.doris.connector.api.credential.UserContext.builder()
                        .username("alice").build(),
                () -> 42);
        Assertions.assertEquals(42, out);
    }

    @Test
    public void unwiredSubOps_throwUOE() {
        DefaultCredentialBroker b = new DefaultCredentialBroker("c", envChain(), 60L);
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> b.storage().invalidate(
                        org.apache.doris.connector.api.credential.CredentialScope.CATALOG));
        // jdbc() is wired in M1-03; null req triggers IAE not UOE.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> b.jdbc().getConnectionProperties(null));
    }

    @Test
    public void resolveBeDispatchable_wrapsCredential() {
        DefaultCredentialBroker b = new DefaultCredentialBroker("c", envChain(), 60L);
        BeDispatchableCredential w = b.resolve(URI.create("env://A"),
                "TYPE_S3", BeDispatchableCredential.Scope.CATALOG);
        Assertions.assertEquals("TYPE_S3", w.beType());
        Assertions.assertEquals(BeDispatchableCredential.Scope.CATALOG, w.scope());
        Assertions.assertEquals("v-A", w.credential().secretAsString());
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
        public java.time.ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
