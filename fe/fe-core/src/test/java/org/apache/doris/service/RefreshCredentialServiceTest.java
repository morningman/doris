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

import org.apache.doris.connector.credential.Credential;
import org.apache.doris.connector.credential.CredentialResolver;
import org.apache.doris.connector.credential.DefaultCredentialBroker;
import org.apache.doris.connector.credential.ResolverContext;
import org.apache.doris.thrift.TRefreshCredentialRequest;
import org.apache.doris.thrift.TRefreshCredentialResult;
import org.apache.doris.thrift.TStatusCode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RefreshCredentialServiceTest {

    private static final String CATALOG = "test_cat";

    @BeforeEach
    public void reset() {
        RefreshCredentialService.getInstance().resetForTest();
    }

    @AfterEach
    public void cleanup() {
        RefreshCredentialService.getInstance().resetForTest();
    }

    private static CredentialResolver counting(String scheme, AtomicInteger calls,
                                                CountDownLatch gate) {
        return new CredentialResolver() {
            @Override
            public String scheme() {
                return scheme;
            }

            @Override
            public Credential resolve(URI ref, ResolverContext ctx) {
                calls.incrementAndGet();
                if (gate != null) {
                    try {
                        gate.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return Credential.ofSecret("token-for-" + ref.getHost());
            }
        };
    }

    private static List<CredentialResolver> chain(CredentialResolver r) {
        return Collections.singletonList(r);
    }

    @Test
    public void refresh_happyPath_returnsCredential() {
        AtomicInteger calls = new AtomicInteger();
        DefaultCredentialBroker broker = new DefaultCredentialBroker(
                CATALOG, chain(counting("env", calls, null)), 60L);
        RefreshCredentialService.getInstance().registerBrokerForTest(CATALOG, broker);

        TRefreshCredentialRequest req = new TRefreshCredentialRequest("env", "env://A");
        req.setCatalog(CATALOG);
        req.setScope("CATALOG");
        req.setRequestorId("be-1");

        TRefreshCredentialResult res = RefreshCredentialService.getInstance().refresh(req);
        Assertions.assertEquals(TStatusCode.OK, res.getStatus().getStatusCode());
        Assertions.assertNotNull(res.getCredential());
        Assertions.assertEquals("env", res.getCredential().getScheme());
        Assertions.assertEquals("env://A", res.getCredential().getRef());
        Assertions.assertArrayEquals("token-for-A".getBytes(), res.getCredential().getSecret());
        Assertions.assertEquals(1, calls.get());
    }

    @Test
    public void refresh_unknownScheme_returnsInternalError() {
        DefaultCredentialBroker broker = new DefaultCredentialBroker(
                CATALOG, chain(counting("env", new AtomicInteger(), null)), 60L);
        RefreshCredentialService.getInstance().registerBrokerForTest(CATALOG, broker);

        TRefreshCredentialRequest req = new TRefreshCredentialRequest("kms", "kms://aws/k1");
        req.setCatalog(CATALOG);
        req.setScope("CATALOG");

        TRefreshCredentialResult res = RefreshCredentialService.getInstance().refresh(req);
        Assertions.assertNotEquals(TStatusCode.OK, res.getStatus().getStatusCode());
    }

    @Test
    public void refresh_missingScheme_returnsInvalidArgument() {
        TRefreshCredentialRequest req = new TRefreshCredentialRequest();
        req.setRef("env://A");
        TRefreshCredentialResult res = RefreshCredentialService.getInstance().refresh(req);
        Assertions.assertEquals(TStatusCode.INVALID_ARGUMENT, res.getStatus().getStatusCode());
    }

    @Test
    public void refresh_invalidScope_returnsInvalidArgument() {
        DefaultCredentialBroker broker = new DefaultCredentialBroker(
                CATALOG, chain(counting("env", new AtomicInteger(), null)), 60L);
        RefreshCredentialService.getInstance().registerBrokerForTest(CATALOG, broker);

        TRefreshCredentialRequest req = new TRefreshCredentialRequest("env", "env://A");
        req.setCatalog(CATALOG);
        req.setScope("WHATEVER");
        TRefreshCredentialResult res = RefreshCredentialService.getInstance().refresh(req);
        Assertions.assertEquals(TStatusCode.INVALID_ARGUMENT, res.getStatus().getStatusCode());
    }

    @Test
    public void refresh_schemeMismatch_returnsInvalidArgument() {
        DefaultCredentialBroker broker = new DefaultCredentialBroker(
                CATALOG, chain(counting("env", new AtomicInteger(), null)), 60L);
        RefreshCredentialService.getInstance().registerBrokerForTest(CATALOG, broker);

        TRefreshCredentialRequest req = new TRefreshCredentialRequest("env", "vault://x");
        req.setCatalog(CATALOG);
        req.setScope("CATALOG");
        TRefreshCredentialResult res = RefreshCredentialService.getInstance().refresh(req);
        Assertions.assertEquals(TStatusCode.INVALID_ARGUMENT, res.getStatus().getStatusCode());
    }

    @Test
    public void refresh_singleFlight_concurrentCallsOneResolve() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch gate = new CountDownLatch(1);
        DefaultCredentialBroker broker = new DefaultCredentialBroker(
                CATALOG, chain(counting("env", calls, gate)), 60L);
        RefreshCredentialService.getInstance().registerBrokerForTest(CATALOG, broker);

        ExecutorService es = Executors.newFixedThreadPool(8);
        try {
            CountDownLatch ready = new CountDownLatch(8);
            Future<?>[] fs = new Future[8];
            for (int i = 0; i < 8; i++) {
                fs[i] = es.submit(() -> {
                    ready.countDown();
                    TRefreshCredentialRequest req = new TRefreshCredentialRequest("env", "env://X");
                    req.setCatalog(CATALOG);
                    req.setScope("CATALOG");
                    TRefreshCredentialResult res = RefreshCredentialService.getInstance().refresh(req);
                    Assertions.assertEquals(TStatusCode.OK, res.getStatus().getStatusCode());
                });
            }
            ready.await(5, TimeUnit.SECONDS);
            // Let the 8 callers race into refresh(). Then release the resolver.
            Thread.sleep(50);
            gate.countDown();
            for (Future<?> f : fs) {
                f.get(10, TimeUnit.SECONDS);
            }
        } finally {
            es.shutdownNow();
        }
        // Single-flight: exactly one resolver invocation across 8 concurrent BEs.
        Assertions.assertEquals(1, calls.get());
    }

    @Test
    public void refresh_secondCall_bypassesFeBrokerCache() {
        AtomicInteger calls = new AtomicInteger();
        DefaultCredentialBroker broker = new DefaultCredentialBroker(
                CATALOG, chain(counting("env", calls, null)), 60L);
        RefreshCredentialService.getInstance().registerBrokerForTest(CATALOG, broker);

        TRefreshCredentialRequest req = new TRefreshCredentialRequest("env", "env://Y");
        req.setCatalog(CATALOG);
        req.setScope("CATALOG");

        RefreshCredentialService.getInstance().refresh(req);
        RefreshCredentialService.getInstance().refresh(req);
        // refresh() bypasses FE cache; each RPC re-resolves.
        Assertions.assertEquals(2, calls.get());
    }
}
