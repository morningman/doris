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

package org.apache.doris.connector.cache;

import org.apache.doris.connector.api.cache.ConnectorMetaCacheBinding;
import org.apache.doris.connector.api.cache.ConnectorMetaCacheInvalidation;
import org.apache.doris.connector.api.cache.InvalidateRequest;
import org.apache.doris.connector.api.cache.MetaCacheHandle;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Concurrency tests for {@link ConnectorMetaCacheRegistry}: confirms that
 * concurrent {@link ConnectorMetaCacheRegistry#bind bind} on the same entry
 * name converges to a single handle, and that concurrent invalidation does
 * not deadlock or lose entries.
 */
public class MetaCacheRegistryConcurrencyTest {

    private static ConnectorMetaCacheBinding<String, String> bindingOf(String name) {
        return ConnectorMetaCacheBinding
                .builder(name, String.class, String.class, k -> "v-" + k)
                .invalidationStrategy(ConnectorMetaCacheInvalidation.always())
                .build();
    }

    @Test
    public void concurrentBindOfSameBindingReturnsOneHandle() throws Exception {
        ConnectorMetaCacheRegistry registry = new ConnectorMetaCacheRegistry("cat-conc");
        ConnectorMetaCacheBinding<String, String> b = bindingOf("e1");

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<MetaCacheHandle<String, String>>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return registry.bind(b);
                }));
            }
            start.countDown();
            MetaCacheHandle<String, String> first = futures.get(0).get(10, TimeUnit.SECONDS);
            for (Future<MetaCacheHandle<String, String>> f : futures) {
                Assertions.assertSame(first, f.get(10, TimeUnit.SECONDS),
                        "every concurrent bind must return the same handle");
            }
            Assertions.assertEquals(1, registry.size());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    public void concurrentBindOfDistinctEntriesAllRegister() throws Exception {
        ConnectorMetaCacheRegistry registry = new ConnectorMetaCacheRegistry("cat-conc");
        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                final int idx = i;
                futures.add(pool.submit(() -> {
                    start.await();
                    registry.bind(bindingOf("e-" + idx));
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> f : futures) {
                f.get(10, TimeUnit.SECONDS);
            }
            Assertions.assertEquals(threads, registry.size());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    public void concurrentInvalidateDoesNotDeadlock() throws Exception {
        ConnectorMetaCacheRegistry registry = new ConnectorMetaCacheRegistry("cat-conc");
        for (int i = 0; i < 8; i++) {
            registry.bind(bindingOf("e-" + i));
        }
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger done = new AtomicInteger();
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    for (int n = 0; n < 100; n++) {
                        registry.invalidate(InvalidateRequest.ofCatalog());
                    }
                    done.incrementAndGet();
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> f : futures) {
                f.get(10, TimeUnit.SECONDS);
            }
            Assertions.assertEquals(threads, done.get());
            Assertions.assertEquals(8, registry.size(), "invalidate must not drop registrations");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    public void bindAndInvalidateInterleavedIsSafe() throws Exception {
        ConnectorMetaCacheRegistry registry = new ConnectorMetaCacheRegistry("cat-conc");
        int producers = 8;
        int invalidators = 4;
        ExecutorService pool = Executors.newFixedThreadPool(producers + invalidators);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < producers; i++) {
                final int idx = i;
                futures.add(pool.submit(() -> {
                    start.await();
                    for (int n = 0; n < 50; n++) {
                        registry.bind(bindingOf("p" + idx + "-" + n));
                    }
                    return null;
                }));
            }
            for (int i = 0; i < invalidators; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    for (int n = 0; n < 200; n++) {
                        registry.invalidate(InvalidateRequest.ofCatalog());
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> f : futures) {
                f.get(15, TimeUnit.SECONDS);
            }
            Assertions.assertEquals(producers * 50, registry.size());
        } finally {
            pool.shutdownNow();
        }
    }
}
