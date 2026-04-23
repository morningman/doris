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

import org.apache.doris.connector.api.cache.CacheLoader;
import org.apache.doris.connector.api.cache.ConnectorCacheSpec;
import org.apache.doris.connector.api.cache.ConnectorMetaCacheBinding;
import org.apache.doris.connector.api.cache.MetaCacheHandle;
import org.apache.doris.connector.api.cache.Weigher;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

public class CaffeineMetaCacheHandleTest {

    private static ConnectorMetaCacheBinding.Builder<String, String> base(String name, CacheLoader<String, String> l) {
        return ConnectorMetaCacheBinding.builder(name, String.class, String.class, l);
    }

    @Test
    public void putAndGetIfPresentRoundTrip() {
        MetaCacheHandle<String, String> h = CaffeineMetaCacheHandle.build(
                base("e", k -> "loaded").build());
        h.put("k", "v");
        Assertions.assertEquals("v", h.getIfPresent("k").orElse(null));

        h.invalidate("k");
        Assertions.assertFalse(h.getIfPresent("k").isPresent());
    }

    @Test
    public void invalidateAllClearsEntries() {
        MetaCacheHandle<String, String> h = CaffeineMetaCacheHandle.build(
                base("e", k -> "v").build());
        h.put("a", "1");
        h.put("b", "2");
        h.invalidateAll();
        Assertions.assertFalse(h.getIfPresent("a").isPresent());
        Assertions.assertFalse(h.getIfPresent("b").isPresent());
    }

    @Test
    public void statsReportHitsAndMissesAfterLoader() {
        AtomicInteger loads = new AtomicInteger();
        MetaCacheHandle<String, String> h = CaffeineMetaCacheHandle.build(
                base("e", k -> {
                    loads.incrementAndGet();
                    return "v-" + k;
                }).build());

        Assertions.assertEquals("v-k1", h.get("k1"));
        Assertions.assertEquals("v-k1", h.get("k1"));
        Assertions.assertEquals("v-k2", h.get("k2"));

        MetaCacheHandle.CacheStats s = h.stats();
        Assertions.assertEquals(1, s.getHitCount(), "second k1 is a hit");
        Assertions.assertEquals(2, s.getMissCount(), "k1 first + k2 miss");
        Assertions.assertEquals(2, s.getLoadSuccessCount());
        Assertions.assertEquals(0, s.getLoadFailureCount());
        Assertions.assertEquals(2, loads.get());
        Assertions.assertTrue(s.getCurrentSize() >= 1);
    }

    @Test
    public void ttlExpiresEntries() throws Exception {
        ConnectorCacheSpec spec = ConnectorCacheSpec.builder()
                .ttl(Duration.ofMillis(50))
                .build();
        MetaCacheHandle<String, String> h = CaffeineMetaCacheHandle.build(
                base("e", k -> "v").defaultSpec(spec).build());
        h.put("k", "v1");
        Assertions.assertTrue(h.getIfPresent("k").isPresent());

        Thread.sleep(300L);
        Assertions.assertFalse(h.getIfPresent("k").isPresent(),
                "entry should have expired after ttl");
    }

    @Test
    public void weigherHonoredForSizeEviction() {
        Weigher<String, String> w = (k, v) -> 10;
        ConnectorCacheSpec spec = ConnectorCacheSpec.builder()
                .maxSize(20L)
                .ttl(Duration.ofHours(1))
                .build();
        MetaCacheHandle<String, String> h = CaffeineMetaCacheHandle.build(
                base("e", k -> "v").weigher(w).defaultSpec(spec).build());

        for (int i = 0; i < 10; i++) {
            h.put("k" + i, "v" + i);
        }

        MetaCacheHandle.CacheStats s = h.stats();
        Assertions.assertTrue(s.getCurrentSize() <= 2,
                "weigher should cap total weight at 20 (=2 entries of weight 10); got size="
                        + s.getCurrentSize());
    }

    @Test
    public void checkedExceptionFromLoaderSurfacesAsRuntime() {
        MetaCacheHandle<String, String> h = CaffeineMetaCacheHandle.build(
                base("e", k -> {
                    throw new Exception("boom");
                }).build());
        Assertions.assertThrows(RuntimeException.class, () -> h.get("k"));
    }
}
