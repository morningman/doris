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

import org.apache.doris.connector.api.ConnectorTableId;
import org.apache.doris.connector.api.cache.CacheLoader;
import org.apache.doris.connector.api.cache.ConnectorCacheSpec;
import org.apache.doris.connector.api.cache.ConnectorMetaCacheBinding;
import org.apache.doris.connector.api.cache.ConnectorMetaCacheInvalidation;
import org.apache.doris.connector.api.cache.InvalidateRequest;
import org.apache.doris.connector.api.cache.InvalidateScope;
import org.apache.doris.connector.api.cache.MetaCacheHandle;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectorMetaCacheRegistryTest {

    private static ConnectorMetaCacheBinding<String, String> binding(
            String name,
            CacheLoader<String, String> loader,
            ConnectorMetaCacheInvalidation inv,
            ConnectorCacheSpec spec) {
        return ConnectorMetaCacheBinding.builder(name, String.class, String.class, loader)
                .invalidationStrategy(inv)
                .defaultSpec(spec)
                .build();
    }

    @Test
    public void getOrCreateCreatesHandleAndLoadsOnce() {
        AtomicInteger calls = new AtomicInteger();
        CacheLoader<String, String> loader = k -> {
            calls.incrementAndGet();
            return "v-" + k;
        };
        ConnectorMetaCacheBinding<String, String> b = binding(
                "e1", loader, ConnectorMetaCacheInvalidation.always(), ConnectorCacheSpec.defaults());
        ConnectorMetaCacheRegistry reg = new ConnectorMetaCacheRegistry("cat1");

        MetaCacheHandle<String, String> h = reg.getOrCreateCache(b);
        Assertions.assertNotNull(h);
        Assertions.assertEquals("v-k1", h.get("k1"));
        Assertions.assertEquals("v-k1", h.get("k1"));
        Assertions.assertEquals(1, calls.get());
    }

    @Test
    public void getOrCreateReturnsSameHandleForSameBinding() {
        ConnectorMetaCacheBinding<String, String> b = binding(
                "e1", k -> k, ConnectorMetaCacheInvalidation.always(), ConnectorCacheSpec.defaults());
        ConnectorMetaCacheRegistry reg = new ConnectorMetaCacheRegistry("cat1");

        MetaCacheHandle<String, String> h1 = reg.getOrCreateCache(b);
        MetaCacheHandle<String, String> h2 = reg.getOrCreateCache(b);
        Assertions.assertSame(h1, h2);
    }

    @Test
    public void getOrCreateRejectsDifferentBindingWithSameEntryName() {
        CacheLoader<String, String> loader = k -> k;
        ConnectorMetaCacheBinding<String, String> b1 = binding(
                "dup", loader, ConnectorMetaCacheInvalidation.always(),
                ConnectorCacheSpec.builder().ttl(Duration.ofMinutes(1)).build());
        ConnectorMetaCacheBinding<String, String> b2 = binding(
                "dup", loader, ConnectorMetaCacheInvalidation.always(),
                ConnectorCacheSpec.builder().ttl(Duration.ofMinutes(5)).build());

        ConnectorMetaCacheRegistry reg = new ConnectorMetaCacheRegistry("cat1");
        reg.getOrCreateCache(b1);
        Assertions.assertThrows(IllegalStateException.class, () -> reg.getOrCreateCache(b2));
    }

    @Test
    public void invalidateCatalogAppliesToAlwaysStrategy() {
        ConnectorMetaCacheBinding<String, String> b = binding(
                "e1", k -> "v", ConnectorMetaCacheInvalidation.always(), ConnectorCacheSpec.defaults());
        ConnectorMetaCacheRegistry reg = new ConnectorMetaCacheRegistry("cat1");
        MetaCacheHandle<String, String> h = reg.getOrCreateCache(b);
        h.put("k", "v1");
        Assertions.assertTrue(h.getIfPresent("k").isPresent());

        reg.invalidate(InvalidateRequest.ofCatalog());
        Assertions.assertFalse(h.getIfPresent("k").isPresent());
    }

    @Test
    public void invalidateDatabaseSkipsTableScopedBinding() {
        ConnectorMetaCacheBinding<String, String> tableScoped = binding(
                "tbl", k -> "v",
                ConnectorMetaCacheInvalidation.byScope(InvalidateScope.TABLE),
                ConnectorCacheSpec.defaults());
        ConnectorMetaCacheRegistry reg = new ConnectorMetaCacheRegistry("cat1");
        MetaCacheHandle<String, String> h = reg.getOrCreateCache(tableScoped);
        h.put("k", "v1");

        reg.invalidate(InvalidateRequest.ofDatabase("db1"));
        Assertions.assertTrue(h.getIfPresent("k").isPresent(),
                "table-scoped binding should NOT be cleared by a database-scope request");
    }

    @Test
    public void invalidateTableAppliesToAlwaysStrategy() {
        ConnectorMetaCacheBinding<String, String> always = binding(
                "e1", k -> "v", ConnectorMetaCacheInvalidation.always(), ConnectorCacheSpec.defaults());
        ConnectorMetaCacheRegistry reg = new ConnectorMetaCacheRegistry("cat1");
        MetaCacheHandle<String, String> h = reg.getOrCreateCache(always);
        h.put("k", "v1");

        reg.invalidate(InvalidateRequest.ofTable(ConnectorTableId.of("db1", "t1")));
        Assertions.assertFalse(h.getIfPresent("k").isPresent());
    }

    @Test
    public void sizeAndContainsReflectRegistrations() {
        ConnectorMetaCacheRegistry reg = new ConnectorMetaCacheRegistry("cat1");
        Assertions.assertEquals(0, reg.size());
        Assertions.assertFalse(reg.contains("e1"));

        reg.getOrCreateCache(binding(
                "e1", k -> "v", ConnectorMetaCacheInvalidation.always(), ConnectorCacheSpec.defaults()));
        reg.getOrCreateCache(binding(
                "e2", k -> "v", ConnectorMetaCacheInvalidation.always(), ConnectorCacheSpec.defaults()));

        Assertions.assertEquals(2, reg.size());
        Assertions.assertTrue(reg.contains("e1"));
        Assertions.assertTrue(reg.contains("e2"));
        Assertions.assertFalse(reg.contains("e3"));
    }

    @Test
    public void constructorRejectsNullCatalogName() {
        Assertions.assertThrows(NullPointerException.class, () -> new ConnectorMetaCacheRegistry(null));
    }

    @Test
    public void getOrCreateRejectsNullBinding() {
        ConnectorMetaCacheRegistry reg = new ConnectorMetaCacheRegistry("cat1");
        Assertions.assertThrows(NullPointerException.class, () -> reg.getOrCreateCache(null));
    }
}
