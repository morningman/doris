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
import org.apache.doris.connector.api.cache.CacheSnapshot;
import org.apache.doris.connector.api.cache.ConnectorCacheSpec;
import org.apache.doris.connector.api.cache.ConnectorMetaCacheBinding;
import org.apache.doris.connector.api.cache.ConnectorMetaCacheInvalidation;
import org.apache.doris.connector.api.cache.InvalidateRequest;
import org.apache.doris.connector.api.cache.MetaCacheHandle;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Tests for the M1-09 additions to {@link ConnectorMetaCacheRegistry}:
 * explicit {@link ConnectorMetaCacheRegistry#bind bind}/{@link
 * ConnectorMetaCacheRegistry#bindAll bindAll}/{@link
 * ConnectorMetaCacheRegistry#detach detach}/{@link
 * ConnectorMetaCacheRegistry#snapshot snapshot}/{@link
 * ConnectorMetaCacheRegistry#entryNames entryNames} surface.
 */
public class MetaCacheRegistryBindingTest {

    private ConnectorMetaCacheRegistry registry;

    @BeforeEach
    public void setUp() {
        registry = new ConnectorMetaCacheRegistry("cat-bind");
    }

    private static ConnectorMetaCacheBinding<String, String> bindingOf(String name) {
        CacheLoader<String, String> loader = k -> "v-" + k;
        return ConnectorMetaCacheBinding.builder(name, String.class, String.class, loader)
                .invalidationStrategy(ConnectorMetaCacheInvalidation.always())
                .defaultSpec(ConnectorCacheSpec.defaults())
                .build();
    }

    @Test
    public void bindReturnsHandleAndRegistersEntry() {
        ConnectorMetaCacheBinding<String, String> b = bindingOf("e1");
        MetaCacheHandle<String, String> h = registry.bind(b);
        Assertions.assertNotNull(h);
        Assertions.assertEquals(1, registry.size());
        Assertions.assertTrue(registry.contains("e1"));
        Assertions.assertEquals(Set.of("e1"), registry.entryNames());
    }

    @Test
    public void bindIsIdempotentForSameBinding() {
        ConnectorMetaCacheBinding<String, String> b = bindingOf("e1");
        MetaCacheHandle<String, String> h1 = registry.bind(b);
        MetaCacheHandle<String, String> h2 = registry.bind(b);
        Assertions.assertSame(h1, h2);
        Assertions.assertEquals(1, registry.size());
    }

    @Test
    public void bindRejectsNullBinding() {
        Assertions.assertThrows(NullPointerException.class, () -> registry.bind(null));
    }

    @Test
    public void bindAllRegistersEverything() {
        List<ConnectorMetaCacheBinding<?, ?>> bindings = Arrays.asList(
                bindingOf("a"), bindingOf("b"), bindingOf("c"));
        int created = registry.bindAll(bindings);
        Assertions.assertEquals(3, created);
        Assertions.assertEquals(3, registry.size());
        Assertions.assertEquals(Set.of("a", "b", "c"), registry.entryNames());
    }

    @Test
    public void bindAllOnlyCountsNewEntries() {
        registry.bind(bindingOf("a"));
        int created = registry.bindAll(Arrays.asList(bindingOf("a"), bindingOf("b")));
        Assertions.assertEquals(1, created);
        Assertions.assertEquals(2, registry.size());
    }

    @Test
    public void bindAllRejectsNullCollection() {
        Assertions.assertThrows(NullPointerException.class, () -> registry.bindAll(null));
    }

    @Test
    public void bindAllRejectsNullElement() {
        List<ConnectorMetaCacheBinding<?, ?>> bindings = Arrays.asList(bindingOf("a"), null);
        Assertions.assertThrows(IllegalArgumentException.class, () -> registry.bindAll(bindings));
    }

    @Test
    public void bindAllOnEmptyIterableIsNoOp() {
        Assertions.assertEquals(0, registry.bindAll(Collections.emptyList()));
        Assertions.assertEquals(0, registry.size());
    }

    @Test
    public void detachInvalidatesEveryBindingAndClearsRegistry() {
        MetaCacheHandle<String, String> h1 = registry.bind(bindingOf("a"));
        MetaCacheHandle<String, String> h2 = registry.bind(bindingOf("b"));
        h1.put("k", "v1");
        h2.put("k", "v2");
        Assertions.assertTrue(h1.getIfPresent("k").isPresent());
        Assertions.assertTrue(h2.getIfPresent("k").isPresent());

        registry.detach();

        Assertions.assertEquals(0, registry.size());
        Assertions.assertFalse(registry.contains("a"));
        Assertions.assertFalse(registry.contains("b"));
        Assertions.assertFalse(h1.getIfPresent("k").isPresent());
        Assertions.assertFalse(h2.getIfPresent("k").isPresent());
    }

    @Test
    public void detachIsIdempotent() {
        registry.bind(bindingOf("a"));
        registry.detach();
        Assertions.assertDoesNotThrow(() -> registry.detach());
        Assertions.assertEquals(0, registry.size());
    }

    @Test
    public void detachAllowsRebindingAfterwards() {
        registry.bind(bindingOf("a"));
        registry.detach();
        MetaCacheHandle<String, String> h = registry.bind(bindingOf("a"));
        Assertions.assertNotNull(h);
        Assertions.assertEquals(1, registry.size());
    }

    @Test
    public void snapshotEnumeratesEveryBinding() {
        MetaCacheHandle<String, String> h = registry.bind(bindingOf("a"));
        registry.bind(bindingOf("b"));
        // hit on cache "a" so its stats are non-trivial
        Assertions.assertEquals("v-x", h.get("x"));

        List<CacheSnapshot> snaps = registry.snapshot();
        Assertions.assertEquals(2, snaps.size());
        Set<String> names = Set.of(
                snaps.get(0).getBindingName(),
                snaps.get(1).getBindingName());
        Assertions.assertEquals(Set.of("a", "b"), names);
        for (CacheSnapshot s : snaps) {
            Assertions.assertNotNull(s.getStats());
        }
    }

    @Test
    public void snapshotOnEmptyRegistryReturnsEmptyList() {
        Assertions.assertTrue(registry.snapshot().isEmpty());
    }

    @Test
    public void entryNamesIsUnmodifiable() {
        registry.bind(bindingOf("a"));
        Set<String> names = registry.entryNames();
        Assertions.assertThrows(UnsupportedOperationException.class, () -> names.add("nope"));
    }

    @Test
    public void invalidateAllAfterBindAllClearsEverything() {
        registry.bindAll(Arrays.asList(bindingOf("a"), bindingOf("b")));
        MetaCacheHandle<String, String> ha = registry.getOrCreateCache(bindingOf("a"));
        ha.put("k", "v");
        Assertions.assertTrue(ha.getIfPresent("k").isPresent());

        registry.invalidate(InvalidateRequest.ofCatalog());
        Assertions.assertFalse(ha.getIfPresent("k").isPresent());
    }
}
