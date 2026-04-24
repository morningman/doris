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

import org.apache.doris.connector.DefaultConnectorContext;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.cache.CacheSnapshot;
import org.apache.doris.connector.api.cache.ConnectorCacheSpec;
import org.apache.doris.connector.api.cache.ConnectorMetaCacheBinding;
import org.apache.doris.connector.api.cache.ConnectorMetaCacheInvalidation;
import org.apache.doris.connector.api.cache.InvalidateRequest;
import org.apache.doris.connector.api.cache.MetaCacheHandle;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * End-to-end "wire is connected" verification for M1-09: a fake
 * {@link Connector} implementation declares two bindings via
 * {@link Connector#getMetaCacheBindings()}; the engine-side
 * {@link ConnectorMetaCacheBootstrap} pulls them into the registry of a
 * {@link DefaultConnectorContext}; the test then exercises the resulting
 * cache handles through the SPI surface.
 *
 * <p>This test deliberately does NOT spin up a real {@code
 * PluginDrivenExternalCatalog} (which would drag in {@code Env}, the
 * catalog manager, and image replay machinery). Instead it covers the
 * narrow contract that M1-09 owns: <i>given a {@link Connector} returned
 * by a plugin, fe-core wires its declared bindings into the registry,
 * tracks them across re-bootstrap, and tears them down on detach</i>.</p>
 */
public class MetaCacheRegistryFakePluginIntegrationTest {

    private static ConnectorMetaCacheBinding<String, String> tableBinding(AtomicInteger calls) {
        return ConnectorMetaCacheBinding
                .builder("fake.tables", String.class, String.class, k -> {
                    calls.incrementAndGet();
                    return "value-of-" + k;
                })
                .invalidationStrategy(ConnectorMetaCacheInvalidation.always())
                .defaultSpec(ConnectorCacheSpec.builder()
                        .maxSize(100)
                        .ttl(Duration.ofMinutes(5))
                        .build())
                .build();
    }

    private static ConnectorMetaCacheBinding<String, String> partitionBinding() {
        return ConnectorMetaCacheBinding
                .builder("fake.partitions", String.class, String.class, k -> "p-" + k)
                .invalidationStrategy(ConnectorMetaCacheInvalidation.always())
                .build();
    }

    /** A minimal fake plugin {@link Connector} that returns the supplied bindings. */
    private static class FakeConnector implements Connector {
        private final List<ConnectorMetaCacheBinding<?, ?>> bindings;

        FakeConnector(List<ConnectorMetaCacheBinding<?, ?>> bindings) {
            this.bindings = bindings;
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return null;
        }

        @Override
        public List<ConnectorMetaCacheBinding<?, ?>> getMetaCacheBindings() {
            return bindings;
        }

        @Override
        public void close() {
        }
    }

    @Test
    public void bootstrapBindsEveryDeclaredEntry() {
        AtomicInteger calls = new AtomicInteger();
        Connector plugin = new FakeConnector(List.of(tableBinding(calls), partitionBinding()));
        DefaultConnectorContext ctx = new DefaultConnectorContext("fake-cat", 42L);

        int created = ConnectorMetaCacheBootstrap.bindAll(ctx, plugin);

        Assertions.assertEquals(2, created);
        ConnectorMetaCacheRegistry reg = ctx.getCacheRegistry();
        Assertions.assertEquals(2, reg.size());
        Assertions.assertTrue(reg.contains("fake.tables"));
        Assertions.assertTrue(reg.contains("fake.partitions"));
    }

    @Test
    public void cacheLoaderRunsThroughBindingSpec() {
        AtomicInteger calls = new AtomicInteger();
        ConnectorMetaCacheBinding<String, String> tableBnd = tableBinding(calls);
        Connector plugin = new FakeConnector(List.of(tableBnd));
        DefaultConnectorContext ctx = new DefaultConnectorContext("fake-cat", 1L);
        ConnectorMetaCacheBootstrap.bindAll(ctx, plugin);

        MetaCacheHandle<String, String> h = ctx.getOrCreateCache(tableBnd);
        Assertions.assertEquals("value-of-x", h.get("x"));
        Assertions.assertEquals("value-of-x", h.get("x"));
        Assertions.assertEquals(1, calls.get(), "loader should only run once for repeated reads");
    }

    @Test
    public void invalidateRequestReachesBoundCache() {
        ConnectorMetaCacheBinding<String, String> tableBnd = tableBinding(new AtomicInteger());
        Connector plugin = new FakeConnector(List.of(tableBnd));
        DefaultConnectorContext ctx = new DefaultConnectorContext("fake-cat", 1L);
        ConnectorMetaCacheBootstrap.bindAll(ctx, plugin);

        MetaCacheHandle<String, String> h = ctx.getOrCreateCache(tableBnd);
        h.put("k", "v");
        Assertions.assertTrue(h.getIfPresent("k").isPresent());

        ctx.invalidate(InvalidateRequest.ofCatalog());

        Assertions.assertFalse(h.getIfPresent("k").isPresent());
    }

    @Test
    public void detachRemovesEveryBoundEntry() {
        Connector plugin = new FakeConnector(
                List.of(tableBinding(new AtomicInteger()), partitionBinding()));
        DefaultConnectorContext ctx = new DefaultConnectorContext("fake-cat", 1L);
        ConnectorMetaCacheBootstrap.bindAll(ctx, plugin);
        Assertions.assertEquals(2, ctx.getCacheRegistry().size());

        ConnectorMetaCacheBootstrap.detachAll(ctx);

        Assertions.assertEquals(0, ctx.getCacheRegistry().size());
        Assertions.assertFalse(ctx.getCacheRegistry().contains("fake.tables"));
    }

    @Test
    public void rebootstrapAfterDetachRestoresBindings() {
        Connector plugin = new FakeConnector(List.of(tableBinding(new AtomicInteger())));
        DefaultConnectorContext ctx = new DefaultConnectorContext("fake-cat", 1L);
        ConnectorMetaCacheBootstrap.bindAll(ctx, plugin);
        ConnectorMetaCacheBootstrap.detachAll(ctx);

        // Same plugin, same bindings: a second pass through the bootstrap re-creates the registry.
        int created = ConnectorMetaCacheBootstrap.bindAll(ctx, plugin);
        Assertions.assertEquals(1, created);
        Assertions.assertEquals(1, ctx.getCacheRegistry().size());
    }

    @Test
    public void emptyBindingsListBindsNothing() {
        Connector plugin = new FakeConnector(Collections.emptyList());
        DefaultConnectorContext ctx = new DefaultConnectorContext("fake-cat", 1L);
        Assertions.assertEquals(0, ConnectorMetaCacheBootstrap.bindAll(ctx, plugin));
        Assertions.assertEquals(0, ctx.getCacheRegistry().size());
    }

    @Test
    public void nullBindingsListFromPluginIsTolerated() {
        // Defend against custom Connector impls that return null from the default-overrideable method.
        Connector plugin = new FakeConnector(null) {
            @Override
            public List<ConnectorMetaCacheBinding<?, ?>> getMetaCacheBindings() {
                return null;
            }
        };
        DefaultConnectorContext ctx = new DefaultConnectorContext("fake-cat", 1L);
        Assertions.assertEquals(0, ConnectorMetaCacheBootstrap.bindAll(ctx, plugin));
    }

    @Test
    public void snapshotAfterBootstrapEnumeratesPluginBindings() {
        Connector plugin = new FakeConnector(
                List.of(tableBinding(new AtomicInteger()), partitionBinding()));
        DefaultConnectorContext ctx = new DefaultConnectorContext("fake-cat", 1L);
        ConnectorMetaCacheBootstrap.bindAll(ctx, plugin);

        List<CacheSnapshot> snaps = ctx.getCacheRegistry().snapshot();
        Assertions.assertEquals(2, snaps.size());
    }

    @Test
    public void bootstrapRejectsNullArguments() {
        DefaultConnectorContext ctx = new DefaultConnectorContext("fake-cat", 1L);
        Assertions.assertThrows(NullPointerException.class,
                () -> ConnectorMetaCacheBootstrap.bindAll(null, new FakeConnector(List.of())));
        Assertions.assertThrows(NullPointerException.class,
                () -> ConnectorMetaCacheBootstrap.bindAll(ctx, null));
        Assertions.assertThrows(NullPointerException.class,
                () -> ConnectorMetaCacheBootstrap.detachAll(null));
    }
}
