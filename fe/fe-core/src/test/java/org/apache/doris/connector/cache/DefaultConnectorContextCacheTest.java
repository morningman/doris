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
import org.apache.doris.connector.api.cache.ConnectorMetaCacheBinding;
import org.apache.doris.connector.api.cache.InvalidateRequest;
import org.apache.doris.connector.api.cache.MetaCacheHandle;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DefaultConnectorContextCacheTest {

    @Test
    public void getOrCreateCacheReturnsWorkingHandle() {
        DefaultConnectorContext ctx = new DefaultConnectorContext("cat", 1L);
        ConnectorMetaCacheBinding<String, String> b =
                ConnectorMetaCacheBinding.builder("e1", String.class, String.class, k -> "v-" + k).build();

        MetaCacheHandle<String, String> h = ctx.getOrCreateCache(b);
        Assertions.assertNotNull(h);
        Assertions.assertEquals("v-k", h.get("k"));

        Assertions.assertNotNull(ctx.getCacheRegistry());
        Assertions.assertEquals(1, ctx.getCacheRegistry().size());
        Assertions.assertTrue(ctx.getCacheRegistry().contains("e1"));
    }

    @Test
    public void invalidateDelegatesToRegistry() {
        DefaultConnectorContext ctx = new DefaultConnectorContext("cat", 1L);
        ConnectorMetaCacheBinding<String, String> b =
                ConnectorMetaCacheBinding.builder("e1", String.class, String.class, k -> "v").build();

        MetaCacheHandle<String, String> h = ctx.getOrCreateCache(b);
        h.put("k", "v1");
        Assertions.assertTrue(h.getIfPresent("k").isPresent());

        ctx.invalidate(InvalidateRequest.ofCatalog());
        Assertions.assertFalse(h.getIfPresent("k").isPresent());
    }

    @Test
    public void invalidateAllClearsEveryBinding() {
        DefaultConnectorContext ctx = new DefaultConnectorContext("cat", 1L);
        ConnectorMetaCacheBinding<String, String> b1 =
                ConnectorMetaCacheBinding.builder("e1", String.class, String.class, k -> "v").build();
        ConnectorMetaCacheBinding<String, String> b2 =
                ConnectorMetaCacheBinding.builder("e2", String.class, String.class, k -> "v").build();

        MetaCacheHandle<String, String> h1 = ctx.getOrCreateCache(b1);
        MetaCacheHandle<String, String> h2 = ctx.getOrCreateCache(b2);
        h1.put("k", "v");
        h2.put("k", "v");

        ctx.invalidateAll();
        Assertions.assertFalse(h1.getIfPresent("k").isPresent());
        Assertions.assertFalse(h2.getIfPresent("k").isPresent());
    }
}
