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

package org.apache.doris.connector.api.cache;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConnectorMetaCacheBindingTest {

    private static final CacheLoader<String, Integer> LOADER = key -> key.length();

    @Test
    public void builderHappyPathWithDefaults() {
        ConnectorMetaCacheBinding<String, Integer> b =
                ConnectorMetaCacheBinding.builder("entry", String.class, Integer.class, LOADER).build();
        Assertions.assertEquals("entry", b.getEntryName());
        Assertions.assertEquals(String.class, b.getKeyType());
        Assertions.assertEquals(Integer.class, b.getValueType());
        Assertions.assertSame(LOADER, b.getLoader());
        Assertions.assertEquals(ConnectorCacheSpec.defaults(), b.getDefaultSpec());
        Assertions.assertNotNull(b.getInvalidationStrategy());
        Assertions.assertTrue(b.getInvalidationStrategy().appliesTo(InvalidateRequest.ofCatalog()));
        Assertions.assertFalse(b.getWeigher().isPresent());
        Assertions.assertFalse(b.getRemovalListener().isPresent());
    }

    @Test
    public void optionalFieldsRoundTrip() {
        Weigher<String, Integer> weigher = (k, v) -> 1;
        RemovalListener<String, Integer> listener = (k, v, c) -> { };
        ConnectorCacheSpec spec = ConnectorCacheSpec.builder().maxSize(5).build();
        ConnectorMetaCacheInvalidation strategy =
                ConnectorMetaCacheInvalidation.byScope(InvalidateScope.TABLE);

        ConnectorMetaCacheBinding<String, Integer> b =
                ConnectorMetaCacheBinding.builder("e", String.class, Integer.class, LOADER)
                        .defaultSpec(spec)
                        .invalidationStrategy(strategy)
                        .weigher(weigher)
                        .removalListener(listener)
                        .build();
        Assertions.assertSame(spec, b.getDefaultSpec());
        Assertions.assertSame(strategy, b.getInvalidationStrategy());
        Assertions.assertSame(weigher, b.getWeigher().orElse(null));
        Assertions.assertSame(listener, b.getRemovalListener().orElse(null));
    }

    @Test
    public void rejectsEmptyEntryName() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorMetaCacheBinding.builder("", String.class, Integer.class, LOADER).build());
    }

    @Test
    public void rejectsNullRequired() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorMetaCacheBinding.builder("e", null, Integer.class, LOADER).build());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorMetaCacheBinding.builder("e", String.class, null, LOADER).build());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorMetaCacheBinding.builder("e", String.class, Integer.class, null).build());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorMetaCacheBinding.builder(null, String.class, Integer.class, LOADER).build());
    }

    @Test
    public void equalsAndHashCode() {
        ConnectorMetaCacheBinding<String, Integer> a =
                ConnectorMetaCacheBinding.builder("e", String.class, Integer.class, LOADER).build();
        ConnectorMetaCacheBinding<String, Integer> b =
                ConnectorMetaCacheBinding.builder("e", String.class, Integer.class, LOADER).build();
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        Assertions.assertTrue(a.toString().contains("entryName=e"));
    }
}
