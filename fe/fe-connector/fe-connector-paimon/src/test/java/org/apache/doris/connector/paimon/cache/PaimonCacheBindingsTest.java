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

package org.apache.doris.connector.paimon.cache;

import org.apache.doris.connector.api.ConnectorTableId;
import org.apache.doris.connector.api.cache.CacheLoader;
import org.apache.doris.connector.api.cache.ConnectorMetaCacheBinding;
import org.apache.doris.connector.api.cache.InvalidateRequest;
import org.apache.doris.connector.api.cache.RefreshPolicy;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.table.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

class PaimonCacheBindingsTest {

    @Test
    void catalogBindingMetadata() {
        CacheLoader<String, Catalog> loader = key -> {
            throw new AssertionError("should not be invoked");
        };
        ConnectorMetaCacheBinding<String, Catalog> b = PaimonCacheBindings.catalogBinding(loader);
        Assertions.assertEquals("paimon.catalog", b.getEntryName());
        Assertions.assertEquals(String.class, b.getKeyType());
        Assertions.assertEquals(Catalog.class, b.getValueType());
        Assertions.assertEquals(4L, b.getDefaultSpec().getMaxSize());
        Assertions.assertEquals(RefreshPolicy.MANUAL_ONLY, b.getDefaultSpec().getRefreshPolicy());
        Assertions.assertTrue(b.getRemovalListener().isPresent(),
                "catalog binding must register a closing removal listener");
    }

    @Test
    void tableBindingMetadata() {
        CacheLoader<PaimonTableCacheKey, Table> loader = key -> {
            throw new AssertionError("should not be invoked");
        };
        ConnectorMetaCacheBinding<PaimonTableCacheKey, Table> b =
                PaimonCacheBindings.tableBinding(loader);
        Assertions.assertEquals("paimon.table", b.getEntryName());
        Assertions.assertEquals(PaimonTableCacheKey.class, b.getKeyType());
        Assertions.assertEquals(Table.class, b.getValueType());
        Assertions.assertEquals(10_000L, b.getDefaultSpec().getMaxSize());
        Assertions.assertEquals(Duration.ofHours(1), b.getDefaultSpec().getTtl());
        Assertions.assertEquals(RefreshPolicy.TTL, b.getDefaultSpec().getRefreshPolicy());
    }

    @Test
    void snapshotsBindingMetadata() {
        CacheLoader<PaimonTableCacheKey, List<Snapshot>> loader = key -> Collections.emptyList();
        ConnectorMetaCacheBinding<PaimonTableCacheKey, List<Snapshot>> b =
                PaimonCacheBindings.snapshotsBinding(loader);
        Assertions.assertEquals("paimon.snapshots", b.getEntryName());
        Assertions.assertEquals(PaimonTableCacheKey.class, b.getKeyType());
        Assertions.assertEquals(Duration.ofMinutes(5), b.getDefaultSpec().getTtl());
    }

    @Test
    void catalogBindingReactsOnlyToCatalogScope() {
        ConnectorMetaCacheBinding<String, Catalog> b =
                PaimonCacheBindings.catalogBinding(key -> null);
        Assertions.assertTrue(b.getInvalidationStrategy().appliesTo(InvalidateRequest.ofCatalog()));
        Assertions.assertFalse(b.getInvalidationStrategy().appliesTo(
                InvalidateRequest.ofTable(ConnectorTableId.of("db", "t"))));
        Assertions.assertFalse(b.getInvalidationStrategy().appliesTo(
                InvalidateRequest.ofDatabase("db")));
    }

    @Test
    void tableBindingReactsToTableDatabaseAndCatalogScopes() {
        ConnectorMetaCacheBinding<PaimonTableCacheKey, Table> b =
                PaimonCacheBindings.tableBinding(key -> null);
        Assertions.assertTrue(b.getInvalidationStrategy().appliesTo(InvalidateRequest.ofCatalog()));
        Assertions.assertTrue(b.getInvalidationStrategy().appliesTo(
                InvalidateRequest.ofDatabase("db")));
        Assertions.assertTrue(b.getInvalidationStrategy().appliesTo(
                InvalidateRequest.ofTable(ConnectorTableId.of("db", "t"))));
        Assertions.assertFalse(b.getInvalidationStrategy().appliesTo(
                InvalidateRequest.ofPartitions(ConnectorTableId.of("db", "t"), Collections.singletonList("p=1"))));
        Assertions.assertFalse(b.getInvalidationStrategy().appliesTo(
                InvalidateRequest.ofSysTable(ConnectorTableId.of("db", "t"), "snapshots")));
    }

    @Test
    void snapshotsBindingReactsToTableDatabaseAndCatalogScopes() {
        ConnectorMetaCacheBinding<PaimonTableCacheKey, List<Snapshot>> b =
                PaimonCacheBindings.snapshotsBinding(key -> Collections.emptyList());
        Assertions.assertTrue(b.getInvalidationStrategy().appliesTo(InvalidateRequest.ofCatalog()));
        Assertions.assertTrue(b.getInvalidationStrategy().appliesTo(
                InvalidateRequest.ofTable(ConnectorTableId.of("db", "t"))));
        Assertions.assertFalse(b.getInvalidationStrategy().appliesTo(
                InvalidateRequest.ofPartitions(ConnectorTableId.of("db", "t"), Collections.emptyList())));
    }

    @Test
    void rejectsNullLoader() {
        Assertions.assertThrows(NullPointerException.class,
                () -> PaimonCacheBindings.catalogBinding(null));
        Assertions.assertThrows(NullPointerException.class,
                () -> PaimonCacheBindings.tableBinding(null));
        Assertions.assertThrows(NullPointerException.class,
                () -> PaimonCacheBindings.snapshotsBinding(null));
    }
}
