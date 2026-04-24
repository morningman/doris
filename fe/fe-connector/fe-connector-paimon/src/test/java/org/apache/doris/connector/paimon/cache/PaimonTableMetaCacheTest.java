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

import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.paimon.PaimonConnector;
import org.apache.doris.connector.paimon.PaimonConnectorProperties;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class PaimonTableMetaCacheTest {

    private FakeConnectorContext ctx;
    private PaimonConnector connector;
    private Catalog fakeCatalog;
    private Table sampleTable;

    interface CloseableCatalog extends Catalog, Closeable {
    }

    @BeforeEach
    void setUp() throws Exception {
        RowType rowType = new RowType(Arrays.asList(
                new DataField(0, "id", DataTypes.INT().notNull()),
                new DataField(1, "name", DataTypes.STRING())));
        sampleTable = Mockito.mock(Table.class);
        Mockito.when(sampleTable.rowType()).thenReturn(rowType);
        Mockito.when(sampleTable.partitionKeys()).thenReturn(Collections.emptyList());
        Mockito.when(sampleTable.primaryKeys()).thenReturn(Collections.emptyList());

        fakeCatalog = Mockito.mock(CloseableCatalog.class);
        Mockito.when(fakeCatalog.getTable(Mockito.any(Identifier.class))).thenReturn(sampleTable);

        FakePaimonBackendFactory.configureCatalogSupplier(() -> fakeCatalog);

        Map<String, String> props = new HashMap<>();
        props.put(PaimonConnectorProperties.PAIMON_CATALOG_TYPE, FakePaimonBackendFactory.TYPE);
        ctx = new FakeConnectorContext("paimon_cat", 1L);
        connector = new PaimonConnector(props, ctx);
    }

    @AfterEach
    void tearDown() throws Exception {
        connector.close();
    }

    @Test
    void getTableSchemaCachesTableInBindingHandle() throws Exception {
        ConnectorMetadata metadata = connector.getMetadata(null);
        Optional<ConnectorTableHandle> handle = metadata.getTableHandle(null, "db", "t");
        Assertions.assertTrue(handle.isPresent());

        ConnectorTableSchema first = metadata.getTableSchema(null, handle.get());
        ConnectorTableSchema second = metadata.getTableSchema(null, handle.get());

        Assertions.assertEquals(2, first.getColumns().size());
        Assertions.assertEquals(2, second.getColumns().size());
        // getTableHandle warms the entry; subsequent getTableSchema calls hit the cache.
        // Total physical loadTable calls = 1 (the cache miss during getTableHandle).
        Mockito.verify(fakeCatalog, Mockito.times(1)).getTable(Identifier.create("db", "t"));
    }

    @Test
    void distinctTablesProduceDistinctCacheEntries() throws Exception {
        ConnectorMetadata metadata = connector.getMetadata(null);
        metadata.getTableSchema(null, metadata.getTableHandle(null, "db", "a").get());
        metadata.getTableSchema(null, metadata.getTableHandle(null, "db", "b").get());
        Mockito.verify(fakeCatalog, Mockito.times(1)).getTable(Identifier.create("db", "a"));
        Mockito.verify(fakeCatalog, Mockito.times(1)).getTable(Identifier.create("db", "b"));
    }

    @Test
    void invalidateAllOnTableHandleEvictsEntries() throws Exception {
        ConnectorMetadata metadata = connector.getMetadata(null);
        metadata.getTableSchema(null, metadata.getTableHandle(null, "db", "t").get());
        InMemoryMetaCacheHandle<?, ?> tHandle = ctx.handleFor(PaimonCacheBindings.ENTRY_TABLE);
        Assertions.assertNotNull(tHandle);
        Assertions.assertEquals(1, tHandle.sizeForTest());

        tHandle.invalidateAll();
        metadata.getTableSchema(null, metadata.getTableHandle(null, "db", "t").get());
        // After invalidate, getTableHandle re-loads (miss) and puts back into cache;
        // getTableSchema then hits. Plus the original load from setUp's first call.
        // So overall getTable calls: 1 (initial handle) + 1 (post-invalidate handle) = 2.
        Mockito.verify(fakeCatalog, Mockito.times(2)).getTable(Identifier.create("db", "t"));
    }

    @Test
    void hitMissCountersTracked() throws Exception {
        ConnectorMetadata metadata = connector.getMetadata(null);
        metadata.getTableSchema(null, metadata.getTableHandle(null, "db", "t").get());
        metadata.getTableSchema(null, metadata.getTableHandle(null, "db", "t").get());
        InMemoryMetaCacheHandle<?, ?> tHandle = ctx.handleFor(PaimonCacheBindings.ENTRY_TABLE);
        Assertions.assertEquals(1, tHandle.stats().getLoadSuccessCount());
        Assertions.assertTrue(tHandle.stats().getHitCount() >= 1);
    }
}
