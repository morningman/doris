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

package org.apache.doris.connector.iceberg.cache;

import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.iceberg.IcebergConnector;
import org.apache.doris.connector.iceberg.IcebergConnectorProperties;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.Closeable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class IcebergTableMetaCacheTest {

    private FakeConnectorContext ctx;
    private IcebergConnector connector;
    private Catalog fakeCatalog;
    private Table sampleTable;

    interface CloseableCatalog extends Catalog, Closeable {
    }

    @BeforeEach
    void setUp() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()));
        sampleTable = Mockito.mock(Table.class);
        Mockito.when(sampleTable.schema()).thenReturn(schema);
        Mockito.when(sampleTable.properties()).thenReturn(Collections.emptyMap());
        Mockito.when(sampleTable.snapshots()).thenReturn(Collections.emptyList());
        org.apache.iceberg.PartitionSpec spec = org.apache.iceberg.PartitionSpec.unpartitioned();
        Mockito.when(sampleTable.spec()).thenReturn(spec);

        fakeCatalog = Mockito.mock(CloseableCatalog.class);
        Mockito.when(fakeCatalog.loadTable(Mockito.any(TableIdentifier.class))).thenReturn(sampleTable);
        Mockito.when(fakeCatalog.tableExists(Mockito.any(TableIdentifier.class))).thenReturn(true);

        FakeIcebergBackendFactory.configureCatalogSupplier(() -> fakeCatalog);

        Map<String, String> props = new HashMap<>();
        props.put(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE, FakeIcebergBackendFactory.TYPE);
        ctx = new FakeConnectorContext("ice_cat", 1L);
        connector = new IcebergConnector(props, ctx);
    }

    @AfterEach
    void tearDown() throws Exception {
        connector.close();
    }

    @Test
    void getTableSchemaCachesTableInBindingHandle() {
        ConnectorMetadata metadata = connector.getMetadata(null);
        Optional<ConnectorTableHandle> handle = metadata.getTableHandle(null, "db", "t");
        Assertions.assertTrue(handle.isPresent());

        ConnectorTableSchema first = metadata.getTableSchema(null, handle.get());
        ConnectorTableSchema second = metadata.getTableSchema(null, handle.get());

        Assertions.assertEquals(2, first.getColumns().size());
        Assertions.assertEquals(2, second.getColumns().size());
        // Loader runs once per (db, t) — second access is a hit.
        Mockito.verify(fakeCatalog, Mockito.times(1)).loadTable(TableIdentifier.of("db", "t"));
    }

    @Test
    void distinctTablesProduceDistinctCacheEntries() {
        ConnectorMetadata metadata = connector.getMetadata(null);
        metadata.getTableSchema(null, metadata.getTableHandle(null, "db", "a").get());
        metadata.getTableSchema(null, metadata.getTableHandle(null, "db", "b").get());
        Mockito.verify(fakeCatalog, Mockito.times(1)).loadTable(TableIdentifier.of("db", "a"));
        Mockito.verify(fakeCatalog, Mockito.times(1)).loadTable(TableIdentifier.of("db", "b"));
    }

    @Test
    void invalidateAllOnTableHandleEvictsEntries() {
        ConnectorMetadata metadata = connector.getMetadata(null);
        metadata.getTableSchema(null, metadata.getTableHandle(null, "db", "t").get());
        InMemoryMetaCacheHandle<?, ?> tHandle = ctx.handleFor(IcebergCacheBindings.ENTRY_TABLE);
        Assertions.assertNotNull(tHandle);
        Assertions.assertEquals(1, tHandle.sizeForTest());

        tHandle.invalidateAll();
        metadata.getTableSchema(null, metadata.getTableHandle(null, "db", "t").get());
        Mockito.verify(fakeCatalog, Mockito.times(2)).loadTable(TableIdentifier.of("db", "t"));
    }

    @Test
    void hitMissCountersTracked() {
        ConnectorMetadata metadata = connector.getMetadata(null);
        metadata.getTableSchema(null, metadata.getTableHandle(null, "db", "t").get());
        metadata.getTableSchema(null, metadata.getTableHandle(null, "db", "t").get());
        InMemoryMetaCacheHandle<?, ?> tHandle = ctx.handleFor(IcebergCacheBindings.ENTRY_TABLE);
        Assertions.assertEquals(1, tHandle.stats().getLoadSuccessCount());
        Assertions.assertTrue(tHandle.stats().getHitCount() >= 1);
    }
}
