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

import org.apache.doris.connector.api.cache.ConnectorMetaCacheBinding;
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
import java.util.List;
import java.util.Map;

class IcebergConnectorBindingsTest {

    private FakeConnectorContext ctx;
    private IcebergConnector connector;
    private CloseableCatalog fakeCatalog;

    @BeforeEach
    void setUp() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.snapshots()).thenReturn(Collections.emptyList());

        fakeCatalog = Mockito.mock(CloseableCatalog.class);
        Mockito.when(fakeCatalog.loadTable(Mockito.any(TableIdentifier.class))).thenReturn(table);

        FakeIcebergBackendFactory.configureCatalogSupplier(() -> fakeCatalog);

        Map<String, String> props = new HashMap<>();
        props.put(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE, FakeIcebergBackendFactory.TYPE);
        ctx = new FakeConnectorContext("ice_cat", 42L);
        connector = new IcebergConnector(props, ctx);
    }

    @AfterEach
    void tearDown() throws Exception {
        connector.close();
    }

    @Test
    void declaresThreeBindingsWithExpectedNames() {
        List<ConnectorMetaCacheBinding<?, ?>> bindings = connector.getMetaCacheBindings();
        Assertions.assertEquals(3, bindings.size());
        Assertions.assertEquals(IcebergCacheBindings.ENTRY_CATALOG, bindings.get(0).getEntryName());
        Assertions.assertEquals(IcebergCacheBindings.ENTRY_TABLE, bindings.get(1).getEntryName());
        Assertions.assertEquals(IcebergCacheBindings.ENTRY_SNAPSHOTS, bindings.get(2).getEntryName());
    }

    @Test
    void getMetaCacheBindingsReturnsStableInstances() {
        List<ConnectorMetaCacheBinding<?, ?>> first = connector.getMetaCacheBindings();
        List<ConnectorMetaCacheBinding<?, ?>> second = connector.getMetaCacheBindings();
        for (int i = 0; i < first.size(); i++) {
            Assertions.assertSame(first.get(i), second.get(i),
                    "binding instances must be stable across calls");
        }
    }

    @Test
    void firstCatalogAccessRegistersAllThreeHandles() {
        Catalog c = connector.getOrCreateCatalog();
        Assertions.assertSame(fakeCatalog, c);
        Assertions.assertEquals(3, ctx.registeredHandleCount(),
                "all three bindings must be wired through ConnectorContext.getOrCreateCache");
        Assertions.assertNotNull(ctx.handleFor(IcebergCacheBindings.ENTRY_CATALOG));
        Assertions.assertNotNull(ctx.handleFor(IcebergCacheBindings.ENTRY_TABLE));
        Assertions.assertNotNull(ctx.handleFor(IcebergCacheBindings.ENTRY_SNAPSHOTS));
    }

    @Test
    void catalogBuiltOnceForRepeatedAccess() {
        connector.getOrCreateCatalog();
        connector.getOrCreateCatalog();
        connector.getOrCreateCatalog();
        Assertions.assertEquals(1, FakeIcebergBackendFactory.buildInvocationCount());
    }

    @Test
    void getMetadataReusesCachedCatalog() {
        connector.getMetadata(null);
        connector.getMetadata(null);
        Assertions.assertEquals(1, FakeIcebergBackendFactory.buildInvocationCount());
    }

    @Test
    void closeInvalidatesCatalogHandleAndClosesUnderlyingCatalog() throws Exception {
        connector.getOrCreateCatalog();
        connector.close();
        Mockito.verify(fakeCatalog).close();
        // After close, the catalog handle has been emptied; next access reloads.
        connector.getOrCreateCatalog();
        Assertions.assertEquals(2, FakeIcebergBackendFactory.buildInvocationCount());
    }

    @Test
    void rejectsMissingCatalogTypeProperty() {
        Assertions.assertThrows(RuntimeException.class,
                () -> new IcebergConnector(Collections.emptyMap(), new FakeConnectorContext("c", 1L)));
    }

    @Test
    void rejectsUnknownCatalogType() {
        Map<String, String> props = new HashMap<>();
        props.put(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE, "no-such-backend");
        Assertions.assertThrows(RuntimeException.class,
                () -> new IcebergConnector(props, new FakeConnectorContext("c", 1L)));
    }

    @Test
    void capabilitiesIncludeTimeTravelAndMvccSnapshot() {
        Assertions.assertTrue(connector.getCapabilities().contains(
                org.apache.doris.connector.api.ConnectorCapability.SUPPORTS_TIME_TRAVEL));
        Assertions.assertTrue(connector.getCapabilities().contains(
                org.apache.doris.connector.api.ConnectorCapability.SUPPORTS_MVCC_SNAPSHOT));
    }

    /** Closeable + Catalog so Mockito mock can verify the close-on-removal path. */
    interface CloseableCatalog extends Catalog, Closeable {
    }
}
