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

import org.apache.doris.connector.api.cache.ConnectorMetaCacheBinding;
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
import java.util.List;
import java.util.Map;

class PaimonConnectorBindingsTest {

    private FakeConnectorContext ctx;
    private PaimonConnector connector;
    private CloseableCatalog fakeCatalog;

    @BeforeEach
    void setUp() throws Exception {
        Table table = Mockito.mock(Table.class);
        RowType rowType = new RowType(Arrays.asList(
                new DataField(0, "id", DataTypes.INT().notNull())));
        Mockito.when(table.rowType()).thenReturn(rowType);
        Mockito.when(table.partitionKeys()).thenReturn(Collections.emptyList());
        Mockito.when(table.primaryKeys()).thenReturn(Collections.emptyList());

        fakeCatalog = Mockito.mock(CloseableCatalog.class);
        Mockito.when(fakeCatalog.getTable(Mockito.any(Identifier.class))).thenReturn(table);

        FakePaimonBackendFactory.configureCatalogSupplier(() -> fakeCatalog);

        Map<String, String> props = new HashMap<>();
        props.put(PaimonConnectorProperties.PAIMON_CATALOG_TYPE, FakePaimonBackendFactory.TYPE);
        ctx = new FakeConnectorContext("paimon_cat", 42L);
        connector = new PaimonConnector(props, ctx);
    }

    @AfterEach
    void tearDown() throws Exception {
        connector.close();
    }

    @Test
    void declaresThreeBindingsWithExpectedNames() {
        List<ConnectorMetaCacheBinding<?, ?>> bindings = connector.getMetaCacheBindings();
        Assertions.assertEquals(3, bindings.size());
        Assertions.assertEquals(PaimonCacheBindings.ENTRY_CATALOG, bindings.get(0).getEntryName());
        Assertions.assertEquals(PaimonCacheBindings.ENTRY_TABLE, bindings.get(1).getEntryName());
        Assertions.assertEquals(PaimonCacheBindings.ENTRY_SNAPSHOTS, bindings.get(2).getEntryName());
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
        Assertions.assertNotNull(ctx.handleFor(PaimonCacheBindings.ENTRY_CATALOG));
        Assertions.assertNotNull(ctx.handleFor(PaimonCacheBindings.ENTRY_TABLE));
        Assertions.assertNotNull(ctx.handleFor(PaimonCacheBindings.ENTRY_SNAPSHOTS));
    }

    @Test
    void catalogBuiltOnceForRepeatedAccess() {
        connector.getOrCreateCatalog();
        connector.getOrCreateCatalog();
        connector.getOrCreateCatalog();
        Assertions.assertEquals(1, FakePaimonBackendFactory.buildInvocationCount());
    }

    @Test
    void getMetadataReusesCachedCatalog() {
        connector.getMetadata(null);
        connector.getMetadata(null);
        Assertions.assertEquals(1, FakePaimonBackendFactory.buildInvocationCount());
    }

    @Test
    void closeInvalidatesCatalogHandleAndClosesUnderlyingCatalog() throws Exception {
        connector.getOrCreateCatalog();
        connector.close();
        Mockito.verify(fakeCatalog).close();
        // After close, the catalog handle has been emptied; next access reloads.
        connector.getOrCreateCatalog();
        Assertions.assertEquals(2, FakePaimonBackendFactory.buildInvocationCount());
    }

    @Test
    void unknownCatalogTypeIsRejected() {
        Map<String, String> props = new HashMap<>();
        props.put(PaimonConnectorProperties.PAIMON_CATALOG_TYPE, "no-such-backend");
        Assertions.assertThrows(RuntimeException.class,
                () -> new PaimonConnector(props, new FakeConnectorContext("c", 1L)));
    }

    @Test
    void backendResolvedEagerlyAtConstruction() {
        Assertions.assertNotNull(connector.getBackend());
        Assertions.assertEquals(FakePaimonBackendFactory.TYPE, connector.getBackend().name());
    }

    /** Closeable + Catalog so Mockito mock can verify the close-on-removal path. */
    interface CloseableCatalog extends Catalog, Closeable {
    }
}
