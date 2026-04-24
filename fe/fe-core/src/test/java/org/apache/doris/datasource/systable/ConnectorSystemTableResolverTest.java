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

package org.apache.doris.datasource.systable;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.systable.ConnectorScanPlanProviderRef;
import org.apache.doris.connector.api.systable.SysTableExecutionMode;
import org.apache.doris.connector.api.systable.SysTableSpec;
import org.apache.doris.connector.api.systable.TvfInvocation;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalDatabase;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * Unit tests for {@link SysTableResolver} plugin / legacy routing.
 */
public class ConnectorSystemTableResolverTest {

    private static final ConnectorTableSchema EMPTY_SCHEMA = new ConnectorTableSchema(
            "tbl",
            Collections.singletonList(
                    new ConnectorColumn("id", ConnectorType.of("INT"), null, true, null)),
            "iceberg",
            Collections.emptyMap());

    // ----- plugin route -----

    @Test
    public void testPluginNativeSpecRoutedToWrapper() {
        SysTableSpec spec = SysTableSpec.builder()
                .name("snapshots")
                .schema(EMPTY_SCHEMA)
                .mode(SysTableExecutionMode.NATIVE)
                .nativeFactory((db, t, n, v) -> Mockito.mock(ConnectorScanPlanProvider.class))
                .build();
        PluginDrivenExternalTable table = newPluginTable(spec);
        Optional<SysTableResolver.SysTablePlan> plan = SysTableResolver.resolveForPlan(
                table, "ctl", "db", "tbl$snapshots");
        Assertions.assertTrue(plan.isPresent());
        Assertions.assertTrue(plan.get().isNative());
        Assertions.assertTrue(plan.get().isPluginManaged());
        Assertions.assertTrue(plan.get().getSysExternalTable() instanceof NativeSysExternalTable);
        Assertions.assertEquals(spec, plan.get().getSysTableSpec().orElseThrow());
    }

    @Test
    public void testPluginTvfSpecRoutedToConnectorManagedWrapper() {
        SysTableSpec spec = SysTableSpec.builder()
                .name("partitions")
                .schema(EMPTY_SCHEMA)
                .mode(SysTableExecutionMode.TVF)
                .tvfInvoker((db, t, n, v) -> new TvfInvocation("partitions", Collections.emptyMap()))
                .build();
        PluginDrivenExternalTable table = newPluginTable(spec);
        Optional<SysTableResolver.SysTablePlan> plan = SysTableResolver.resolveForPlan(
                table, "ctl", "db", "tbl$partitions");
        Assertions.assertTrue(plan.isPresent());
        Assertions.assertTrue(plan.get().isPluginManaged());
        Assertions.assertTrue(plan.get().getSysExternalTable() instanceof ConnectorManagedSysExternalTable);
    }

    @Test
    public void testPluginScanPlanSpecRoutedToConnectorManagedWrapper() {
        SysTableSpec spec = SysTableSpec.builder()
                .name("plan")
                .schema(EMPTY_SCHEMA)
                .mode(SysTableExecutionMode.CONNECTOR_SCAN_PLAN)
                .scanPlanProviderRef(ConnectorScanPlanProviderRef.of("provider-1"))
                .build();
        PluginDrivenExternalTable table = newPluginTable(spec);
        Optional<SysTableResolver.SysTableDescribe> describe = SysTableResolver.resolveForDescribe(
                table, "ctl", "db", "tbl$plan");
        Assertions.assertTrue(describe.isPresent());
        Assertions.assertTrue(describe.get().isPluginManaged());
        Assertions.assertTrue(describe.get().getSysExternalTable() instanceof ConnectorManagedSysExternalTable);
    }

    @Test
    public void testValidateForQueryAcceptsPluginSysTable() {
        SysTableSpec spec = SysTableSpec.builder()
                .name("snapshots")
                .schema(EMPTY_SCHEMA)
                .mode(SysTableExecutionMode.NATIVE)
                .nativeFactory((db, t, n, v) -> Mockito.mock(ConnectorScanPlanProvider.class))
                .build();
        PluginDrivenExternalTable table = newPluginTable(spec);
        Assertions.assertTrue(SysTableResolver.validateForQuery(table, "ctl", "db", "tbl$snapshots"));
    }

    @Test
    public void testPluginUnknownSysTableFallsThroughToEmpty() {
        SysTableSpec spec = SysTableSpec.builder()
                .name("snapshots")
                .schema(EMPTY_SCHEMA)
                .mode(SysTableExecutionMode.NATIVE)
                .nativeFactory((db, t, n, v) -> Mockito.mock(ConnectorScanPlanProvider.class))
                .build();
        PluginDrivenExternalTable table = newPluginTable(spec);
        Optional<SysTableResolver.SysTablePlan> plan = SysTableResolver.resolveForPlan(
                table, "ctl", "db", "tbl$does_not_exist");
        Assertions.assertFalse(plan.isPresent());
        Assertions.assertFalse(SysTableResolver.validateForQuery(
                table, "ctl", "db", "tbl$does_not_exist"));
    }

    @Test
    public void testNoSuffixMeansNoPluginLookup() {
        SysTableSpec spec = SysTableSpec.builder()
                .name("snapshots")
                .schema(EMPTY_SCHEMA)
                .mode(SysTableExecutionMode.NATIVE)
                .nativeFactory((db, t, n, v) -> Mockito.mock(ConnectorScanPlanProvider.class))
                .build();
        PluginDrivenExternalTable table = newPluginTable(spec);
        Assertions.assertFalse(SysTableResolver.lookupPluginSpec(table, "tbl").isPresent());
    }

    // ----- legacy fallback -----

    @Test
    public void testNonPluginCatalogFallsBackToLegacyResolver() throws Exception {
        IcebergExternalTable iceberg = newIcebergTable();
        Optional<SysTableResolver.SysTablePlan> plan = SysTableResolver.resolveForPlan(
                iceberg, "ctl", "db", "tbl$snapshots");
        Assertions.assertTrue(plan.isPresent(), "legacy iceberg native path must still resolve");
        Assertions.assertTrue(plan.get().isNative());
        Assertions.assertFalse(plan.get().isPluginManaged());
    }

    @Test
    public void testNonExternalTableNoPluginLookup() {
        Assertions.assertFalse(
                SysTableResolver.lookupPluginSpec(Mockito.mock(org.apache.doris.catalog.TableIf.class),
                        "tbl$snapshots").isPresent());
    }

    @Test
    public void testSuffixStripping() {
        org.apache.doris.common.Pair<String, String> parsed
                = SysTable.getTableNameWithSysTableName("orders$snapshots");
        Assertions.assertEquals("orders", parsed.first);
        Assertions.assertEquals("snapshots", parsed.second);
    }

    // ----- helpers -----

    private static PluginDrivenExternalTable newPluginTable(SysTableSpec spec) {
        Connector connector = Mockito.mock(Connector.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
        Mockito.when(metadata.getSysTable(Mockito.anyString(), Mockito.eq("tbl"), Mockito.eq(spec.name())))
                .thenReturn(Optional.of(spec));
        Mockito.when(metadata.getSysTable(Mockito.anyString(), Mockito.eq("tbl"),
                Mockito.argThat(arg -> arg != null && !arg.equals(spec.name()))))
                .thenReturn(Optional.empty());

        PluginDrivenExternalCatalog catalog = new TestablePluginCatalog(connector);
        @SuppressWarnings("unchecked")
        ExternalDatabase<PluginDrivenExternalTable> db = Mockito.mock(ExternalDatabase.class);
        Mockito.when(db.getFullName()).thenReturn("db");
        Mockito.when(db.getRemoteName()).thenReturn("db");
        return new PluginDrivenExternalTable(1L, "tbl", "tbl", catalog, db);
    }

    private IcebergExternalTable newIcebergTable() throws Exception {
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        IcebergExternalDatabase db = Mockito.mock(IcebergExternalDatabase.class);
        Mockito.when(catalog.getId()).thenReturn(1L);
        Mockito.when(db.getFullName()).thenReturn("db");
        Mockito.when(db.getRemoteName()).thenReturn("db");
        Mockito.doReturn(db).when(catalog).getDbOrAnalysisException("db");
        Mockito.when(db.getId()).thenReturn(2L);
        return new IcebergExternalTable(3L, "tbl", "tbl", catalog, db);
    }

    /** Bypasses init / Connector creation; returns a fixed mocked Connector. */
    private static class TestablePluginCatalog extends PluginDrivenExternalCatalog {
        private final Connector mocked;

        TestablePluginCatalog(Connector mocked) {
            super(7L, "ctl", null, makeProps(), "", mocked);
            this.mocked = mocked;
        }

        @Override
        public Connector getConnector() {
            return mocked;
        }

        @Override
        public ConnectorSession buildConnectorSession() {
            return Mockito.mock(ConnectorSession.class);
        }

        @Override
        public String getType() {
            return "iceberg";
        }

        @Override
        protected java.util.List<String> listDatabaseNames() {
            return Collections.emptyList();
        }

        @Override
        public boolean tableExist(org.apache.doris.datasource.SessionContext ctx, String dbName, String tblName) {
            return false;
        }

        private static Map<String, String> makeProps() {
            return Collections.singletonMap("type", "iceberg");
        }

        @SuppressWarnings("unused")
        private ExternalCatalog asExternalCatalog() {
            return this;
        }
    }
}
