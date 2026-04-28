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
import org.apache.doris.connector.api.ConnectorTableId;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.systable.SysTableExecutionMode;
import org.apache.doris.connector.api.systable.SysTableSpec;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.datasource.SessionContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Simulates the routing flow exercised by
 * {@code BindRelation#handleMetaTable} (BindRelation:397),
 * {@code DescribeCommand#doRun} (DescribeCommand:207),
 * {@code ShowCreateTableCommand#resolveShowCreateTarget} (ShowCreateTableCommand:199),
 * and {@code RelationUtil#getDbAndTable} (RelationUtil:149) — verifying that
 * a plugin-managed sys table is resolved via the new plugin route
 * (without falling back to the legacy registry).
 */
public class BindRelationSysTableRoutingTest {

    private static final ConnectorTableSchema SCHEMA = new ConnectorTableSchema(
            "tbl",
            Collections.singletonList(
                    new ConnectorColumn("id", ConnectorType.of("INT"), null, true, null)),
            "iceberg",
            Collections.emptyMap());

    @Test
    public void testHandleMetaTableNativeRouteProducesLogicalFileScanCompatibleWrapper() {
        SysTableSpec spec = SysTableSpec.builder()
                .name("snapshots")
                .schema(SCHEMA)
                .mode(SysTableExecutionMode.NATIVE)
                .nativeFactory((db, t, n, v) -> Mockito.mock(ConnectorScanPlanProvider.class))
                .build();
        PluginDrivenExternalTable table = newPluginTable(spec);

        // Replicates BindRelation:397
        Optional<SysTableResolver.SysTablePlan> plan = SysTableResolver.resolveForPlan(
                table, "ctl", "db", "tbl$snapshots");
        Assertions.assertTrue(plan.isPresent());
        Assertions.assertTrue(plan.get().isNative(), "isNative() drives BindRelation LogicalFileScan branch");
        Assertions.assertTrue(plan.get().isPluginManaged());
        Assertions.assertTrue(plan.get().getSysExternalTable() instanceof NativeSysExternalTable);
    }

    @Test
    public void testDescribeCommandRouteProducesNativeWrapperForPluginNative() {
        SysTableSpec spec = SysTableSpec.builder()
                .name("snapshots")
                .schema(SCHEMA)
                .mode(SysTableExecutionMode.NATIVE)
                .nativeFactory((db, t, n, v) -> Mockito.mock(ConnectorScanPlanProvider.class))
                .build();
        PluginDrivenExternalTable table = newPluginTable(spec);

        // Replicates DescribeCommand:207 / ShowCreateTableCommand:199
        Optional<SysTableResolver.SysTableDescribe> describe = SysTableResolver.resolveForDescribe(
                table, "ctl", "db", "tbl$snapshots");
        Assertions.assertTrue(describe.isPresent());
        Assertions.assertTrue(describe.get().isNative());
        Assertions.assertTrue(describe.get().isPluginManaged());
    }

    @Test
    public void testRelationUtilValidateAcceptsPluginSysTable() {
        SysTableSpec spec = SysTableSpec.builder()
                .name("snapshots")
                .schema(SCHEMA)
                .mode(SysTableExecutionMode.NATIVE)
                .nativeFactory((db, t, n, v) -> Mockito.mock(ConnectorScanPlanProvider.class))
                .build();
        PluginDrivenExternalTable table = newPluginTable(spec);

        // Replicates RelationUtil:149
        Assertions.assertTrue(SysTableResolver.validateForQuery(table, "ctl", "db", "tbl$snapshots"));
        Assertions.assertFalse(SysTableResolver.validateForQuery(table, "ctl", "db", "tbl$bogus"));
    }

    private static PluginDrivenExternalTable newPluginTable(SysTableSpec spec) {
        Connector connector = Mockito.mock(Connector.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
        Mockito.when(metadata.getSysTable(Mockito.any(ConnectorTableId.class), Mockito.eq(spec.name())))
                .thenReturn(Optional.of(spec));
        Mockito.when(metadata.getSysTable(Mockito.any(ConnectorTableId.class), Mockito.argThat(arg -> arg != null && !arg.equals(spec.name()))))
                .thenReturn(Optional.empty());

        PluginDrivenExternalCatalog catalog = new TestablePluginCatalog(connector);
        @SuppressWarnings("unchecked")
        ExternalDatabase<PluginDrivenExternalTable> db = Mockito.mock(ExternalDatabase.class);
        Mockito.when(db.getFullName()).thenReturn("db");
        Mockito.when(db.getRemoteName()).thenReturn("db");
        return new PluginDrivenExternalTable(99L, "tbl", "tbl", catalog, db);
    }

    private static class TestablePluginCatalog extends PluginDrivenExternalCatalog {
        private final Connector mocked;

        TestablePluginCatalog(Connector mocked) {
            super(7L, "ctl", null, Collections.singletonMap("type", "iceberg"), "", mocked);
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
        protected List<String> listDatabaseNames() {
            return Collections.emptyList();
        }

        @Override
        public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
            return false;
        }
    }
}
