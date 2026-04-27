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
import org.apache.doris.connector.api.systable.SysTableExecutionMode;
import org.apache.doris.connector.api.systable.SysTableSpec;
import org.apache.doris.connector.api.systable.TvfInvocation;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.datasource.SessionContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Verifies the M1-15 routing for plugin-managed sys tables in
 * {@link SysTableExecutionMode#TVF TVF} mode.
 *
 * <p>Complements {@link BindRelationSysTableRoutingTest}, which exercises the
 * NATIVE-mode plugin path landed in M1-12. Here we ensure that
 * {@link SysTableResolver.SysTablePlan#isPluginTvf()} is set correctly,
 * {@link SysTableResolver.SysTablePlan#isNative()} returns {@code false} for
 * plugin TVF specs (so {@code BindRelation} skips the {@code LogicalFileScan}
 * branch), and the wrapper exposes the plugin-supplied
 * {@link TvfInvocation}.</p>
 */
public class BindRelationTvfSysTableRoutingTest {

    private static final ConnectorTableSchema SCHEMA = new ConnectorTableSchema(
            "tbl",
            Collections.singletonList(
                    new ConnectorColumn("partition_name", ConnectorType.of("STRING"), null, true, null)),
            "HIVE_METADATA",
            Collections.emptyMap());

    @Test
    public void resolverReturnsTvfPluginPlanForPartitions() {
        SysTableSpec spec = tvfPartitionsSpec();
        PluginDrivenExternalTable table = newPluginTable(spec);

        Optional<SysTableResolver.SysTablePlan> plan = SysTableResolver.resolveForPlan(
                table, "hive_ctl", "default", "tbl$partitions");
        Assertions.assertTrue(plan.isPresent());
        SysTableResolver.SysTablePlan p = plan.get();
        Assertions.assertTrue(p.isPluginManaged(), "must route via plugin");
        Assertions.assertTrue(p.isPluginTvf(), "spec.mode()=TVF must surface as isPluginTvf()");
        Assertions.assertFalse(p.isNative(),
                "TVF-mode plugin plans must NOT report isNative() — would mis-route to LogicalFileScan");
    }

    @Test
    public void planExposesConnectorManagedWrapperForTvfMode() {
        SysTableSpec spec = tvfPartitionsSpec();
        PluginDrivenExternalTable table = newPluginTable(spec);

        SysTableResolver.SysTablePlan plan = SysTableResolver.resolveForPlan(
                table, "hive_ctl", "default", "tbl$partitions").orElseThrow();
        Assertions.assertTrue(plan.getSysExternalTable() instanceof ConnectorManagedSysExternalTable,
                "TVF-mode plugin plan exposes ConnectorManagedSysExternalTable wrapper");
    }

    @Test
    public void wrapperResolvesTvfInvocation() {
        SysTableSpec spec = tvfPartitionsSpec();
        PluginDrivenExternalTable table = newPluginTable(spec);

        SysTableResolver.SysTablePlan plan = SysTableResolver.resolveForPlan(
                table, "hive_ctl", "default", "tbl$partitions").orElseThrow();
        ConnectorManagedSysExternalTable wrapper =
                (ConnectorManagedSysExternalTable) plan.getSysExternalTable();
        TvfInvocation inv = wrapper.resolveTvfInvocation(Optional.empty());
        Assertions.assertEquals("partition_values", inv.functionName());
        // The invoker is fed the *remote* (db, table) pair from the wrapper —
        // see ConnectorManagedSysExternalTable.resolveTvfInvocation. The
        // mock plugin echoes those into the invocation.
        Assertions.assertEquals("tbl_remote", inv.properties().get("table"));
        Assertions.assertEquals("default_remote", inv.properties().get("database"));
    }

    @Test
    public void describeRouteSurfacesPluginTvf() {
        SysTableSpec spec = tvfPartitionsSpec();
        PluginDrivenExternalTable table = newPluginTable(spec);

        Optional<SysTableResolver.SysTableDescribe> describe = SysTableResolver.resolveForDescribe(
                table, "hive_ctl", "default", "tbl$partitions");
        Assertions.assertTrue(describe.isPresent());
        Assertions.assertTrue(describe.get().isPluginManaged());
        Assertions.assertTrue(describe.get().isPluginTvf());
        Assertions.assertFalse(describe.get().isNative(),
                "DescribeCommand uses isNative()||isPluginTvf() — both surfaces required.");
        Assertions.assertTrue(describe.get().getSysExternalTable()
                instanceof ConnectorManagedSysExternalTable);
    }

    @Test
    public void validateForQueryAcceptsTvfPluginSysTable() {
        SysTableSpec spec = tvfPartitionsSpec();
        PluginDrivenExternalTable table = newPluginTable(spec);

        Assertions.assertTrue(SysTableResolver.validateForQuery(
                table, "hive_ctl", "default", "tbl$partitions"));
        Assertions.assertFalse(SysTableResolver.validateForQuery(
                table, "hive_ctl", "default", "tbl$bogus"));
    }

    @Test
    public void endToEndPluginTvfDispatcherProducesPartitionValues() {
        // End-to-end: fake plugin spec → resolver → wrapper.resolveTvfInvocation
        // → PluginTvfDispatcher.toTableValuedFunction → fe-core PartitionValues.
        SysTableSpec spec = tvfPartitionsSpec();
        PluginDrivenExternalTable table = newPluginTable(spec);

        ConnectorManagedSysExternalTable wrapper = (ConnectorManagedSysExternalTable)
                SysTableResolver.resolveForPlan(table, "hive_ctl", "default", "tbl$partitions")
                        .orElseThrow().getSysExternalTable();
        TvfInvocation inv = wrapper.resolveTvfInvocation(Optional.empty());
        org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction tvf =
                PluginTvfDispatcher.toTableValuedFunction("hive_ctl", "default", "orders", inv);
        Assertions.assertEquals("partition_values", tvf.getName());
    }

    private static SysTableSpec tvfPartitionsSpec() {
        return SysTableSpec.builder()
                .name("partitions")
                .schema(SCHEMA)
                .mode(SysTableExecutionMode.TVF)
                .acceptsTableVersion(false)
                .tvfInvoker((db, t, n, v) -> {
                    Map<String, String> p = new LinkedHashMap<>();
                    p.put("database", db);
                    p.put("table", t);
                    return new TvfInvocation("partition_values", p);
                })
                .build();
    }

    private static PluginDrivenExternalTable newPluginTable(SysTableSpec spec) {
        Connector connector = Mockito.mock(Connector.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
        // SysTableResolver.lookupPluginSpec passes (db.remoteName, parsed table from
        // the "tbl$suffix" string, suffix). The parsed table is always the engine-side
        // table identifier ("tbl"), not the plugin's remote table name.
        Mockito.when(metadata.getSysTable(Mockito.anyString(), Mockito.eq("tbl"),
                        Mockito.eq(spec.name())))
                .thenReturn(Optional.of(spec));
        Mockito.when(metadata.getSysTable(Mockito.anyString(), Mockito.eq("tbl"),
                        Mockito.argThat(arg -> arg != null && !arg.equals(spec.name()))))
                .thenReturn(Optional.empty());

        PluginDrivenExternalCatalog catalog = new TestablePluginCatalog(connector);
        @SuppressWarnings("unchecked")
        ExternalDatabase<PluginDrivenExternalTable> db = Mockito.mock(ExternalDatabase.class);
        Mockito.when(db.getFullName()).thenReturn("default");
        Mockito.when(db.getRemoteName()).thenReturn("default_remote");
        return new PluginDrivenExternalTable(101L, "tbl", "tbl_remote", catalog, db);
    }

    private static class TestablePluginCatalog extends PluginDrivenExternalCatalog {
        private final Connector mocked;

        TestablePluginCatalog(Connector mocked) {
            super(11L, "hive_ctl", null, Collections.singletonMap("type", "hive"), "", mocked);
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
            return "hive";
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
