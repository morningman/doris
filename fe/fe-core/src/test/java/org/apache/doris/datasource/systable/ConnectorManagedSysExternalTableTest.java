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

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.systable.ConnectorScanPlanProviderRef;
import org.apache.doris.connector.api.systable.SysTableExecutionMode;
import org.apache.doris.connector.api.systable.SysTableSpec;
import org.apache.doris.connector.api.systable.TvfInvocation;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.PluginDrivenExternalTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class ConnectorManagedSysExternalTableTest {

    private static final ConnectorTableSchema SCHEMA = new ConnectorTableSchema(
            "tbl",
            Arrays.asList(
                    new ConnectorColumn("p", ConnectorType.of("STRING"), null, true, null)),
            "hive",
            Collections.emptyMap());

    @Test
    public void testTvfInvokerInvokedAndPropagatesProperties() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put("table", "orders");
        TvfInvocation invocation = new TvfInvocation("partition_values", props);
        SysTableSpec spec = SysTableSpec.builder()
                .name("partitions")
                .schema(SCHEMA)
                .mode(SysTableExecutionMode.TVF)
                .tvfInvoker((db, t, n, v) -> invocation)
                .build();
        ConnectorManagedSysExternalTable wrapper = new ConnectorManagedSysExternalTable(newSource(), spec);

        TvfInvocation result = wrapper.resolveTvfInvocation(Optional.empty());
        Assertions.assertSame(invocation, result);
        Assertions.assertEquals("partition_values", result.functionName());
        Assertions.assertEquals("orders", result.properties().get("table"));
    }

    @Test
    public void testConnectorScanPlanRefExposed() {
        ConnectorScanPlanProviderRef ref = ConnectorScanPlanProviderRef.of("provider-xyz");
        SysTableSpec spec = SysTableSpec.builder()
                .name("plan")
                .schema(SCHEMA)
                .mode(SysTableExecutionMode.CONNECTOR_SCAN_PLAN)
                .scanPlanProviderRef(ref)
                .build();
        ConnectorManagedSysExternalTable wrapper = new ConnectorManagedSysExternalTable(newSource(), spec);
        Assertions.assertSame(ref, wrapper.getScanPlanProviderRef());
    }

    @Test
    public void testTvfMethodOnScanPlanModeRejected() {
        SysTableSpec spec = SysTableSpec.builder()
                .name("plan")
                .schema(SCHEMA)
                .mode(SysTableExecutionMode.CONNECTOR_SCAN_PLAN)
                .scanPlanProviderRef(ConnectorScanPlanProviderRef.of("p"))
                .build();
        ConnectorManagedSysExternalTable wrapper = new ConnectorManagedSysExternalTable(newSource(), spec);
        Assertions.assertThrows(IllegalStateException.class,
                () -> wrapper.resolveTvfInvocation(Optional.empty()));
    }

    @Test
    public void testScanPlanRefMethodOnTvfModeRejected() {
        SysTableSpec spec = SysTableSpec.builder()
                .name("partitions")
                .schema(SCHEMA)
                .mode(SysTableExecutionMode.TVF)
                .tvfInvoker((db, t, n, v) -> new TvfInvocation("partitions", Collections.emptyMap()))
                .build();
        ConnectorManagedSysExternalTable wrapper = new ConnectorManagedSysExternalTable(newSource(), spec);
        Assertions.assertThrows(IllegalStateException.class, wrapper::getScanPlanProviderRef);
    }

    @Test
    public void testNativeModeRejected() {
        SysTableSpec spec = SysTableSpec.builder()
                .name("snapshots")
                .schema(SCHEMA)
                .mode(SysTableExecutionMode.NATIVE)
                .nativeFactory((db, t, n, v) ->
                        Mockito.mock(org.apache.doris.connector.api.scan.ConnectorScanPlanProvider.class))
                .build();
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new ConnectorManagedSysExternalTable(newSource(), spec));
    }

    @Test
    public void testWrapperNameAndSchema() {
        SysTableSpec spec = SysTableSpec.builder()
                .name("partitions")
                .schema(SCHEMA)
                .mode(SysTableExecutionMode.TVF)
                .tvfInvoker((db, t, n, v) -> new TvfInvocation("partitions", Collections.emptyMap()))
                .build();
        ConnectorManagedSysExternalTable wrapper = new ConnectorManagedSysExternalTable(newSource(), spec);
        Assertions.assertEquals("tbl$partitions", wrapper.getName());
        Assertions.assertEquals(1, wrapper.getFullSchema().size());
        Assertions.assertSame(spec, wrapper.getSpec());
    }

    private ExternalTable newSource() {
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(2L);
        @SuppressWarnings("unchecked")
        ExternalDatabase<PluginDrivenExternalTable> db = Mockito.mock(ExternalDatabase.class);
        Mockito.when(db.getFullName()).thenReturn("db");
        Mockito.when(db.getRemoteName()).thenReturn("db_remote");
        return new PluginDrivenExternalTable(20L, "tbl", "tbl_remote", catalog, db);
    }
}
