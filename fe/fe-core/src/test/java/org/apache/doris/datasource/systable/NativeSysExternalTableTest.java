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
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.systable.NativeSysTableScanFactory;
import org.apache.doris.connector.api.systable.SysTableExecutionMode;
import org.apache.doris.connector.api.systable.SysTableSpec;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.PluginDrivenExternalTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class NativeSysExternalTableTest {

    private static final ConnectorTableSchema SCHEMA = new ConnectorTableSchema(
            "tbl",
            Arrays.asList(
                    new ConnectorColumn("snapshot_id", ConnectorType.of("BIGINT"), null, false, null),
                    new ConnectorColumn("ts", ConnectorType.of("BIGINT"), null, true, null)),
            "iceberg",
            Collections.emptyMap());

    @Test
    public void testCreateScanPlanProviderInvokesNativeFactory() {
        ConnectorScanPlanProvider provider = Mockito.mock(ConnectorScanPlanProvider.class);
        AtomicReference<String> calledDb = new AtomicReference<>();
        AtomicReference<String> calledTbl = new AtomicReference<>();
        AtomicReference<String> calledSys = new AtomicReference<>();
        AtomicReference<Optional<ConnectorTableVersion>> calledVersion = new AtomicReference<>();
        NativeSysTableScanFactory factory = (db, t, n, v) -> {
            calledDb.set(db);
            calledTbl.set(t);
            calledSys.set(n);
            calledVersion.set(v);
            return provider;
        };
        SysTableSpec spec = SysTableSpec.builder()
                .name("snapshots")
                .schema(SCHEMA)
                .mode(SysTableExecutionMode.NATIVE)
                .nativeFactory(factory)
                .build();
        ExternalTable source = newSource();
        NativeSysExternalTable wrapper = new NativeSysExternalTable(source, spec);

        ConnectorScanPlanProvider returned = wrapper.createScanPlanProvider(Optional.empty());
        Assertions.assertSame(provider, returned);
        Assertions.assertEquals("db_remote", calledDb.get());
        Assertions.assertEquals("tbl_remote", calledTbl.get());
        Assertions.assertEquals("snapshots", calledSys.get());
        Assertions.assertTrue(calledVersion.get().isEmpty());
    }

    @Test
    public void testVersionForwardedOnlyWhenAccepted() {
        ConnectorTableVersion v = new ConnectorTableVersion.BySnapshotId(42L);
        AtomicReference<Optional<ConnectorTableVersion>> seen = new AtomicReference<>();
        NativeSysTableScanFactory factory = (db, t, n, ver) -> {
            seen.set(ver);
            return Mockito.mock(ConnectorScanPlanProvider.class);
        };
        SysTableSpec accepting = SysTableSpec.builder()
                .name("files")
                .schema(SCHEMA)
                .mode(SysTableExecutionMode.NATIVE)
                .acceptsTableVersion(true)
                .nativeFactory(factory)
                .build();
        SysTableSpec stripping = SysTableSpec.builder()
                .name("snapshots")
                .schema(SCHEMA)
                .mode(SysTableExecutionMode.NATIVE)
                .acceptsTableVersion(false)
                .nativeFactory(factory)
                .build();
        ExternalTable source = newSource();
        new NativeSysExternalTable(source, accepting).createScanPlanProvider(Optional.of(v));
        Assertions.assertSame(v, seen.get().orElseThrow());
        new NativeSysExternalTable(source, stripping).createScanPlanProvider(Optional.of(v));
        Assertions.assertTrue(seen.get().isEmpty());
    }

    @Test
    public void testWrapperSchemaConvertedFromSpec() {
        SysTableSpec spec = SysTableSpec.builder()
                .name("snapshots")
                .schema(SCHEMA)
                .mode(SysTableExecutionMode.NATIVE)
                .nativeFactory((db, t, n, v) -> Mockito.mock(ConnectorScanPlanProvider.class))
                .build();
        NativeSysExternalTable wrapper = new NativeSysExternalTable(newSource(), spec);
        List<org.apache.doris.catalog.Column> cols = wrapper.getFullSchema();
        Assertions.assertEquals(2, cols.size());
        Assertions.assertEquals("snapshot_id", cols.get(0).getName());
    }

    @Test
    public void testNameAndId() {
        SysTableSpec spec = SysTableSpec.builder()
                .name("snapshots")
                .schema(SCHEMA)
                .mode(SysTableExecutionMode.NATIVE)
                .nativeFactory((db, t, n, v) -> Mockito.mock(ConnectorScanPlanProvider.class))
                .build();
        NativeSysExternalTable wrapper = new NativeSysExternalTable(newSource(), spec);
        Assertions.assertEquals("tbl$snapshots", wrapper.getName());
        Assertions.assertEquals("tbl_remote$snapshots", wrapper.getRemoteName());
        Assertions.assertSame(spec, wrapper.getSpec());
    }

    @Test
    public void testWrongModeRejected() {
        SysTableSpec spec = SysTableSpec.builder()
                .name("partitions")
                .schema(SCHEMA)
                .mode(SysTableExecutionMode.TVF)
                .tvfInvoker((db, t, n, v) ->
                        new org.apache.doris.connector.api.systable.TvfInvocation("p", Collections.emptyMap()))
                .build();
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new NativeSysExternalTable(newSource(), spec));
    }

    private ExternalTable newSource() {
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(1L);
        @SuppressWarnings("unchecked")
        ExternalDatabase<PluginDrivenExternalTable> db = Mockito.mock(ExternalDatabase.class);
        Mockito.when(db.getFullName()).thenReturn("db");
        Mockito.when(db.getRemoteName()).thenReturn("db_remote");
        return new PluginDrivenExternalTable(10L, "tbl", "tbl_remote", catalog, db);
    }
}
