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

package org.apache.doris.connector.iceberg.backend.hadoop;

import org.apache.doris.connector.api.timetravel.ConnectorRef;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;
import org.apache.doris.connector.api.timetravel.RefKind;
import org.apache.doris.connector.iceberg.api.IcebergBackendContext;
import org.apache.doris.connector.iceberg.api.IcebergBackendException;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Verifies that {@link HadoopIcebergBackend} inherits the shared
 * time-travel default methods from {@code IcebergBackend} and wires
 * through to the underlying {@link Catalog} / {@link Table}. The
 * exhaustive matrix lives in {@code IcebergBackendSupportTest} inside
 * {@code fe-connector-iceberg-api}; this test covers the per-backend
 * routing for the scenarios called out in the M1-07 task brief.
 */
class HadoopIcebergBackendTimeTravelTest {

    private static final String DB = "db";
    private static final String TBL = "t";

    private Catalog catalog;
    private Table table;
    private HadoopIcebergBackend backend;

    @BeforeEach
    void setUp() {
        catalog = Mockito.mock(Catalog.class);
        table = Mockito.mock(Table.class);
        Mockito.when(catalog.loadTable(TableIdentifier.of(DB, TBL))).thenReturn(table);
        final Catalog fixed = catalog;
        backend = new HadoopIcebergBackend() {
            @Override
            public Catalog buildCatalog(IcebergBackendContext context) {
                return fixed;
            }
        };
    }

    private IcebergBackendContext ctx() {
        return new IcebergBackendContext("c", new HashMap<>(), new Configuration(false));
    }

    private SnapshotRef branchRef(long sid) {
        SnapshotRef r = Mockito.mock(SnapshotRef.class);
        Mockito.when(r.isBranch()).thenReturn(true);
        Mockito.when(r.isTag()).thenReturn(false);
        Mockito.when(r.snapshotId()).thenReturn(sid);
        return r;
    }

    private SnapshotRef tagRef(long sid) {
        SnapshotRef r = Mockito.mock(SnapshotRef.class);
        Mockito.when(r.isBranch()).thenReturn(false);
        Mockito.when(r.isTag()).thenReturn(true);
        Mockito.when(r.snapshotId()).thenReturn(sid);
        return r;
    }

    @Test
    void listRefsReturnsBranchesAndTags() {
        Map<String, SnapshotRef> refs = new LinkedHashMap<>();
        refs.put("main", branchRef(10L));
        refs.put("dev", branchRef(20L));
        refs.put("v1", tagRef(30L));
        Mockito.when(table.refs()).thenReturn(refs);
        Mockito.when(table.snapshot(Mockito.anyLong())).thenReturn(null);

        List<ConnectorRef> out = backend.listRefs(ctx(), DB, TBL);

        Assertions.assertEquals(3, out.size());
        Assertions.assertEquals(RefKind.BRANCH, out.get(0).kind());
        Assertions.assertEquals(RefKind.BRANCH, out.get(1).kind());
        Assertions.assertEquals(RefKind.TAG, out.get(2).kind());
    }

    @Test
    void resolveBySnapshotId() {
        Snapshot s = Mockito.mock(Snapshot.class);
        Mockito.when(table.snapshot(42L)).thenReturn(s);
        Snapshot got = backend.resolveVersion(ctx(), DB, TBL,
                new ConnectorTableVersion.BySnapshotId(42L));
        Assertions.assertSame(s, got);
    }

    @Test
    void resolveByTimestamp() {
        Snapshot s = Mockito.mock(Snapshot.class);
        Mockito.when(table.snapshot(77L)).thenReturn(s);
        try (MockedStatic<org.apache.iceberg.util.SnapshotUtil> mocked
                     = Mockito.mockStatic(org.apache.iceberg.util.SnapshotUtil.class)) {
            mocked.when(() -> org.apache.iceberg.util.SnapshotUtil
                            .snapshotIdAsOfTime(table, 1_000L))
                    .thenReturn(77L);
            Snapshot got = backend.resolveVersion(ctx(), DB, TBL,
                    new ConnectorTableVersion.ByTimestamp(Instant.ofEpochMilli(1_000L)));
            Assertions.assertSame(s, got);
        }
    }

    @Test
    void resolveByRefBranch() {
        Map<String, SnapshotRef> refs = new HashMap<>();
        refs.put("feature", branchRef(500L));
        Mockito.when(table.refs()).thenReturn(refs);
        Snapshot s = Mockito.mock(Snapshot.class);
        Mockito.when(table.snapshot(500L)).thenReturn(s);

        Snapshot got = backend.resolveVersion(ctx(), DB, TBL,
                new ConnectorTableVersion.ByRef("feature", RefKind.BRANCH));
        Assertions.assertSame(s, got);
    }

    @Test
    void resolveByRefTag() {
        Map<String, SnapshotRef> refs = new HashMap<>();
        refs.put("v1", tagRef(600L));
        Mockito.when(table.refs()).thenReturn(refs);
        Snapshot s = Mockito.mock(Snapshot.class);
        Mockito.when(table.snapshot(600L)).thenReturn(s);

        Snapshot got = backend.resolveVersion(ctx(), DB, TBL,
                new ConnectorTableVersion.ByRef("v1", RefKind.TAG));
        Assertions.assertSame(s, got);
    }

    @Test
    void resolveByOpaqueThrowsUoe() {
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> backend.resolveVersion(ctx(), DB, TBL,
                        new ConnectorTableVersion.ByOpaque("tok")));
    }

    @Test
    void resolveNullVersionThrowsIcebergBackendException() {
        Assertions.assertThrows(IcebergBackendException.class,
                () -> backend.resolveVersion(ctx(), DB, TBL, null));
    }
}
