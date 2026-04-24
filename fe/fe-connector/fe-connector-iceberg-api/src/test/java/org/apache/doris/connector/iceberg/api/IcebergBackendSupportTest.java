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

package org.apache.doris.connector.iceberg.api;

import org.apache.doris.connector.api.timetravel.ConnectorRef;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;
import org.apache.doris.connector.api.timetravel.RefKind;

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
 * Unit tests for {@link IcebergBackendSupport}: exercises the shared ref /
 * version resolution logic over a Mockito-backed {@link Catalog} /
 * {@link Table} pair. Real backends inherit these semantics via the
 * {@link IcebergBackend} default methods; see per-backend smoke tests for
 * routing coverage.
 */
class IcebergBackendSupportTest {

    private static final String DB = "db";
    private static final String TBL = "t";

    private Catalog catalog;
    private Table table;

    @BeforeEach
    void setUp() {
        catalog = Mockito.mock(Catalog.class);
        table = Mockito.mock(Table.class);
        Mockito.when(catalog.loadTable(TableIdentifier.of(DB, TBL))).thenReturn(table);
    }

    // ---- listRefs ----

    @Test
    void listRefsConvertsBranchesAndTags() {
        SnapshotRef branchMain = Mockito.mock(SnapshotRef.class);
        Mockito.when(branchMain.isBranch()).thenReturn(true);
        Mockito.when(branchMain.isTag()).thenReturn(false);
        Mockito.when(branchMain.snapshotId()).thenReturn(100L);

        SnapshotRef branchDev = Mockito.mock(SnapshotRef.class);
        Mockito.when(branchDev.isBranch()).thenReturn(true);
        Mockito.when(branchDev.isTag()).thenReturn(false);
        Mockito.when(branchDev.snapshotId()).thenReturn(200L);

        SnapshotRef tagV1 = Mockito.mock(SnapshotRef.class);
        Mockito.when(tagV1.isBranch()).thenReturn(false);
        Mockito.when(tagV1.isTag()).thenReturn(true);
        Mockito.when(tagV1.snapshotId()).thenReturn(300L);

        Map<String, SnapshotRef> refs = new LinkedHashMap<>();
        refs.put("main", branchMain);
        refs.put("dev", branchDev);
        refs.put("v1", tagV1);
        Mockito.when(table.refs()).thenReturn(refs);

        Snapshot snap100 = Mockito.mock(Snapshot.class);
        Mockito.when(snap100.timestampMillis()).thenReturn(1_700_000_000_000L);
        Mockito.when(table.snapshot(100L)).thenReturn(snap100);
        Mockito.when(table.snapshot(200L)).thenReturn(null);
        Mockito.when(table.snapshot(300L)).thenReturn(Mockito.mock(Snapshot.class));

        List<ConnectorRef> out = IcebergBackendSupport.listRefs(catalog, DB, TBL);

        Assertions.assertEquals(3, out.size());
        Assertions.assertEquals("main", out.get(0).name());
        Assertions.assertEquals(RefKind.BRANCH, out.get(0).kind());
        Assertions.assertEquals(100L, out.get(0).snapshotId());
        Assertions.assertEquals(Instant.ofEpochMilli(1_700_000_000_000L),
                out.get(0).createdAt().orElseThrow());
        Assertions.assertEquals("dev", out.get(1).name());
        Assertions.assertEquals(RefKind.BRANCH, out.get(1).kind());
        Assertions.assertTrue(out.get(1).createdAt().isEmpty(),
                "missing snapshot timestamp should leave createdAt empty");
        Assertions.assertEquals("v1", out.get(2).name());
        Assertions.assertEquals(RefKind.TAG, out.get(2).kind());
    }

    @Test
    void listRefsEmptyWhenTableHasNoRefs() {
        Mockito.when(table.refs()).thenReturn(new HashMap<>());
        List<ConnectorRef> out = IcebergBackendSupport.listRefs(catalog, DB, TBL);
        Assertions.assertTrue(out.isEmpty());
    }

    // ---- resolveVersion ----

    @Test
    void resolveBySnapshotIdReturnsSnapshot() {
        Snapshot s = Mockito.mock(Snapshot.class);
        Mockito.when(table.snapshot(42L)).thenReturn(s);
        Snapshot got = IcebergBackendSupport.resolveVersion(
                catalog, DB, TBL, new ConnectorTableVersion.BySnapshotId(42L));
        Assertions.assertSame(s, got);
    }

    @Test
    void resolveBySnapshotIdMissingSnapshotThrows() {
        Mockito.when(table.snapshot(99L)).thenReturn(null);
        IcebergBackendException ex = Assertions.assertThrows(
                IcebergBackendException.class,
                () -> IcebergBackendSupport.resolveVersion(
                        catalog, DB, TBL, new ConnectorTableVersion.BySnapshotId(99L)));
        Assertions.assertTrue(ex.getMessage().contains("99"));
        Assertions.assertTrue(ex.getMessage().contains(DB + "." + TBL));
    }

    @Test
    void resolveByTimestampUsesSnapshotUtil() {
        Snapshot s = Mockito.mock(Snapshot.class);
        Mockito.when(table.snapshot(77L)).thenReturn(s);
        try (MockedStatic<org.apache.iceberg.util.SnapshotUtil> mocked
                     = Mockito.mockStatic(org.apache.iceberg.util.SnapshotUtil.class)) {
            mocked.when(() -> org.apache.iceberg.util.SnapshotUtil
                            .snapshotIdAsOfTime(table, 1_700_000_000_000L))
                    .thenReturn(77L);
            Snapshot got = IcebergBackendSupport.resolveVersion(
                    catalog, DB, TBL,
                    new ConnectorTableVersion.ByTimestamp(
                            Instant.ofEpochMilli(1_700_000_000_000L)));
            Assertions.assertSame(s, got);
        }
    }

    @Test
    void resolveByRefBranchReturnsSnapshot() {
        SnapshotRef branch = Mockito.mock(SnapshotRef.class);
        Mockito.when(branch.isBranch()).thenReturn(true);
        Mockito.when(branch.isTag()).thenReturn(false);
        Mockito.when(branch.snapshotId()).thenReturn(500L);

        Map<String, SnapshotRef> refs = new HashMap<>();
        refs.put("feature", branch);
        Mockito.when(table.refs()).thenReturn(refs);

        Snapshot s = Mockito.mock(Snapshot.class);
        Mockito.when(table.snapshot(500L)).thenReturn(s);

        Snapshot got = IcebergBackendSupport.resolveVersion(
                catalog, DB, TBL,
                new ConnectorTableVersion.ByRef("feature", RefKind.BRANCH));
        Assertions.assertSame(s, got);
    }

    @Test
    void resolveByRefTagReturnsSnapshot() {
        SnapshotRef tag = Mockito.mock(SnapshotRef.class);
        Mockito.when(tag.isBranch()).thenReturn(false);
        Mockito.when(tag.isTag()).thenReturn(true);
        Mockito.when(tag.snapshotId()).thenReturn(600L);

        Map<String, SnapshotRef> refs = new HashMap<>();
        refs.put("v1", tag);
        Mockito.when(table.refs()).thenReturn(refs);

        Snapshot s = Mockito.mock(Snapshot.class);
        Mockito.when(table.snapshot(600L)).thenReturn(s);

        Snapshot got = IcebergBackendSupport.resolveVersion(
                catalog, DB, TBL,
                new ConnectorTableVersion.ByRef("v1", RefKind.TAG));
        Assertions.assertSame(s, got);
    }

    @Test
    void resolveByRefMissingNameThrows() {
        Mockito.when(table.refs()).thenReturn(new HashMap<>());
        IcebergBackendException ex = Assertions.assertThrows(
                IcebergBackendException.class,
                () -> IcebergBackendSupport.resolveVersion(
                        catalog, DB, TBL,
                        new ConnectorTableVersion.ByRef("nope", RefKind.BRANCH)));
        Assertions.assertTrue(ex.getMessage().contains("nope"));
    }

    @Test
    void resolveByRefWrongKindThrows() {
        SnapshotRef tag = Mockito.mock(SnapshotRef.class);
        Mockito.when(tag.isBranch()).thenReturn(false);
        Mockito.when(tag.isTag()).thenReturn(true);
        Map<String, SnapshotRef> refs = new HashMap<>();
        refs.put("v1", tag);
        Mockito.when(table.refs()).thenReturn(refs);

        IcebergBackendException ex = Assertions.assertThrows(
                IcebergBackendException.class,
                () -> IcebergBackendSupport.resolveVersion(
                        catalog, DB, TBL,
                        new ConnectorTableVersion.ByRef("v1", RefKind.BRANCH)));
        Assertions.assertTrue(ex.getMessage().contains("not a branch"));
    }

    @Test
    void resolveByRefAtTimestampValidatesRefAndResolvesByTs() {
        SnapshotRef branch = Mockito.mock(SnapshotRef.class);
        Mockito.when(branch.isBranch()).thenReturn(true);
        Mockito.when(branch.isTag()).thenReturn(false);
        Mockito.when(branch.snapshotId()).thenReturn(100L);
        Map<String, SnapshotRef> refs = new HashMap<>();
        refs.put("main", branch);
        Mockito.when(table.refs()).thenReturn(refs);
        Mockito.when(table.snapshot(100L)).thenReturn(Mockito.mock(Snapshot.class));

        Snapshot s = Mockito.mock(Snapshot.class);
        Mockito.when(table.snapshot(123L)).thenReturn(s);

        try (MockedStatic<org.apache.iceberg.util.SnapshotUtil> mocked
                     = Mockito.mockStatic(org.apache.iceberg.util.SnapshotUtil.class)) {
            mocked.when(() -> org.apache.iceberg.util.SnapshotUtil
                            .snapshotIdAsOfTime(table, 999L))
                    .thenReturn(123L);
            Snapshot got = IcebergBackendSupport.resolveVersion(
                    catalog, DB, TBL,
                    new ConnectorTableVersion.ByRefAtTimestamp(
                            "main", RefKind.BRANCH, Instant.ofEpochMilli(999L)));
            Assertions.assertSame(s, got);
        }
    }

    @Test
    void resolveByOpaqueThrowsUoe() {
        UnsupportedOperationException ex = Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> IcebergBackendSupport.resolveVersion(
                        catalog, DB, TBL,
                        new ConnectorTableVersion.ByOpaque("token")));
        Assertions.assertTrue(ex.getMessage().contains("opaque"));
    }

    @Test
    void resolveNullVersionThrows() {
        IcebergBackendException ex = Assertions.assertThrows(
                IcebergBackendException.class,
                () -> IcebergBackendSupport.resolveVersion(catalog, DB, TBL, null));
        Assertions.assertTrue(ex.getMessage().contains("null"));
    }
}
