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

package org.apache.doris.connector.paimon.api;

import org.apache.doris.connector.api.timetravel.ConnectorRef;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;
import org.apache.doris.connector.api.timetravel.RefKind;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Unit tests for {@link PaimonBackendSupport}: exercises the shared ref /
 * version resolution logic over a Mockito-backed
 * {@link Catalog}/{@link DataTable}/{@link BranchManager}/{@link TagManager}
 * stack. Real backends inherit these semantics via the
 * {@link PaimonBackend} default methods; per-backend smoke tests cover the
 * routing.
 */
class PaimonBackendSupportTest {

    private static final String DB = "db";
    private static final String TBL = "t";

    private Catalog catalog;
    private DataTable table;
    private BranchManager branchManager;
    private TagManager tagManager;
    private SnapshotManager snapshotManager;

    @BeforeEach
    void setUp() throws Exception {
        catalog = Mockito.mock(Catalog.class);
        table = Mockito.mock(DataTable.class);
        branchManager = Mockito.mock(BranchManager.class);
        tagManager = Mockito.mock(TagManager.class);
        snapshotManager = Mockito.mock(SnapshotManager.class);
        Mockito.when(catalog.getTable(Identifier.create(DB, TBL))).thenReturn(table);
        Mockito.when(table.branchManager()).thenReturn(branchManager);
        Mockito.when(table.tagManager()).thenReturn(tagManager);
        Mockito.when(table.snapshotManager()).thenReturn(snapshotManager);
    }

    private Snapshot snapshotMock(long id, long ts) {
        Snapshot s = Mockito.mock(Snapshot.class);
        Mockito.when(s.id()).thenReturn(id);
        Mockito.when(s.timeMillis()).thenReturn(ts);
        return s;
    }

    // ---- listRefs ----

    @Test
    void listRefsConvertsBranchesAndTags() {
        Mockito.when(branchManager.branches()).thenReturn(List.of("main", "dev"));
        TreeMap<Snapshot, List<String>> tags = new TreeMap<>(
                java.util.Comparator.comparingLong(Snapshot::id));
        Snapshot snap100 = snapshotMock(100L, 1_700_000_000_000L);
        Snapshot snap200 = snapshotMock(200L, 1_700_000_001_000L);
        tags.put(snap100, List.of("v1"));
        tags.put(snap200, List.of("v2", "v2-alias"));
        Mockito.when(tagManager.tags()).thenReturn(tags);

        List<ConnectorRef> out = PaimonBackendSupport.listRefs(catalog, DB, TBL);

        Assertions.assertEquals(5, out.size());
        Assertions.assertEquals("main", out.get(0).name());
        Assertions.assertEquals(RefKind.BRANCH, out.get(0).kind());
        Assertions.assertEquals(-1L, out.get(0).snapshotId());
        Assertions.assertTrue(out.get(0).createdAt().isEmpty());
        Assertions.assertEquals("dev", out.get(1).name());
        Assertions.assertEquals(RefKind.BRANCH, out.get(1).kind());

        Assertions.assertEquals("v1", out.get(2).name());
        Assertions.assertEquals(RefKind.TAG, out.get(2).kind());
        Assertions.assertEquals(100L, out.get(2).snapshotId());
        Assertions.assertEquals(Instant.ofEpochMilli(1_700_000_000_000L),
                out.get(2).createdAt().orElseThrow());

        Assertions.assertEquals("v2", out.get(3).name());
        Assertions.assertEquals(RefKind.TAG, out.get(3).kind());
        Assertions.assertEquals(200L, out.get(3).snapshotId());
        Assertions.assertEquals("v2-alias", out.get(4).name());
        Assertions.assertEquals(200L, out.get(4).snapshotId());
    }

    @Test
    void listRefsEmptyWhenNoBranchesAndNoTags() {
        Mockito.when(branchManager.branches()).thenReturn(Collections.emptyList());
        Mockito.when(tagManager.tags()).thenReturn(new TreeMap<>(
                java.util.Comparator.comparingLong(Snapshot::id)));
        List<ConnectorRef> out = PaimonBackendSupport.listRefs(catalog, DB, TBL);
        Assertions.assertTrue(out.isEmpty());
    }

    // ---- resolveVersion ----

    @Test
    void resolveBySnapshotIdReturnsSnapshot() {
        Snapshot s = snapshotMock(42L, 0L);
        Mockito.when(table.snapshot(42L)).thenReturn(s);
        Snapshot got = PaimonBackendSupport.resolveVersion(catalog, DB, TBL,
                new ConnectorTableVersion.BySnapshotId(42L));
        Assertions.assertSame(s, got);
    }

    @Test
    void resolveBySnapshotIdMissingThrows() {
        Mockito.when(table.snapshot(99L)).thenReturn(null);
        Assertions.assertThrows(PaimonBackendException.class,
                () -> PaimonBackendSupport.resolveVersion(catalog, DB, TBL,
                        new ConnectorTableVersion.BySnapshotId(99L)));
    }

    @Test
    void resolveByTimestamp() {
        Snapshot s = snapshotMock(7L, 0L);
        Mockito.when(snapshotManager.earlierOrEqualTimeMills(1_000L)).thenReturn(s);
        Snapshot got = PaimonBackendSupport.resolveVersion(catalog, DB, TBL,
                new ConnectorTableVersion.ByTimestamp(Instant.ofEpochMilli(1_000L)));
        Assertions.assertSame(s, got);
    }

    @Test
    void resolveByTimestampMissingThrows() {
        Mockito.when(snapshotManager.earlierOrEqualTimeMills(Mockito.anyLong()))
                .thenReturn(null);
        Assertions.assertThrows(PaimonBackendException.class,
                () -> PaimonBackendSupport.resolveVersion(catalog, DB, TBL,
                        new ConnectorTableVersion.ByTimestamp(Instant.ofEpochMilli(5L))));
    }

    @Test
    void resolveByRefBranchUsesSwitchToBranch() {
        DataTable branchTable = Mockito.mock(DataTable.class);
        Snapshot s = snapshotMock(500L, 0L);
        Mockito.when(table.switchToBranch("feature")).thenReturn(branchTable);
        Mockito.when(branchTable.latestSnapshot()).thenReturn(Optional.of(s));

        Snapshot got = PaimonBackendSupport.resolveVersion(catalog, DB, TBL,
                new ConnectorTableVersion.ByRef("feature", RefKind.BRANCH));
        Assertions.assertSame(s, got);
    }

    @Test
    void resolveByRefBranchEmptyThrows() {
        DataTable branchTable = Mockito.mock(DataTable.class);
        Mockito.when(table.switchToBranch("empty")).thenReturn(branchTable);
        Mockito.when(branchTable.latestSnapshot()).thenReturn(Optional.empty());

        Assertions.assertThrows(PaimonBackendException.class,
                () -> PaimonBackendSupport.resolveVersion(catalog, DB, TBL,
                        new ConnectorTableVersion.ByRef("empty", RefKind.BRANCH)));
    }

    @Test
    void resolveByRefTagUsesTagManager() {
        Snapshot trimmed = snapshotMock(600L, 0L);
        Tag tag = Mockito.mock(Tag.class);
        Mockito.when(tag.trimToSnapshot()).thenReturn(trimmed);
        Mockito.when(tagManager.get("v1")).thenReturn(Optional.of(tag));

        Snapshot got = PaimonBackendSupport.resolveVersion(catalog, DB, TBL,
                new ConnectorTableVersion.ByRef("v1", RefKind.TAG));
        Assertions.assertSame(trimmed, got);
    }

    @Test
    void resolveByRefTagMissingThrows() {
        Mockito.when(tagManager.get("missing")).thenReturn(Optional.empty());
        Assertions.assertThrows(PaimonBackendException.class,
                () -> PaimonBackendSupport.resolveVersion(catalog, DB, TBL,
                        new ConnectorTableVersion.ByRef("missing", RefKind.TAG)));
    }

    @Test
    void resolveByRefUnknownKindThrows() {
        Assertions.assertThrows(PaimonBackendException.class,
                () -> PaimonBackendSupport.resolveVersion(catalog, DB, TBL,
                        new ConnectorTableVersion.ByRef("x", RefKind.UNKNOWN)));
    }

    @Test
    void resolveByRefAtTimestampBranch() {
        DataTable branchTable = Mockito.mock(DataTable.class);
        SnapshotManager bsm = Mockito.mock(SnapshotManager.class);
        Snapshot s = snapshotMock(800L, 0L);
        Mockito.when(table.switchToBranch("feature")).thenReturn(branchTable);
        Mockito.when(branchTable.snapshotManager()).thenReturn(bsm);
        Mockito.when(bsm.earlierOrEqualTimeMills(2_000L)).thenReturn(s);

        Snapshot got = PaimonBackendSupport.resolveVersion(catalog, DB, TBL,
                new ConnectorTableVersion.ByRefAtTimestamp(
                        "feature", RefKind.BRANCH, Instant.ofEpochMilli(2_000L)));
        Assertions.assertSame(s, got);
    }

    @Test
    void resolveByRefAtTimestampTagThrows() {
        Assertions.assertThrows(PaimonBackendException.class,
                () -> PaimonBackendSupport.resolveVersion(catalog, DB, TBL,
                        new ConnectorTableVersion.ByRefAtTimestamp(
                                "v1", RefKind.TAG, Instant.ofEpochMilli(5L))));
    }

    @Test
    void resolveByOpaqueThrowsUoe() {
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> PaimonBackendSupport.resolveVersion(catalog, DB, TBL,
                        new ConnectorTableVersion.ByOpaque("tok")));
    }

    @Test
    void resolveNullVersionThrowsPaimonBackendException() {
        Assertions.assertThrows(PaimonBackendException.class,
                () -> PaimonBackendSupport.resolveVersion(catalog, DB, TBL, null));
    }

    @Test
    void loadDataTableTableNotExistWraps() throws Exception {
        Mockito.when(catalog.getTable(Identifier.create(DB, TBL)))
                .thenThrow(new Catalog.TableNotExistException(Identifier.create(DB, TBL)));
        Assertions.assertThrows(PaimonBackendException.class,
                () -> PaimonBackendSupport.listRefs(catalog, DB, TBL));
    }

    @Test
    void loadDataTableNonDataTableThrows() throws Exception {
        Table nonData = Mockito.mock(Table.class);
        Mockito.when(catalog.getTable(Identifier.create(DB, TBL))).thenReturn(nonData);
        Assertions.assertThrows(PaimonBackendException.class,
                () -> PaimonBackendSupport.listRefs(catalog, DB, TBL));
    }
}
