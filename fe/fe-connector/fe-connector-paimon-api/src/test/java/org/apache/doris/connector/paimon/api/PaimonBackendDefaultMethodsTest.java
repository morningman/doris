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
import org.apache.paimon.tag.Tag;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Verifies that {@link PaimonBackend}'s default {@code listRefs} and
 * {@code resolveVersion} dispatch every {@link ConnectorTableVersion}
 * sealed-subtype correctly through to {@link PaimonBackendSupport}, and
 * that the default body opens a fresh catalog via
 * {@link PaimonBackend#buildCatalog(PaimonBackendContext)}.
 */
class PaimonBackendDefaultMethodsTest {

    private static final String DB = "db";
    private static final String TBL = "t";

    private static PaimonBackendContext ctx() {
        return new PaimonBackendContext("c", new HashMap<>());
    }

    private static final class FakeBackend implements PaimonBackend {
        final Catalog catalog;
        int builds;

        FakeBackend(Catalog catalog) {
            this.catalog = catalog;
        }

        @Override
        public String name() {
            return "fake";
        }

        @Override
        public Catalog buildCatalog(PaimonBackendContext context) {
            builds++;
            return catalog;
        }
    }

    private Catalog newCatalogWithDataTable(DataTable dt) throws Exception {
        Catalog catalog = Mockito.mock(Catalog.class);
        Mockito.when(catalog.getTable(Identifier.create(DB, TBL))).thenReturn(dt);
        return catalog;
    }

    @Test
    void defaultListRefsBuildsCatalogAndDelegatesToSupport() throws Exception {
        DataTable dt = Mockito.mock(DataTable.class);
        BranchManager bm = Mockito.mock(BranchManager.class);
        TagManager tm = Mockito.mock(TagManager.class);
        Mockito.when(dt.branchManager()).thenReturn(bm);
        Mockito.when(dt.tagManager()).thenReturn(tm);
        Mockito.when(bm.branches()).thenReturn(List.of("main"));
        Mockito.when(tm.tags()).thenReturn(new TreeMap<>(
                java.util.Comparator.comparingLong(Snapshot::id)));

        FakeBackend backend = new FakeBackend(newCatalogWithDataTable(dt));
        List<ConnectorRef> out = backend.listRefs(ctx(), DB, TBL);

        Assertions.assertEquals(1, out.size());
        Assertions.assertEquals("main", out.get(0).name());
        Assertions.assertEquals(RefKind.BRANCH, out.get(0).kind());
        Assertions.assertEquals(1, backend.builds);
    }

    @Test
    void defaultResolveVersionDispatchesBySnapshotId() throws Exception {
        DataTable dt = Mockito.mock(DataTable.class);
        Snapshot s = Mockito.mock(Snapshot.class);
        Mockito.when(dt.snapshot(11L)).thenReturn(s);
        FakeBackend backend = new FakeBackend(newCatalogWithDataTable(dt));

        Snapshot got = backend.resolveVersion(ctx(), DB, TBL,
                new ConnectorTableVersion.BySnapshotId(11L));
        Assertions.assertSame(s, got);
    }

    @Test
    void defaultResolveVersionDispatchesByTimestamp() throws Exception {
        DataTable dt = Mockito.mock(DataTable.class);
        SnapshotManager sm = Mockito.mock(SnapshotManager.class);
        Snapshot s = Mockito.mock(Snapshot.class);
        Mockito.when(dt.snapshotManager()).thenReturn(sm);
        Mockito.when(sm.earlierOrEqualTimeMills(123L)).thenReturn(s);
        FakeBackend backend = new FakeBackend(newCatalogWithDataTable(dt));

        Snapshot got = backend.resolveVersion(ctx(), DB, TBL,
                new ConnectorTableVersion.ByTimestamp(Instant.ofEpochMilli(123L)));
        Assertions.assertSame(s, got);
    }

    @Test
    void defaultResolveVersionDispatchesByRefBranch() throws Exception {
        DataTable dt = Mockito.mock(DataTable.class);
        DataTable branchTable = Mockito.mock(DataTable.class);
        Snapshot s = Mockito.mock(Snapshot.class);
        Mockito.when(dt.switchToBranch("b")).thenReturn(branchTable);
        Mockito.when(branchTable.latestSnapshot()).thenReturn(Optional.of(s));
        FakeBackend backend = new FakeBackend(newCatalogWithDataTable(dt));

        Snapshot got = backend.resolveVersion(ctx(), DB, TBL,
                new ConnectorTableVersion.ByRef("b", RefKind.BRANCH));
        Assertions.assertSame(s, got);
    }

    @Test
    void defaultResolveVersionDispatchesByRefTag() throws Exception {
        DataTable dt = Mockito.mock(DataTable.class);
        TagManager tm = Mockito.mock(TagManager.class);
        Tag tag = Mockito.mock(Tag.class);
        Snapshot trimmed = Mockito.mock(Snapshot.class);
        Mockito.when(dt.tagManager()).thenReturn(tm);
        Mockito.when(tm.get("v1")).thenReturn(Optional.of(tag));
        Mockito.when(tag.trimToSnapshot()).thenReturn(trimmed);
        FakeBackend backend = new FakeBackend(newCatalogWithDataTable(dt));

        Snapshot got = backend.resolveVersion(ctx(), DB, TBL,
                new ConnectorTableVersion.ByRef("v1", RefKind.TAG));
        Assertions.assertSame(trimmed, got);
    }

    @Test
    void defaultResolveVersionDispatchesByRefAtTimestampBranch() throws Exception {
        DataTable dt = Mockito.mock(DataTable.class);
        DataTable branchTable = Mockito.mock(DataTable.class);
        SnapshotManager sm = Mockito.mock(SnapshotManager.class);
        Snapshot s = Mockito.mock(Snapshot.class);
        Mockito.when(dt.switchToBranch("b")).thenReturn(branchTable);
        Mockito.when(branchTable.snapshotManager()).thenReturn(sm);
        Mockito.when(sm.earlierOrEqualTimeMills(50L)).thenReturn(s);
        FakeBackend backend = new FakeBackend(newCatalogWithDataTable(dt));

        Snapshot got = backend.resolveVersion(ctx(), DB, TBL,
                new ConnectorTableVersion.ByRefAtTimestamp(
                        "b", RefKind.BRANCH, Instant.ofEpochMilli(50L)));
        Assertions.assertSame(s, got);
    }

    @Test
    void defaultResolveVersionRejectsByOpaque() throws Exception {
        DataTable dt = Mockito.mock(DataTable.class);
        FakeBackend backend = new FakeBackend(newCatalogWithDataTable(dt));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> backend.resolveVersion(ctx(), DB, TBL,
                        new ConnectorTableVersion.ByOpaque("tok")));
    }

    @Test
    void defaultResolveVersionRejectsNull() throws Exception {
        DataTable dt = Mockito.mock(DataTable.class);
        FakeBackend backend = new FakeBackend(newCatalogWithDataTable(dt));
        Assertions.assertThrows(PaimonBackendException.class,
                () -> backend.resolveVersion(ctx(), DB, TBL, null));
    }

    @Test
    void stubBackendSurfacesPaimonBackendException() {
        // A backend whose buildCatalog throws (mirroring aliyun-dlf stub) must
        // surface the same exception through the inherited default methods.
        PaimonBackend stub = new PaimonBackend() {
            @Override
            public String name() {
                return "stub";
            }

            @Override
            public Catalog buildCatalog(PaimonBackendContext context) {
                throw new PaimonBackendException("not implemented");
            }
        };
        Assertions.assertThrows(PaimonBackendException.class,
                () -> stub.listRefs(ctx(), DB, TBL));
        Assertions.assertThrows(PaimonBackendException.class,
                () -> stub.resolveVersion(ctx(), DB, TBL,
                        new ConnectorTableVersion.BySnapshotId(1L)));
    }

    @Test
    void emptyListRefsReturnsEmpty() throws Exception {
        DataTable dt = Mockito.mock(DataTable.class);
        BranchManager bm = Mockito.mock(BranchManager.class);
        TagManager tm = Mockito.mock(TagManager.class);
        Mockito.when(dt.branchManager()).thenReturn(bm);
        Mockito.when(dt.tagManager()).thenReturn(tm);
        Mockito.when(bm.branches()).thenReturn(Collections.emptyList());
        Mockito.when(tm.tags()).thenReturn(new TreeMap<>(
                java.util.Comparator.comparingLong(Snapshot::id)));
        FakeBackend backend = new FakeBackend(newCatalogWithDataTable(dt));
        Assertions.assertTrue(backend.listRefs(ctx(), DB, TBL).isEmpty());
    }
}
