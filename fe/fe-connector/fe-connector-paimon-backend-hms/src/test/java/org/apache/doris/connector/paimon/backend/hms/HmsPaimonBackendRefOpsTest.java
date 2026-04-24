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

package org.apache.doris.connector.paimon.backend.hms;

import org.apache.doris.connector.api.timetravel.ConnectorRef;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;
import org.apache.doris.connector.api.timetravel.RefKind;
import org.apache.doris.connector.paimon.api.PaimonBackendContext;
import org.apache.doris.connector.paimon.api.PaimonBackendException;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Verifies that {@link HmsPaimonBackend} inherits the shared paimon
 * time-travel default methods and routes through to the underlying
 * {@link Catalog}/{@link DataTable}. Exhaustive matrix lives in
 * {@code PaimonBackendSupportTest}; this test covers per-backend routing.
 */
class HmsPaimonBackendRefOpsTest {

    private static final String DB = "db";
    private static final String TBL = "t";

    private Catalog catalog;
    private DataTable table;
    private BranchManager branchManager;
    private TagManager tagManager;
    private SnapshotManager snapshotManager;
    private HmsPaimonBackend backend;

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

        final Catalog fixed = catalog;
        backend = new HmsPaimonBackend() {
            @Override
            public Catalog buildCatalog(PaimonBackendContext context) {
                return fixed;
            }
        };
    }

    private PaimonBackendContext ctx() {
        return new PaimonBackendContext("c", new HashMap<>());
    }

    private Snapshot snap(long id, long ts) {
        Snapshot s = Mockito.mock(Snapshot.class);
        Mockito.when(s.id()).thenReturn(id);
        Mockito.when(s.timeMillis()).thenReturn(ts);
        return s;
    }

    @Test
    void listRefsReturnsBranchesAndTags() {
        Mockito.when(branchManager.branches()).thenReturn(List.of("main", "dev"));
        TreeMap<Snapshot, List<String>> tags = new TreeMap<>(
                java.util.Comparator.comparingLong(Snapshot::id));
        tags.put(snap(10L, 1L), List.of("v1"));
        Mockito.when(tagManager.tags()).thenReturn(tags);

        List<ConnectorRef> out = backend.listRefs(ctx(), DB, TBL);
        Assertions.assertEquals(3, out.size());
        Assertions.assertEquals(RefKind.BRANCH, out.get(0).kind());
        Assertions.assertEquals(RefKind.BRANCH, out.get(1).kind());
        Assertions.assertEquals(RefKind.TAG, out.get(2).kind());
        Assertions.assertEquals(10L, out.get(2).snapshotId());
    }

    @Test
    void resolveBySnapshotId() {
        Snapshot s = snap(42L, 0L);
        Mockito.when(table.snapshot(42L)).thenReturn(s);
        Snapshot got = backend.resolveVersion(ctx(), DB, TBL,
                new ConnectorTableVersion.BySnapshotId(42L));
        Assertions.assertSame(s, got);
    }

    @Test
    void resolveByTimestamp() {
        Snapshot s = snap(7L, 0L);
        Mockito.when(snapshotManager.earlierOrEqualTimeMills(1_000L)).thenReturn(s);
        Snapshot got = backend.resolveVersion(ctx(), DB, TBL,
                new ConnectorTableVersion.ByTimestamp(Instant.ofEpochMilli(1_000L)));
        Assertions.assertSame(s, got);
    }

    @Test
    void resolveByRefBranch() {
        DataTable branchTable = Mockito.mock(DataTable.class);
        Snapshot s = snap(500L, 0L);
        Mockito.when(table.switchToBranch("feature")).thenReturn(branchTable);
        Mockito.when(branchTable.latestSnapshot()).thenReturn(Optional.of(s));
        Snapshot got = backend.resolveVersion(ctx(), DB, TBL,
                new ConnectorTableVersion.ByRef("feature", RefKind.BRANCH));
        Assertions.assertSame(s, got);
    }

    @Test
    void resolveByRefTag() {
        Tag tag = Mockito.mock(Tag.class);
        Snapshot trimmed = snap(600L, 0L);
        Mockito.when(tagManager.get("v1")).thenReturn(Optional.of(tag));
        Mockito.when(tag.trimToSnapshot()).thenReturn(trimmed);
        Snapshot got = backend.resolveVersion(ctx(), DB, TBL,
                new ConnectorTableVersion.ByRef("v1", RefKind.TAG));
        Assertions.assertSame(trimmed, got);
    }

    @Test
    void resolveByOpaqueThrowsUoe() {
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> backend.resolveVersion(ctx(), DB, TBL,
                        new ConnectorTableVersion.ByOpaque("tok")));
    }

    @Test
    void resolveNullVersionThrowsPaimonBackendException() {
        Assertions.assertThrows(PaimonBackendException.class,
                () -> backend.resolveVersion(ctx(), DB, TBL, null));
    }
}
