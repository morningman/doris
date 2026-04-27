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

package org.apache.doris.connector.paimon.event;

import org.apache.doris.connector.api.event.ConnectorMetaChangeEvent;
import org.apache.doris.connector.api.event.EventBatch;
import org.apache.doris.connector.api.event.EventCursor;
import org.apache.doris.connector.api.event.EventFilter;
import org.apache.doris.connector.api.event.EventSourceException;
import org.apache.doris.connector.api.event.EventSourceOps;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class PaimonEventSourceOpsTest {

    private static final String CATALOG = "paimon";

    /** In-memory paimon Catalog stub backed by maps. */
    private static final class FakeCatalog {
        final Map<String, Map<String, Table>> dbs = new LinkedHashMap<>();
        Catalog mock;
        RuntimeException listDatabasesFailure;
        Identifier loadFailureTable;
        RuntimeException loadFailureFor;

        FakeCatalog() {
            this.mock = Mockito.mock(Catalog.class);
            try {
                Mockito.when(mock.listDatabases()).thenAnswer(inv -> {
                    if (listDatabasesFailure != null) {
                        throw listDatabasesFailure;
                    }
                    return new ArrayList<>(dbs.keySet());
                });
                Mockito.when(mock.listTables(Mockito.anyString())).thenAnswer(inv -> {
                    String db = inv.getArgument(0);
                    Map<String, Table> tables = dbs.get(db);
                    if (tables == null) {
                        throw new Catalog.DatabaseNotExistException(db);
                    }
                    return new ArrayList<>(tables.keySet());
                });
                Mockito.when(mock.getTable(Mockito.any(Identifier.class))).thenAnswer(inv -> {
                    Identifier id = inv.getArgument(0);
                    if (loadFailureTable != null && loadFailureTable.equals(id)) {
                        throw loadFailureFor;
                    }
                    Map<String, Table> tables = dbs.get(id.getDatabaseName());
                    if (tables == null) {
                        throw new Catalog.TableNotExistException(id);
                    }
                    Table t = tables.get(id.getObjectName());
                    if (t == null) {
                        throw new Catalog.TableNotExistException(id);
                    }
                    return t;
                });
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }

        Catalog asCatalog() {
            return mock;
        }

        void addTable(String db, String name, long snapshotId, Snapshot.CommitKind kind, long schemaId) {
            dbs.computeIfAbsent(db, k -> new LinkedHashMap<>())
                    .put(name, mockTable(snapshotId, kind, schemaId));
        }

        void addDatabaseOnly(String db) {
            dbs.computeIfAbsent(db, k -> new LinkedHashMap<>());
        }

        void removeTable(String db, String name) {
            Map<String, Table> tables = dbs.get(db);
            if (tables != null) {
                tables.remove(name);
            }
        }

        void removeDatabase(String db) {
            dbs.remove(db);
        }
    }

    private static Table mockTable(long snapshotId, Snapshot.CommitKind kind, long schemaId) {
        Table t = Mockito.mock(Table.class);
        Snapshot s = Mockito.mock(Snapshot.class);
        Mockito.when(s.id()).thenReturn(snapshotId);
        Mockito.when(s.commitKind()).thenReturn(kind);
        Mockito.when(s.schemaId()).thenReturn(schemaId);
        Mockito.when(t.latestSnapshot()).thenReturn(Optional.of(s));
        return t;
    }

    @Test
    void initialCursor_seedsKnownTables() {
        FakeCatalog cat = new FakeCatalog();
        cat.addTable("db1", "t1", 100L, Snapshot.CommitKind.APPEND, 0L);
        cat.addTable("db1", "t2", 200L, Snapshot.CommitKind.APPEND, 0L);
        cat.addTable("db2", "tx", 300L, Snapshot.CommitKind.APPEND, 0L);
        PaimonEventSourceOps ops = new PaimonEventSourceOps(cat.asCatalog(), CATALOG);

        PaimonEventCursor c = (PaimonEventCursor) ops.initialCursor().orElseThrow();
        Assertions.assertEquals(0L, c.tickId());
        Assertions.assertEquals(3, c.tables().size());
        Assertions.assertEquals(100L,
                c.tables().get(PaimonEventCursor.key("db1", "t1")).snapshotId());
    }

    @Test
    void poll_noChange_returnsEmptyBatchAndAdvancesTick() throws Exception {
        FakeCatalog cat = new FakeCatalog();
        cat.addTable("db1", "t1", 100L, Snapshot.CommitKind.APPEND, 0L);
        PaimonEventSourceOps ops = new PaimonEventSourceOps(cat.asCatalog(), CATALOG);
        EventCursor c0 = ops.initialCursor().orElseThrow();

        EventBatch batch = ops.poll(c0, 0, Duration.ZERO, EventFilter.ALL);
        Assertions.assertTrue(batch.events().isEmpty());
        Assertions.assertEquals(1L, ((PaimonEventCursor) batch.nextCursor()).tickId());
        Assertions.assertFalse(batch.hasMore());
    }

    @Test
    void poll_appendSnapshot_emitsDataChangedThenSecondPollEmpty() throws Exception {
        FakeCatalog cat = new FakeCatalog();
        cat.addTable("db1", "t1", 100L, Snapshot.CommitKind.APPEND, 0L);
        PaimonEventSourceOps ops = new PaimonEventSourceOps(cat.asCatalog(), CATALOG);
        EventCursor c0 = ops.initialCursor().orElseThrow();

        cat.addTable("db1", "t1", 101L, Snapshot.CommitKind.APPEND, 0L);

        EventBatch batch = ops.poll(c0, 0, Duration.ZERO, EventFilter.ALL);
        Assertions.assertEquals(1, batch.events().size());
        Assertions.assertTrue(batch.events().get(0) instanceof ConnectorMetaChangeEvent.DataChanged);

        EventBatch batch2 = ops.poll(batch.nextCursor(), 0, Duration.ZERO, EventFilter.ALL);
        Assertions.assertTrue(batch2.events().isEmpty());
    }

    @Test
    void poll_overwriteSnapshot_emitsSingleDataChangedNoVendorSibling() throws Exception {
        FakeCatalog cat = new FakeCatalog();
        cat.addTable("db1", "t1", 100L, Snapshot.CommitKind.APPEND, 0L);
        PaimonEventSourceOps ops = new PaimonEventSourceOps(cat.asCatalog(), CATALOG);
        EventCursor c0 = ops.initialCursor().orElseThrow();

        cat.addTable("db1", "t1", 101L, Snapshot.CommitKind.OVERWRITE, 0L);
        EventBatch batch = ops.poll(c0, 0, Duration.ZERO, EventFilter.ALL);
        Assertions.assertEquals(1, batch.events().size());
        Assertions.assertTrue(batch.events().get(0) instanceof ConnectorMetaChangeEvent.DataChanged);
    }

    @Test
    void poll_compactSnapshot_emitsDataChangedAndVendorSibling() throws Exception {
        FakeCatalog cat = new FakeCatalog();
        cat.addTable("db1", "t1", 100L, Snapshot.CommitKind.APPEND, 0L);
        PaimonEventSourceOps ops = new PaimonEventSourceOps(cat.asCatalog(), CATALOG);
        EventCursor c0 = ops.initialCursor().orElseThrow();

        cat.addTable("db1", "t1", 101L, Snapshot.CommitKind.COMPACT, 0L);
        EventBatch batch = ops.poll(c0, 0, Duration.ZERO, EventFilter.ALL);
        Assertions.assertEquals(2, batch.events().size());
        Assertions.assertTrue(batch.events().get(0) instanceof ConnectorMetaChangeEvent.DataChanged);
        Assertions.assertTrue(batch.events().get(1) instanceof ConnectorMetaChangeEvent.VendorEvent);
    }

    @Test
    void poll_dropTable_emitsTableDroppedAndPurgesCursor() throws Exception {
        FakeCatalog cat = new FakeCatalog();
        cat.addTable("db1", "t1", 100L, Snapshot.CommitKind.APPEND, 0L);
        cat.addTable("db1", "t2", 200L, Snapshot.CommitKind.APPEND, 0L);
        PaimonEventSourceOps ops = new PaimonEventSourceOps(cat.asCatalog(), CATALOG);
        EventCursor c0 = ops.initialCursor().orElseThrow();

        cat.removeTable("db1", "t1");

        EventBatch batch = ops.poll(c0, 0, Duration.ZERO, EventFilter.ALL);
        Assertions.assertEquals(1, batch.events().size());
        Assertions.assertTrue(batch.events().get(0) instanceof ConnectorMetaChangeEvent.TableDropped);
        PaimonEventCursor next = (PaimonEventCursor) batch.nextCursor();
        Assertions.assertFalse(next.tables().containsKey(PaimonEventCursor.key("db1", "t1")));
        Assertions.assertTrue(next.tables().containsKey(PaimonEventCursor.key("db1", "t2")));
    }

    @Test
    void poll_newDatabaseAppears_emitsDatabaseCreated() throws Exception {
        FakeCatalog cat = new FakeCatalog();
        cat.addTable("db1", "t1", 100L, Snapshot.CommitKind.APPEND, 0L);
        // namespaceListInterval=1 so every poll runs full discovery.
        PaimonEventSourceOps ops = new PaimonEventSourceOps(cat.asCatalog(), CATALOG, 256, 1);
        EventCursor c0 = ops.initialCursor().orElseThrow();

        cat.addTable("db2", "x", 1L, Snapshot.CommitKind.APPEND, 0L);

        EventBatch batch = ops.poll(c0, 0, Duration.ZERO, EventFilter.ALL);
        boolean sawDbCreated = batch.events().stream()
                .anyMatch(e -> e instanceof ConnectorMetaChangeEvent.DatabaseCreated
                        && "db2".equals(e.database().orElse("")));
        Assertions.assertTrue(sawDbCreated, "expected DatabaseCreated for db2; got " + batch.events());
        boolean sawTblCreated = batch.events().stream()
                .anyMatch(e -> e instanceof ConnectorMetaChangeEvent.TableCreated
                        && "x".equals(e.table().orElse("")));
        Assertions.assertTrue(sawTblCreated);
    }

    @Test
    void poll_databaseDisappears_emitsDatabaseDropped() throws Exception {
        FakeCatalog cat = new FakeCatalog();
        cat.addTable("db1", "t1", 100L, Snapshot.CommitKind.APPEND, 0L);
        cat.addTable("db2", "x", 1L, Snapshot.CommitKind.APPEND, 0L);
        PaimonEventSourceOps ops = new PaimonEventSourceOps(cat.asCatalog(), CATALOG, 256, 1);
        EventCursor c0 = ops.initialCursor().orElseThrow();

        cat.removeDatabase("db2");

        EventBatch batch = ops.poll(c0, 0, Duration.ZERO, EventFilter.ALL);
        boolean sawDbDropped = batch.events().stream()
                .anyMatch(e -> e instanceof ConnectorMetaChangeEvent.DatabaseDropped
                        && "db2".equals(e.database().orElse("")));
        Assertions.assertTrue(sawDbDropped);
        PaimonEventCursor next = (PaimonEventCursor) batch.nextCursor();
        Assertions.assertFalse(next.tables().keySet().stream()
                .anyMatch(k -> k.startsWith("db2\u0001")));
    }

    @Test
    void poll_eventFilterDb_advancesCursorEvenWhenAllFilteredOut() throws Exception {
        FakeCatalog cat = new FakeCatalog();
        cat.addTable("db1", "t1", 100L, Snapshot.CommitKind.APPEND, 0L);
        cat.addTable("db_other", "ty", 200L, Snapshot.CommitKind.APPEND, 0L);
        PaimonEventSourceOps ops = new PaimonEventSourceOps(cat.asCatalog(), CATALOG);
        EventCursor c0 = ops.initialCursor().orElseThrow();

        cat.addTable("db_other", "ty", 201L, Snapshot.CommitKind.APPEND, 0L);

        EventFilter filter = EventFilter.builder().database("db1").build();
        EventBatch batch = ops.poll(c0, 0, Duration.ZERO, filter);
        Assertions.assertTrue(batch.events().isEmpty(),
                "expected filter to drop the only changed event");
        PaimonEventCursor next = (PaimonEventCursor) batch.nextCursor();
        Assertions.assertEquals(1L, next.tickId());
        Assertions.assertEquals(201L,
                next.tables().get(PaimonEventCursor.key("db_other", "ty")).snapshotId(),
                "filtered table state must still advance to avoid replay on next tick");
    }

    @Test
    void poll_eventIdsAreStrictlyAscending() throws Exception {
        FakeCatalog cat = new FakeCatalog();
        cat.addTable("db1", "a", 1L, Snapshot.CommitKind.APPEND, 0L);
        cat.addTable("db1", "b", 2L, Snapshot.CommitKind.APPEND, 0L);
        cat.addTable("db1", "c", 3L, Snapshot.CommitKind.APPEND, 0L);
        PaimonEventSourceOps ops = new PaimonEventSourceOps(cat.asCatalog(), CATALOG);
        EventCursor c0 = ops.initialCursor().orElseThrow();

        cat.addTable("db1", "a", 11L, Snapshot.CommitKind.APPEND, 0L);
        cat.addTable("db1", "b", 12L, Snapshot.CommitKind.APPEND, 0L);
        cat.addTable("db1", "c", 13L, Snapshot.CommitKind.APPEND, 0L);

        EventBatch batch = ops.poll(c0, 0, Duration.ZERO, EventFilter.ALL);
        Assertions.assertEquals(3, batch.events().size());
        long prev = Long.MIN_VALUE;
        for (ConnectorMetaChangeEvent e : batch.events()) {
            Assertions.assertTrue(e.eventId() >= prev,
                    "eventId must be ascending; got "
                            + Arrays.toString(batch.events().stream()
                                    .mapToLong(ConnectorMetaChangeEvent::eventId).toArray()));
            prev = e.eventId();
        }
    }

    @Test
    void poll_capLimitsBatchAndSetsHasMore() throws Exception {
        FakeCatalog cat = new FakeCatalog();
        for (int i = 0; i < 5; i++) {
            cat.addTable("db1", "t" + i, i + 1L, Snapshot.CommitKind.APPEND, 0L);
        }
        PaimonEventSourceOps ops = new PaimonEventSourceOps(cat.asCatalog(), CATALOG);
        EventCursor c0 = ops.initialCursor().orElseThrow();
        for (int i = 0; i < 5; i++) {
            cat.addTable("db1", "t" + i, i + 100L, Snapshot.CommitKind.APPEND, 0L);
        }

        EventBatch batch = ops.poll(c0, 2, Duration.ZERO, EventFilter.ALL);
        Assertions.assertEquals(2, batch.events().size());
        Assertions.assertTrue(batch.hasMore());
    }

    @Test
    void poll_getTableThrowsNonNotExist_emitsVendorEvent() throws Exception {
        FakeCatalog cat = new FakeCatalog();
        cat.addTable("db1", "t1", 100L, Snapshot.CommitKind.APPEND, 0L);
        PaimonEventSourceOps ops = new PaimonEventSourceOps(cat.asCatalog(), CATALOG);
        EventCursor c0 = ops.initialCursor().orElseThrow();

        cat.loadFailureTable = Identifier.create("db1", "t1");
        cat.loadFailureFor = new IllegalStateException("transient");

        EventBatch batch = ops.poll(c0, 0, Duration.ZERO, EventFilter.ALL);
        Assertions.assertEquals(1, batch.events().size());
        ConnectorMetaChangeEvent.VendorEvent ve =
                (ConnectorMetaChangeEvent.VendorEvent) batch.events().get(0);
        Assertions.assertEquals("paimon", ve.vendor());
        Assertions.assertTrue(ve.cause().contains("getTable.failed"));
    }

    @Test
    void poll_rejectsNonPaimonCursor() {
        FakeCatalog cat = new FakeCatalog();
        PaimonEventSourceOps ops = new PaimonEventSourceOps(cat.asCatalog(), CATALOG);
        EventCursor wrong = new EventCursor() {
            @Override
            public int compareTo(EventCursor other) {
                return 0;
            }

            @Override
            public String describe() {
                return "wrong";
            }
        };
        Assertions.assertThrows(EventSourceException.class,
                () -> ops.poll(wrong, 0, Duration.ZERO, EventFilter.ALL));
    }

    @Test
    void poll_fullDiscoveryFailureWrappedAsEventSourceException() {
        FakeCatalog cat = new FakeCatalog();
        cat.listDatabasesFailure = new IllegalStateException("network down");

        PaimonEventSourceOps ops = new PaimonEventSourceOps(cat.asCatalog(), CATALOG, 256, 1);
        // initialCursor recovers via empty optional
        Assertions.assertTrue(ops.initialCursor().isEmpty());

        // Empty cursor → fullPass triggers and wraps the failure.
        PaimonEventCursor empty = new PaimonEventCursor(0L, new HashMap<>());
        Assertions.assertThrows(EventSourceException.class,
                () -> ops.poll(empty, 0, Duration.ZERO, EventFilter.ALL));
    }

    @Test
    void cursorRoundtripPreservesTickAndStates() {
        FakeCatalog cat = new FakeCatalog();
        PaimonEventSourceOps ops = new PaimonEventSourceOps(cat.asCatalog(), CATALOG);
        Map<String, PaimonEventCursor.TableState> tables = new HashMap<>();
        tables.put(PaimonEventCursor.key("db1", "t1"), new PaimonEventCursor.TableState(7L, 0L));
        tables.put(PaimonEventCursor.key("db1", "t2"), new PaimonEventCursor.TableState(8L, 3L));
        PaimonEventCursor original = new PaimonEventCursor(42L, tables);
        byte[] blob = ops.serializeCursor(original);
        EventCursor restored = ops.parseCursor(blob);
        Assertions.assertEquals(original, restored);
    }

    @Test
    void cursorParseRejectsBadMagic() {
        FakeCatalog cat = new FakeCatalog();
        PaimonEventSourceOps ops = new PaimonEventSourceOps(cat.asCatalog(), CATALOG);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ops.parseCursor(new byte[]{0x00, 0x01, 0, 0, 0, 0, 0, 0, 0, 0}));
    }

    @Test
    void noOpProperties() {
        FakeCatalog cat = new FakeCatalog();
        PaimonEventSourceOps ops = new PaimonEventSourceOps(cat.asCatalog(), CATALOG);
        Assertions.assertFalse(ops.isSelfManaged());
        Assertions.assertSame(EventSourceOps.NONE, EventSourceOps.NONE);
    }

    @Test
    void poll_emptyDatabaseDoesNotEmitTableEvents() throws Exception {
        FakeCatalog cat = new FakeCatalog();
        cat.addDatabaseOnly("empty_db");
        PaimonEventSourceOps ops = new PaimonEventSourceOps(cat.asCatalog(), CATALOG, 256, 1);
        EventCursor c0 = ops.initialCursor().orElseThrow();
        Assertions.assertTrue(((PaimonEventCursor) c0).tables().isEmpty());

        EventBatch batch = ops.poll(c0, 0, Duration.ZERO, EventFilter.ALL);
        // First poll from empty cursor runs a fullPass and may emit DatabaseCreated.
        // Verify no spurious table events are produced for the empty db.
        for (ConnectorMetaChangeEvent e : batch.events()) {
            Assertions.assertFalse(e instanceof ConnectorMetaChangeEvent.TableCreated);
            Assertions.assertFalse(e instanceof ConnectorMetaChangeEvent.DataChanged);
        }
    }

    @Test
    void ctorRejectsBadArguments() {
        FakeCatalog cat = new FakeCatalog();
        Assertions.assertThrows(NullPointerException.class,
                () -> new PaimonEventSourceOps(null, CATALOG));
        Assertions.assertThrows(NullPointerException.class,
                () -> new PaimonEventSourceOps(cat.asCatalog(), null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new PaimonEventSourceOps(cat.asCatalog(), CATALOG, 0, 1));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new PaimonEventSourceOps(cat.asCatalog(), CATALOG, 1, 0));
    }

    /** Tiny helper used in some assertions; suppresses unused warnings. */
    @SuppressWarnings("unused")
    private static List<String> dbNames(Map<String, ?> m) {
        return m == null ? Collections.emptyList() : new ArrayList<>(m.keySet());
    }
}
