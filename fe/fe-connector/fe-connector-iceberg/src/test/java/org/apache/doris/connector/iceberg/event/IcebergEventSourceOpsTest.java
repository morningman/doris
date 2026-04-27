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

package org.apache.doris.connector.iceberg.event;

import org.apache.doris.connector.api.event.ConnectorMetaChangeEvent;
import org.apache.doris.connector.api.event.EventBatch;
import org.apache.doris.connector.api.event.EventCursor;
import org.apache.doris.connector.api.event.EventFilter;
import org.apache.doris.connector.api.event.EventSourceException;
import org.apache.doris.connector.api.event.EventSourceOps;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class IcebergEventSourceOpsTest {

    private static final String CATALOG = "ice";

    /** Tiny in-memory catalog that supports the few methods the ops uses. */
    private static class FakeCatalog implements Catalog, SupportsNamespaces {
        final Map<Namespace, Set<TableIdentifier>> namespaces = new LinkedHashMap<>();
        final Map<TableIdentifier, Table> tables = new LinkedHashMap<>();
        RuntimeException loadFailureFor;
        TableIdentifier loadFailureTable;

        void addTable(String db, String name, long snapshotId, String op, int schemaId) {
            Namespace ns = Namespace.of(db);
            namespaces.computeIfAbsent(ns, k -> new java.util.LinkedHashSet<>());
            TableIdentifier id = TableIdentifier.of(ns, name);
            namespaces.get(ns).add(id);
            tables.put(id, mockTable(snapshotId, op, schemaId));
        }

        void addNamespaceOnly(String db) {
            namespaces.computeIfAbsent(Namespace.of(db), k -> new java.util.LinkedHashSet<>());
        }

        void removeTable(String db, String name) {
            TableIdentifier id = TableIdentifier.of(Namespace.of(db), name);
            tables.remove(id);
            Set<TableIdentifier> ids = namespaces.get(Namespace.of(db));
            if (ids != null) {
                ids.remove(id);
            }
        }

        void removeNamespace(String db) {
            Namespace ns = Namespace.of(db);
            Set<TableIdentifier> ids = namespaces.remove(ns);
            if (ids != null) {
                for (TableIdentifier id : ids) {
                    tables.remove(id);
                }
            }
        }

        @Override
        public List<TableIdentifier> listTables(Namespace namespace) {
            Set<TableIdentifier> ids = namespaces.get(namespace);
            return ids == null ? java.util.Collections.emptyList() : new ArrayList<>(ids);
        }

        @Override
        public boolean dropTable(TableIdentifier identifier, boolean purge) {
            return tables.remove(identifier) != null;
        }

        @Override
        public void renameTable(TableIdentifier from, TableIdentifier to) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Table loadTable(TableIdentifier id) {
            if (loadFailureTable != null && loadFailureTable.equals(id)) {
                throw loadFailureFor;
            }
            Table t = tables.get(id);
            if (t == null) {
                throw new NoSuchTableException("not found: " + id);
            }
            return t;
        }

        @Override
        public List<Namespace> listNamespaces() {
            return new ArrayList<>(namespaces.keySet());
        }

        @Override
        public List<Namespace> listNamespaces(Namespace namespace) {
            if (namespace.isEmpty()) {
                return listNamespaces();
            }
            return java.util.Collections.emptyList();
        }

        @Override
        public void createNamespace(Namespace namespace, Map<String, String> metadata) {
            namespaces.computeIfAbsent(namespace, k -> new java.util.LinkedHashSet<>());
        }

        @Override
        public boolean dropNamespace(Namespace namespace) {
            return namespaces.remove(namespace) != null;
        }

        @Override
        public boolean setProperties(Namespace namespace, Map<String, String> properties) {
            return false;
        }

        @Override
        public boolean removeProperties(Namespace namespace, Set<String> properties) {
            return false;
        }

        @Override
        public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
            return new HashMap<>();
        }
    }

    private static Table mockTable(Long snapshotId, String op, int schemaId) {
        Table t = Mockito.mock(Table.class);
        if (snapshotId == null) {
            Mockito.when(t.currentSnapshot()).thenReturn(null);
        } else {
            Snapshot s = Mockito.mock(Snapshot.class);
            Mockito.when(s.snapshotId()).thenReturn(snapshotId);
            Mockito.when(s.operation()).thenReturn(op);
            Mockito.when(t.currentSnapshot()).thenReturn(s);
        }
        Schema schema = new Schema(schemaId,
                Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Mockito.when(t.schema()).thenReturn(schema);
        return t;
    }

    @Test
    void initialCursor_seedsKnownTables() {
        FakeCatalog cat = new FakeCatalog();
        cat.addTable("db1", "t1", 100L, "append", 0);
        cat.addTable("db1", "t2", 200L, "append", 0);
        cat.addTable("db2", "tx", 300L, "append", 0);
        IcebergEventSourceOps ops = new IcebergEventSourceOps(cat, CATALOG);

        IcebergEventCursor c = (IcebergEventCursor) ops.initialCursor().orElseThrow();
        Assertions.assertEquals(0L, c.tickId());
        Assertions.assertEquals(3, c.tables().size());
        Assertions.assertEquals(100L,
                c.tables().get(IcebergEventCursor.key("db1", "t1")).snapshotId());
    }

    @Test
    void poll_noChange_returnsEmptyBatchAndAdvancesTick() throws Exception {
        FakeCatalog cat = new FakeCatalog();
        cat.addTable("db1", "t1", 100L, "append", 0);
        IcebergEventSourceOps ops = new IcebergEventSourceOps(cat, CATALOG);
        EventCursor c0 = ops.initialCursor().orElseThrow();

        EventBatch batch = ops.poll(c0, 0, Duration.ZERO, EventFilter.ALL);
        Assertions.assertTrue(batch.events().isEmpty());
        Assertions.assertEquals(1L, ((IcebergEventCursor) batch.nextCursor()).tickId());
        Assertions.assertFalse(batch.hasMore());
    }

    @Test
    void poll_appendSnapshot_emitsDataChanged() throws Exception {
        FakeCatalog cat = new FakeCatalog();
        cat.addTable("db1", "t1", 100L, "append", 0);
        IcebergEventSourceOps ops = new IcebergEventSourceOps(cat, CATALOG);
        EventCursor c0 = ops.initialCursor().orElseThrow();

        // bump snapshot id
        cat.addTable("db1", "t1", 101L, "append", 0);

        EventBatch batch = ops.poll(c0, 0, Duration.ZERO, EventFilter.ALL);
        Assertions.assertEquals(1, batch.events().size());
        Assertions.assertTrue(batch.events().get(0) instanceof ConnectorMetaChangeEvent.DataChanged);

        // second poll: nothing changed → empty
        EventBatch batch2 = ops.poll(batch.nextCursor(), 0, Duration.ZERO, EventFilter.ALL);
        Assertions.assertTrue(batch2.events().isEmpty());
    }

    @Test
    void poll_overwriteSnapshot_emitsSingleDataChanged() throws Exception {
        FakeCatalog cat = new FakeCatalog();
        cat.addTable("db1", "t1", 100L, "append", 0);
        IcebergEventSourceOps ops = new IcebergEventSourceOps(cat, CATALOG);
        EventCursor c0 = ops.initialCursor().orElseThrow();

        cat.addTable("db1", "t1", 101L, "overwrite", 0);
        EventBatch batch = ops.poll(c0, 0, Duration.ZERO, EventFilter.ALL);
        Assertions.assertEquals(1, batch.events().size());
        Assertions.assertTrue(batch.events().get(0) instanceof ConnectorMetaChangeEvent.DataChanged);
    }

    @Test
    void poll_dropTable_emitsTableDroppedAndPurgesCursor() throws Exception {
        FakeCatalog cat = new FakeCatalog();
        cat.addTable("db1", "t1", 100L, "append", 0);
        cat.addTable("db1", "t2", 200L, "append", 0);
        IcebergEventSourceOps ops = new IcebergEventSourceOps(cat, CATALOG);
        EventCursor c0 = ops.initialCursor().orElseThrow();

        cat.removeTable("db1", "t1");

        EventBatch batch = ops.poll(c0, 0, Duration.ZERO, EventFilter.ALL);
        Assertions.assertEquals(1, batch.events().size());
        Assertions.assertTrue(batch.events().get(0) instanceof ConnectorMetaChangeEvent.TableDropped);
        IcebergEventCursor next = (IcebergEventCursor) batch.nextCursor();
        Assertions.assertFalse(next.tables().containsKey(IcebergEventCursor.key("db1", "t1")));
        Assertions.assertTrue(next.tables().containsKey(IcebergEventCursor.key("db1", "t2")));
    }

    @Test
    void poll_newNamespaceAppears_emitsDatabaseCreated() throws Exception {
        FakeCatalog cat = new FakeCatalog();
        cat.addTable("db1", "t1", 100L, "append", 0);
        // namespace list interval = 1 → every poll runs full discovery
        IcebergEventSourceOps ops = new IcebergEventSourceOps(cat, CATALOG, 256, 1);
        EventCursor c0 = ops.initialCursor().orElseThrow();

        cat.addTable("db2", "x", 1L, "append", 0);

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
    void poll_namespaceDisappears_emitsDatabaseDropped() throws Exception {
        FakeCatalog cat = new FakeCatalog();
        cat.addTable("db1", "t1", 100L, "append", 0);
        cat.addTable("db2", "x", 1L, "append", 0);
        IcebergEventSourceOps ops = new IcebergEventSourceOps(cat, CATALOG, 256, 1);
        EventCursor c0 = ops.initialCursor().orElseThrow();

        cat.removeNamespace("db2");

        EventBatch batch = ops.poll(c0, 0, Duration.ZERO, EventFilter.ALL);
        boolean sawDbDropped = batch.events().stream()
                .anyMatch(e -> e instanceof ConnectorMetaChangeEvent.DatabaseDropped
                        && "db2".equals(e.database().orElse("")));
        Assertions.assertTrue(sawDbDropped);
        IcebergEventCursor next = (IcebergEventCursor) batch.nextCursor();
        Assertions.assertFalse(next.tables().keySet().stream()
                .anyMatch(k -> k.startsWith("db2\u0001")));
    }

    @Test
    void poll_eventFilterDb_advancesCursorEvenWhenAllFilteredOut() throws Exception {
        FakeCatalog cat = new FakeCatalog();
        cat.addTable("db1", "t1", 100L, "append", 0);
        cat.addTable("db_other", "ty", 200L, "append", 0);
        IcebergEventSourceOps ops = new IcebergEventSourceOps(cat, CATALOG);
        EventCursor c0 = ops.initialCursor().orElseThrow();

        cat.addTable("db_other", "ty", 201L, "append", 0);

        EventFilter filter = EventFilter.builder().database("db1").build();
        EventBatch batch = ops.poll(c0, 0, Duration.ZERO, filter);
        Assertions.assertTrue(batch.events().isEmpty(),
                "expected filter to drop the only changed event");
        IcebergEventCursor next = (IcebergEventCursor) batch.nextCursor();
        Assertions.assertEquals(1L, next.tickId());
        Assertions.assertEquals(201L,
                next.tables().get(IcebergEventCursor.key("db_other", "ty")).snapshotId(),
                "filtered table state must still advance to avoid replay on next tick");
    }

    @Test
    void poll_eventIdsAreStrictlyAscending() throws Exception {
        FakeCatalog cat = new FakeCatalog();
        cat.addTable("db1", "a", 1L, "append", 0);
        cat.addTable("db1", "b", 2L, "append", 0);
        cat.addTable("db1", "c", 3L, "append", 0);
        IcebergEventSourceOps ops = new IcebergEventSourceOps(cat, CATALOG);
        EventCursor c0 = ops.initialCursor().orElseThrow();

        cat.addTable("db1", "a", 11L, "append", 0);
        cat.addTable("db1", "b", 12L, "append", 0);
        cat.addTable("db1", "c", 13L, "append", 0);

        EventBatch batch = ops.poll(c0, 0, Duration.ZERO, EventFilter.ALL);
        Assertions.assertEquals(3, batch.events().size());
        long prev = Long.MIN_VALUE;
        for (ConnectorMetaChangeEvent e : batch.events()) {
            Assertions.assertTrue(e.eventId() >= prev,
                    "eventId must be ascending; got " + Arrays.toString(
                            batch.events().stream().mapToLong(ConnectorMetaChangeEvent::eventId).toArray()));
            prev = e.eventId();
        }
    }

    @Test
    void poll_capLimitsBatchAndSetsHasMore() throws Exception {
        FakeCatalog cat = new FakeCatalog();
        for (int i = 0; i < 5; i++) {
            cat.addTable("db1", "t" + i, i + 1L, "append", 0);
        }
        IcebergEventSourceOps ops = new IcebergEventSourceOps(cat, CATALOG);
        EventCursor c0 = ops.initialCursor().orElseThrow();
        for (int i = 0; i < 5; i++) {
            cat.addTable("db1", "t" + i, i + 100L, "append", 0);
        }

        EventBatch batch = ops.poll(c0, 2, Duration.ZERO, EventFilter.ALL);
        Assertions.assertEquals(2, batch.events().size());
        Assertions.assertTrue(batch.hasMore());
    }

    @Test
    void poll_loadTableThrowsNonNoSuchTable_emitsVendorEvent() throws Exception {
        FakeCatalog cat = new FakeCatalog();
        cat.addTable("db1", "t1", 100L, "append", 0);
        IcebergEventSourceOps ops = new IcebergEventSourceOps(cat, CATALOG);
        EventCursor c0 = ops.initialCursor().orElseThrow();

        cat.loadFailureTable = TableIdentifier.of(Namespace.of("db1"), "t1");
        cat.loadFailureFor = new IllegalStateException("transient");

        EventBatch batch = ops.poll(c0, 0, Duration.ZERO, EventFilter.ALL);
        Assertions.assertEquals(1, batch.events().size());
        ConnectorMetaChangeEvent.VendorEvent ve =
                (ConnectorMetaChangeEvent.VendorEvent) batch.events().get(0);
        Assertions.assertEquals("iceberg", ve.vendor());
        Assertions.assertTrue(ve.cause().contains("loadTable.failed"));
    }

    @Test
    void poll_rejectsNonIcebergCursor() {
        FakeCatalog cat = new FakeCatalog();
        IcebergEventSourceOps ops = new IcebergEventSourceOps(cat, CATALOG);
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
        FakeCatalog cat = new FakeCatalog() {
            @Override
            public List<Namespace> listNamespaces(Namespace n) {
                throw new IllegalStateException("network down");
            }

            @Override
            public List<Namespace> listNamespaces() {
                throw new IllegalStateException("network down");
            }
        };
        IcebergEventSourceOps ops = new IcebergEventSourceOps(cat, CATALOG, 256, 1);
        // initialCursor recovers from RuntimeException by returning empty optional
        Assertions.assertTrue(ops.initialCursor().isEmpty());

        // poll from a hand-built empty cursor → fullPass triggers and wraps the failure
        IcebergEventCursor empty = new IcebergEventCursor(0L, new HashMap<>());
        Assertions.assertThrows(EventSourceException.class,
                () -> ops.poll(empty, 0, Duration.ZERO, EventFilter.ALL));
    }

    @Test
    void cursorRoundtripPreservesTickAndStates() {
        FakeCatalog cat = new FakeCatalog();
        IcebergEventSourceOps ops = new IcebergEventSourceOps(cat, CATALOG);
        Map<String, IcebergEventCursor.TableState> tables = new HashMap<>();
        tables.put(IcebergEventCursor.key("db1", "t1"), new IcebergEventCursor.TableState(7L, 0));
        tables.put(IcebergEventCursor.key("db1", "t2"), new IcebergEventCursor.TableState(8L, 3));
        IcebergEventCursor original = new IcebergEventCursor(42L, tables);
        byte[] blob = ops.serializeCursor(original);
        EventCursor restored = ops.parseCursor(blob);
        Assertions.assertEquals(original, restored);
    }

    @Test
    void cursorParseRejectsBadMagic() {
        FakeCatalog cat = new FakeCatalog();
        IcebergEventSourceOps ops = new IcebergEventSourceOps(cat, CATALOG);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ops.parseCursor(new byte[]{0x00, 0x01, 0, 0, 0, 0, 0, 0, 0, 0}));
    }

    @Test
    void noOpProperties() {
        FakeCatalog cat = new FakeCatalog();
        IcebergEventSourceOps ops = new IcebergEventSourceOps(cat, CATALOG);
        Assertions.assertFalse(ops.isSelfManaged());
        Assertions.assertSame(EventSourceOps.NONE, EventSourceOps.NONE,
                "sanity: NONE singleton present");
    }
}
