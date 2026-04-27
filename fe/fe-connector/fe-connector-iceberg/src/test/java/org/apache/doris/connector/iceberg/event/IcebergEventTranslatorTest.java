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

import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.List;

class IcebergEventTranslatorTest {

    private static final String CATALOG = "iceberg_cat";
    private static final TableIdentifier ID =
            TableIdentifier.of(Namespace.of("db1"), "tbl1");

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
    void diffTable_noPrevious_emitsTableCreated() {
        IcebergEventTranslator tr = new IcebergEventTranslator(CATALOG);
        List<ConnectorMetaChangeEvent> ev = tr.diffTable(
                10L, Instant.EPOCH, ID, null, mockTable(7L, "append", 0));
        Assertions.assertEquals(1, ev.size());
        Assertions.assertTrue(ev.get(0) instanceof ConnectorMetaChangeEvent.TableCreated);
        Assertions.assertEquals("db1", ev.get(0).database().orElseThrow());
        Assertions.assertEquals("tbl1", ev.get(0).table().orElseThrow());
        Assertions.assertEquals(CATALOG, ev.get(0).catalog());
        Assertions.assertEquals(10L, ev.get(0).eventId());
    }

    @Test
    void diffTable_currentNullWithPrevious_emitsTableDropped() {
        IcebergEventTranslator tr = new IcebergEventTranslator(CATALOG);
        IcebergEventCursor.TableState prev = new IcebergEventCursor.TableState(7L, 0);
        List<ConnectorMetaChangeEvent> ev = tr.diffTable(11L, Instant.EPOCH, ID, prev, null);
        Assertions.assertEquals(1, ev.size());
        Assertions.assertTrue(ev.get(0) instanceof ConnectorMetaChangeEvent.TableDropped);
    }

    @Test
    void diffTable_currentNullNoPrevious_emitsNothing() {
        IcebergEventTranslator tr = new IcebergEventTranslator(CATALOG);
        Assertions.assertTrue(tr.diffTable(1L, Instant.EPOCH, ID, null, null).isEmpty());
    }

    @Test
    void diffTable_appendSnapshot_emitsDataChangedKnownOp() {
        IcebergEventTranslator tr = new IcebergEventTranslator(CATALOG);
        IcebergEventCursor.TableState prev = new IcebergEventCursor.TableState(7L, 0);
        List<ConnectorMetaChangeEvent> ev = tr.diffTable(
                12L, Instant.EPOCH, ID, prev, mockTable(8L, "append", 0));
        Assertions.assertEquals(1, ev.size());
        ConnectorMetaChangeEvent.DataChanged dc = (ConnectorMetaChangeEvent.DataChanged) ev.get(0);
        Assertions.assertEquals(8L, dc.snapshotId().orElseThrow());
        Assertions.assertTrue(dc.partitionSpec().isEmpty(),
                "iceberg per-partition info not enumerable; expect table-scope invalidation");
        Assertions.assertTrue(dc.cause().contains("op=append"));
    }

    @Test
    void diffTable_overwriteSnapshot_emitsDataChangedKnownOp() {
        IcebergEventTranslator tr = new IcebergEventTranslator(CATALOG);
        IcebergEventCursor.TableState prev = new IcebergEventCursor.TableState(1L, 0);
        List<ConnectorMetaChangeEvent> ev = tr.diffTable(
                13L, Instant.EPOCH, ID, prev, mockTable(2L, "overwrite", 0));
        Assertions.assertEquals(1, ev.size(), "known op should not produce VendorEvent sibling");
        Assertions.assertTrue(ev.get(0) instanceof ConnectorMetaChangeEvent.DataChanged);
    }

    @Test
    void diffTable_deleteSnapshot_emitsDataChanged() {
        IcebergEventTranslator tr = new IcebergEventTranslator(CATALOG);
        IcebergEventCursor.TableState prev = new IcebergEventCursor.TableState(1L, 0);
        List<ConnectorMetaChangeEvent> ev = tr.diffTable(
                14L, Instant.EPOCH, ID, prev, mockTable(2L, "delete", 0));
        Assertions.assertEquals(1, ev.size());
        Assertions.assertTrue(ev.get(0) instanceof ConnectorMetaChangeEvent.DataChanged);
    }

    @Test
    void diffTable_replaceSnapshot_emitsDataChanged() {
        IcebergEventTranslator tr = new IcebergEventTranslator(CATALOG);
        IcebergEventCursor.TableState prev = new IcebergEventCursor.TableState(1L, 0);
        List<ConnectorMetaChangeEvent> ev = tr.diffTable(
                15L, Instant.EPOCH, ID, prev, mockTable(3L, "replace", 0));
        Assertions.assertEquals(1, ev.size());
        Assertions.assertTrue(ev.get(0) instanceof ConnectorMetaChangeEvent.DataChanged);
    }

    @Test
    void diffTable_unknownOp_emitsDataChangedAndVendorEvent() {
        IcebergEventTranslator tr = new IcebergEventTranslator(CATALOG);
        IcebergEventCursor.TableState prev = new IcebergEventCursor.TableState(1L, 0);
        List<ConnectorMetaChangeEvent> ev = tr.diffTable(
                16L, Instant.EPOCH, ID, prev, mockTable(2L, "weird-op", 0));
        Assertions.assertEquals(2, ev.size());
        Assertions.assertTrue(ev.get(0) instanceof ConnectorMetaChangeEvent.DataChanged);
        ConnectorMetaChangeEvent.VendorEvent ve = (ConnectorMetaChangeEvent.VendorEvent) ev.get(1);
        Assertions.assertEquals("iceberg", ve.vendor());
        Assertions.assertEquals("weird-op", ve.attributes().get("op"));
    }

    @Test
    void diffTable_schemaIdChanged_emitsTableAltered() {
        IcebergEventTranslator tr = new IcebergEventTranslator(CATALOG);
        IcebergEventCursor.TableState prev = new IcebergEventCursor.TableState(7L, 0);
        List<ConnectorMetaChangeEvent> ev = tr.diffTable(
                17L, Instant.EPOCH, ID, prev, mockTable(7L, "append", 5));
        Assertions.assertEquals(1, ev.size());
        Assertions.assertTrue(ev.get(0) instanceof ConnectorMetaChangeEvent.TableAltered);
        Assertions.assertTrue(ev.get(0).cause().contains("0->5"));
    }

    @Test
    void diffTable_schemaAndSnapshotChanged_emitsBothAlterAndData() {
        IcebergEventTranslator tr = new IcebergEventTranslator(CATALOG);
        IcebergEventCursor.TableState prev = new IcebergEventCursor.TableState(7L, 0);
        List<ConnectorMetaChangeEvent> ev = tr.diffTable(
                18L, Instant.EPOCH, ID, prev, mockTable(8L, "append", 5));
        Assertions.assertEquals(2, ev.size());
        Assertions.assertTrue(ev.get(0) instanceof ConnectorMetaChangeEvent.TableAltered);
        Assertions.assertTrue(ev.get(1) instanceof ConnectorMetaChangeEvent.DataChanged);
        Assertions.assertEquals(ev.get(0).eventId(), ev.get(1).eventId(),
                "events from same diff share eventId (allowed by EventBatch invariant)");
    }

    @Test
    void diffTable_noChange_emitsNothing() {
        IcebergEventTranslator tr = new IcebergEventTranslator(CATALOG);
        IcebergEventCursor.TableState prev = new IcebergEventCursor.TableState(7L, 0);
        Assertions.assertTrue(
                tr.diffTable(19L, Instant.EPOCH, ID, prev, mockTable(7L, "append", 0)).isEmpty());
    }

    @Test
    void diffTable_currentSnapshotNull_treatsAsZero() {
        IcebergEventTranslator tr = new IcebergEventTranslator(CATALOG);
        IcebergEventCursor.TableState prev = new IcebergEventCursor.TableState(7L, 0);
        List<ConnectorMetaChangeEvent> ev = tr.diffTable(
                20L, Instant.EPOCH, ID, prev, mockTable(null, null, 0));
        Assertions.assertEquals(1, ev.size());
        ConnectorMetaChangeEvent.DataChanged dc = (ConnectorMetaChangeEvent.DataChanged) ev.get(0);
        Assertions.assertTrue(dc.snapshotId().isEmpty());
    }

    @Test
    void dbCreatedAndDroppedEventsHaveLowercaseDb() {
        IcebergEventTranslator tr = new IcebergEventTranslator(CATALOG);
        ConnectorMetaChangeEvent c = tr.dbCreated(1L, Instant.EPOCH, Namespace.of("MyDb"));
        Assertions.assertTrue(c instanceof ConnectorMetaChangeEvent.DatabaseCreated);
        Assertions.assertEquals("mydb", c.database().orElseThrow());

        ConnectorMetaChangeEvent d = tr.dbDropped(2L, Instant.EPOCH, "OtherDb");
        Assertions.assertTrue(d instanceof ConnectorMetaChangeEvent.DatabaseDropped);
        Assertions.assertEquals("otherdb", d.database().orElseThrow());
    }

    @Test
    void loadFailedReturnsVendorEventWithErrorAttributes() {
        IcebergEventTranslator tr = new IcebergEventTranslator(CATALOG);
        ConnectorMetaChangeEvent.VendorEvent ev = (ConnectorMetaChangeEvent.VendorEvent)
                tr.loadFailed(99L, Instant.EPOCH, ID, new RuntimeException("boom"));
        Assertions.assertEquals("iceberg", ev.vendor());
        Assertions.assertEquals("RuntimeException", ev.attributes().get("error"));
        Assertions.assertEquals("boom", ev.attributes().get("message"));
        Assertions.assertEquals("db1", ev.database().orElseThrow());
        Assertions.assertEquals("tbl1", ev.table().orElseThrow());
    }
}
