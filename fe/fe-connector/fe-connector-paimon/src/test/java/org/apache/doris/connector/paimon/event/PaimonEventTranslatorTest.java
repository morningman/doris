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

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

class PaimonEventTranslatorTest {

    private static final String CATALOG = "paimon_cat";
    private static final Identifier ID = Identifier.create("db1", "tbl1");

    private static Table mockTable(Long snapshotId, Snapshot.CommitKind kind, Long schemaId) {
        Table t = Mockito.mock(Table.class);
        if (snapshotId == null) {
            Mockito.when(t.latestSnapshot()).thenReturn(Optional.empty());
        } else {
            Snapshot s = Mockito.mock(Snapshot.class);
            Mockito.when(s.id()).thenReturn(snapshotId);
            Mockito.when(s.commitKind()).thenReturn(kind);
            Mockito.when(s.schemaId()).thenReturn(schemaId == null ? 0L : schemaId);
            Mockito.when(t.latestSnapshot()).thenReturn(Optional.of(s));
        }
        return t;
    }

    @Test
    void diffTable_noPrevious_emitsTableCreated() {
        PaimonEventTranslator tr = new PaimonEventTranslator(CATALOG);
        List<ConnectorMetaChangeEvent> ev = tr.diffTable(
                10L, Instant.EPOCH, ID, null, mockTable(7L, Snapshot.CommitKind.APPEND, 0L));
        Assertions.assertEquals(1, ev.size());
        Assertions.assertTrue(ev.get(0) instanceof ConnectorMetaChangeEvent.TableCreated);
        Assertions.assertEquals("db1", ev.get(0).database().orElseThrow());
        Assertions.assertEquals("tbl1", ev.get(0).table().orElseThrow());
        Assertions.assertEquals(CATALOG, ev.get(0).catalog());
        Assertions.assertEquals(10L, ev.get(0).eventId());
    }

    @Test
    void diffTable_currentNullWithPrevious_emitsTableDropped() {
        PaimonEventTranslator tr = new PaimonEventTranslator(CATALOG);
        PaimonEventCursor.TableState prev = new PaimonEventCursor.TableState(7L, 0L);
        List<ConnectorMetaChangeEvent> ev = tr.diffTable(11L, Instant.EPOCH, ID, prev, null);
        Assertions.assertEquals(1, ev.size());
        Assertions.assertTrue(ev.get(0) instanceof ConnectorMetaChangeEvent.TableDropped);
    }

    @Test
    void diffTable_currentNullNoPrevious_emitsNothing() {
        PaimonEventTranslator tr = new PaimonEventTranslator(CATALOG);
        Assertions.assertTrue(tr.diffTable(1L, Instant.EPOCH, ID, null, null).isEmpty());
    }

    @Test
    void diffTable_appendSnapshot_emitsDataChangedNoVendorSibling() {
        PaimonEventTranslator tr = new PaimonEventTranslator(CATALOG);
        PaimonEventCursor.TableState prev = new PaimonEventCursor.TableState(7L, 0L);
        List<ConnectorMetaChangeEvent> ev = tr.diffTable(
                12L, Instant.EPOCH, ID, prev, mockTable(8L, Snapshot.CommitKind.APPEND, 0L));
        Assertions.assertEquals(1, ev.size());
        ConnectorMetaChangeEvent.DataChanged dc = (ConnectorMetaChangeEvent.DataChanged) ev.get(0);
        Assertions.assertEquals(8L, dc.snapshotId().orElseThrow());
        Assertions.assertTrue(dc.partitionSpec().isEmpty());
        Assertions.assertTrue(dc.cause().contains("kind=append"));
    }

    @Test
    void diffTable_overwriteSnapshot_emitsSingleDataChanged() {
        PaimonEventTranslator tr = new PaimonEventTranslator(CATALOG);
        PaimonEventCursor.TableState prev = new PaimonEventCursor.TableState(1L, 0L);
        List<ConnectorMetaChangeEvent> ev = tr.diffTable(
                13L, Instant.EPOCH, ID, prev, mockTable(2L, Snapshot.CommitKind.OVERWRITE, 0L));
        Assertions.assertEquals(1, ev.size(),
                "overwrite is plain data-change; no vendor sibling");
        Assertions.assertTrue(ev.get(0) instanceof ConnectorMetaChangeEvent.DataChanged);
        Assertions.assertTrue(ev.get(0).cause().contains("kind=overwrite"));
    }

    @Test
    void diffTable_compactSnapshot_emitsDataChangedAndVendorSibling() {
        PaimonEventTranslator tr = new PaimonEventTranslator(CATALOG);
        PaimonEventCursor.TableState prev = new PaimonEventCursor.TableState(1L, 0L);
        List<ConnectorMetaChangeEvent> ev = tr.diffTable(
                14L, Instant.EPOCH, ID, prev, mockTable(2L, Snapshot.CommitKind.COMPACT, 0L));
        Assertions.assertEquals(2, ev.size());
        Assertions.assertTrue(ev.get(0) instanceof ConnectorMetaChangeEvent.DataChanged);
        ConnectorMetaChangeEvent.VendorEvent ve = (ConnectorMetaChangeEvent.VendorEvent) ev.get(1);
        Assertions.assertEquals("paimon", ve.vendor());
        Assertions.assertEquals("compact", ve.attributes().get("kind"));
        Assertions.assertEquals(ev.get(0).eventId(), ev.get(1).eventId());
    }

    @Test
    void diffTable_analyzeSnapshot_emitsDataChangedAndVendorSibling() {
        PaimonEventTranslator tr = new PaimonEventTranslator(CATALOG);
        PaimonEventCursor.TableState prev = new PaimonEventCursor.TableState(1L, 0L);
        List<ConnectorMetaChangeEvent> ev = tr.diffTable(
                15L, Instant.EPOCH, ID, prev, mockTable(2L, Snapshot.CommitKind.ANALYZE, 0L));
        Assertions.assertEquals(2, ev.size());
        Assertions.assertTrue(ev.get(0) instanceof ConnectorMetaChangeEvent.DataChanged);
        ConnectorMetaChangeEvent.VendorEvent ve = (ConnectorMetaChangeEvent.VendorEvent) ev.get(1);
        Assertions.assertEquals("analyze", ve.attributes().get("kind"));
    }

    @Test
    void diffTable_unknownNullKind_emitsDataChangedAndVendorSibling() {
        PaimonEventTranslator tr = new PaimonEventTranslator(CATALOG);
        PaimonEventCursor.TableState prev = new PaimonEventCursor.TableState(1L, 0L);
        // Snapshot present but commitKind() returns null → translator should
        // still emit DataChanged + VendorEvent fallback.
        Table t = Mockito.mock(Table.class);
        Snapshot s = Mockito.mock(Snapshot.class);
        Mockito.when(s.id()).thenReturn(2L);
        Mockito.when(s.commitKind()).thenReturn(null);
        Mockito.when(s.schemaId()).thenReturn(0L);
        Mockito.when(t.latestSnapshot()).thenReturn(Optional.of(s));

        List<ConnectorMetaChangeEvent> ev = tr.diffTable(16L, Instant.EPOCH, ID, prev, t);
        Assertions.assertEquals(2, ev.size());
        Assertions.assertTrue(ev.get(0) instanceof ConnectorMetaChangeEvent.DataChanged);
        ConnectorMetaChangeEvent.VendorEvent ve = (ConnectorMetaChangeEvent.VendorEvent) ev.get(1);
        Assertions.assertEquals("unknown", ve.attributes().get("kind"));
        Assertions.assertTrue(ve.cause().contains("paimon.unknown.kind"));
    }

    @Test
    void diffTable_schemaIdChanged_emitsTableAltered() {
        PaimonEventTranslator tr = new PaimonEventTranslator(CATALOG);
        PaimonEventCursor.TableState prev = new PaimonEventCursor.TableState(7L, 0L);
        List<ConnectorMetaChangeEvent> ev = tr.diffTable(
                17L, Instant.EPOCH, ID, prev, mockTable(7L, Snapshot.CommitKind.APPEND, 5L));
        Assertions.assertEquals(1, ev.size());
        Assertions.assertTrue(ev.get(0) instanceof ConnectorMetaChangeEvent.TableAltered);
        Assertions.assertTrue(ev.get(0).cause().contains("0->5"));
    }

    @Test
    void diffTable_schemaAndSnapshotChanged_emitsBothAlterAndDataSharedEventId() {
        PaimonEventTranslator tr = new PaimonEventTranslator(CATALOG);
        PaimonEventCursor.TableState prev = new PaimonEventCursor.TableState(7L, 0L);
        List<ConnectorMetaChangeEvent> ev = tr.diffTable(
                18L, Instant.EPOCH, ID, prev, mockTable(8L, Snapshot.CommitKind.APPEND, 5L));
        Assertions.assertEquals(2, ev.size());
        Assertions.assertTrue(ev.get(0) instanceof ConnectorMetaChangeEvent.TableAltered);
        Assertions.assertTrue(ev.get(1) instanceof ConnectorMetaChangeEvent.DataChanged);
        Assertions.assertEquals(ev.get(0).eventId(), ev.get(1).eventId());
    }

    @Test
    void diffTable_noChange_emitsNothing() {
        PaimonEventTranslator tr = new PaimonEventTranslator(CATALOG);
        PaimonEventCursor.TableState prev = new PaimonEventCursor.TableState(7L, 0L);
        Assertions.assertTrue(tr.diffTable(
                19L, Instant.EPOCH, ID, prev, mockTable(7L, Snapshot.CommitKind.APPEND, 0L)).isEmpty());
    }

    @Test
    void diffTable_currentSnapshotEmpty_treatsAsZeroAndEmitsVendorFallback() {
        PaimonEventTranslator tr = new PaimonEventTranslator(CATALOG);
        PaimonEventCursor.TableState prev = new PaimonEventCursor.TableState(7L, 0L);
        List<ConnectorMetaChangeEvent> ev = tr.diffTable(
                20L, Instant.EPOCH, ID, prev, mockTable(null, null, null));
        Assertions.assertEquals(2, ev.size());
        ConnectorMetaChangeEvent.DataChanged dc = (ConnectorMetaChangeEvent.DataChanged) ev.get(0);
        Assertions.assertTrue(dc.snapshotId().isEmpty(),
                "no current snapshot → DataChanged carries no snapshotId");
        Assertions.assertTrue(ev.get(1) instanceof ConnectorMetaChangeEvent.VendorEvent,
                "absent commit kind → vendor fallback so the table-level "
                        + "drop-of-snapshots semantic is observable");
    }

    @Test
    void dbCreatedAndDroppedEventsHaveLowercaseDb() {
        PaimonEventTranslator tr = new PaimonEventTranslator(CATALOG);
        ConnectorMetaChangeEvent c = tr.dbCreated(1L, Instant.EPOCH, "MyDb");
        Assertions.assertTrue(c instanceof ConnectorMetaChangeEvent.DatabaseCreated);
        Assertions.assertEquals("mydb", c.database().orElseThrow());

        ConnectorMetaChangeEvent d = tr.dbDropped(2L, Instant.EPOCH, "OtherDb");
        Assertions.assertTrue(d instanceof ConnectorMetaChangeEvent.DatabaseDropped);
        Assertions.assertEquals("otherdb", d.database().orElseThrow());
    }

    @Test
    void loadFailedReturnsVendorEventWithErrorAttributes() {
        PaimonEventTranslator tr = new PaimonEventTranslator(CATALOG);
        ConnectorMetaChangeEvent.VendorEvent ev = (ConnectorMetaChangeEvent.VendorEvent)
                tr.loadFailed(99L, Instant.EPOCH, ID, new RuntimeException("boom"));
        Assertions.assertEquals("paimon", ev.vendor());
        Assertions.assertEquals("RuntimeException", ev.attributes().get("error"));
        Assertions.assertEquals("boom", ev.attributes().get("message"));
        Assertions.assertEquals("db1", ev.database().orElseThrow());
        Assertions.assertEquals("tbl1", ev.table().orElseThrow());
    }
}
