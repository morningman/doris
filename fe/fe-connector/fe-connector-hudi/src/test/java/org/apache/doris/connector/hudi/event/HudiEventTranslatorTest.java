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

package org.apache.doris.connector.hudi.event;

import org.apache.doris.connector.api.event.ConnectorMetaChangeEvent;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

class HudiEventTranslatorTest {

    private static final String CATALOG = "hudi_cat";
    private static final HudiTableRef REF = new HudiTableRef("db1", "t1", "/warehouse/db1.db/t1");
    private final HudiEventTranslator translator = new HudiEventTranslator(CATALOG);
    private final Instant now = Instant.parse("2026-04-22T00:00:00Z");

    @Test
    void commitProducesDataChanged() {
        List<ConnectorMetaChangeEvent> evs = translator.translate(
                100L, now, REF, Optional.empty(), HudiInstant.completed("20260422001", "commit"));
        Assertions.assertEquals(1, evs.size());
        Assertions.assertInstanceOf(ConnectorMetaChangeEvent.DataChanged.class, evs.get(0));
        ConnectorMetaChangeEvent.DataChanged dc = (ConnectorMetaChangeEvent.DataChanged) evs.get(0);
        Assertions.assertEquals(100L, dc.eventId());
        Assertions.assertEquals(CATALOG, dc.catalog());
        Assertions.assertEquals("db1", dc.db());
        Assertions.assertEquals("t1", dc.tbl());
        Assertions.assertTrue(dc.cause().startsWith("hudi.commit:"));
        Assertions.assertTrue(dc.snapshotId().isEmpty());
    }

    @Test
    void deltaCommitProducesDataChanged() {
        List<ConnectorMetaChangeEvent> evs = translator.translate(
                1L, now, REF, Optional.empty(),
                HudiInstant.completed("20260422002", "deltacommit"));
        Assertions.assertEquals(1, evs.size());
        Assertions.assertInstanceOf(ConnectorMetaChangeEvent.DataChanged.class, evs.get(0));
        Assertions.assertTrue(evs.get(0).cause().startsWith("hudi.delta_commit:"));
    }

    @Test
    void replaceCommitProducesDataChanged() {
        List<ConnectorMetaChangeEvent> evs = translator.translate(
                1L, now, REF, Optional.empty(),
                HudiInstant.completed("20260422003", "replacecommit"));
        Assertions.assertEquals(1, evs.size());
        Assertions.assertInstanceOf(ConnectorMetaChangeEvent.DataChanged.class, evs.get(0));
        Assertions.assertTrue(evs.get(0).cause().startsWith("hudi.replace_commit:"));
    }

    @Test
    void cleanProducesDataChangedPlusVendor() {
        List<ConnectorMetaChangeEvent> evs = translator.translate(
                1L, now, REF, Optional.empty(),
                HudiInstant.completed("20260422004", "clean"));
        Assertions.assertEquals(2, evs.size());
        Assertions.assertInstanceOf(ConnectorMetaChangeEvent.DataChanged.class, evs.get(0));
        Assertions.assertInstanceOf(ConnectorMetaChangeEvent.VendorEvent.class, evs.get(1));
        ConnectorMetaChangeEvent.VendorEvent ve = (ConnectorMetaChangeEvent.VendorEvent) evs.get(1);
        Assertions.assertEquals("hudi", ve.vendor());
        Assertions.assertEquals("clean", ve.attributes().get("kind"));
        Assertions.assertEquals("20260422004", ve.attributes().get("instant"));
        // both events must share the same eventId
        Assertions.assertEquals(evs.get(0).eventId(), evs.get(1).eventId());
    }

    @Test
    void rollbackProducesDataChangedPlusVendor() {
        List<ConnectorMetaChangeEvent> evs = translator.translate(
                1L, now, REF, Optional.empty(),
                HudiInstant.completed("20260422005", "rollback"));
        Assertions.assertEquals(2, evs.size());
        ConnectorMetaChangeEvent.VendorEvent ve = (ConnectorMetaChangeEvent.VendorEvent) evs.get(1);
        Assertions.assertEquals("rollback", ve.attributes().get("kind"));
    }

    @Test
    void compactionProducesDataChangedPlusVendor() {
        List<ConnectorMetaChangeEvent> evs = translator.translate(
                1L, now, REF, Optional.empty(),
                HudiInstant.completed("20260422006", "compaction"));
        Assertions.assertEquals(2, evs.size());
        ConnectorMetaChangeEvent.VendorEvent ve = (ConnectorMetaChangeEvent.VendorEvent) evs.get(1);
        Assertions.assertEquals("compaction", ve.attributes().get("kind"));
        // raw action preserved verbatim
        Assertions.assertEquals("compaction", ve.attributes().get("rawAction"));
    }

    @Test
    void logCompactionMapsToCompaction() {
        List<ConnectorMetaChangeEvent> evs = translator.translate(
                1L, now, REF, Optional.empty(),
                HudiInstant.completed("20260422007", "logcompaction"));
        Assertions.assertEquals(2, evs.size());
        ConnectorMetaChangeEvent.VendorEvent ve = (ConnectorMetaChangeEvent.VendorEvent) evs.get(1);
        Assertions.assertEquals("compaction", ve.attributes().get("kind"));
        Assertions.assertEquals("logcompaction", ve.attributes().get("rawAction"));
    }

    @Test
    void savepointProducesVendorOnly() {
        List<ConnectorMetaChangeEvent> evs = translator.translate(
                1L, now, REF, Optional.empty(),
                HudiInstant.completed("20260422008", "savepoint"));
        Assertions.assertEquals(1, evs.size());
        Assertions.assertInstanceOf(ConnectorMetaChangeEvent.VendorEvent.class, evs.get(0));
        ConnectorMetaChangeEvent.VendorEvent ve = (ConnectorMetaChangeEvent.VendorEvent) evs.get(0);
        Assertions.assertEquals("savepoint", ve.attributes().get("kind"));
        Assertions.assertEquals("hudi.savepoint", ve.cause());
    }

    @Test
    void unknownActionProducesVendorOnly() {
        List<ConnectorMetaChangeEvent> evs = translator.translate(
                1L, now, REF, Optional.empty(),
                HudiInstant.completed("20260422009", "indexing"));
        Assertions.assertEquals(1, evs.size());
        ConnectorMetaChangeEvent.VendorEvent ve = (ConnectorMetaChangeEvent.VendorEvent) evs.get(0);
        Assertions.assertEquals("hudi.unknown.action", ve.cause());
        Assertions.assertEquals("indexing", ve.attributes().get("rawAction"));
    }

    @Test
    void schemaChangeOnCommitEmitsTableAlteredFirst() {
        HudiInstant inst = HudiInstant.completed("20260422010", "commit").withSchemaHash("hash2");
        List<ConnectorMetaChangeEvent> evs =
                translator.translate(1L, now, REF, Optional.of("hash1"), inst);
        Assertions.assertEquals(2, evs.size());
        // TableAltered emitted before DataChanged for the same table /
        // same eventId; both ride the same eventId slot which is allowed
        // by EventBatch ascending invariant.
        Assertions.assertInstanceOf(ConnectorMetaChangeEvent.TableAltered.class, evs.get(0));
        Assertions.assertInstanceOf(ConnectorMetaChangeEvent.DataChanged.class, evs.get(1));
        Assertions.assertTrue(evs.get(0).cause().contains("hash1->hash2"));
    }

    @Test
    void firstObservedSchemaCountsAsAlter() {
        HudiInstant inst = HudiInstant.completed("20260422011", "commit").withSchemaHash("hash1");
        List<ConnectorMetaChangeEvent> evs =
                translator.translate(1L, now, REF, Optional.empty(), inst);
        Assertions.assertEquals(2, evs.size());
        Assertions.assertInstanceOf(ConnectorMetaChangeEvent.TableAltered.class, evs.get(0));
        Assertions.assertTrue(evs.get(0).cause().contains("<none>->hash1"));
    }

    @Test
    void unchangedSchemaProducesOnlyDataChanged() {
        HudiInstant inst = HudiInstant.completed("20260422012", "commit").withSchemaHash("hashA");
        List<ConnectorMetaChangeEvent> evs =
                translator.translate(1L, now, REF, Optional.of("hashA"), inst);
        Assertions.assertEquals(1, evs.size());
        Assertions.assertInstanceOf(ConnectorMetaChangeEvent.DataChanged.class, evs.get(0));
    }

    @Test
    void requestedAndInflightStatesProduceNoEvents() {
        HudiInstant requested = HudiInstant.of("20260422013", "commit", "REQUESTED");
        HudiInstant inflight = HudiInstant.of("20260422013", "commit", "INFLIGHT");
        Assertions.assertTrue(translator.translate(
                1L, now, REF, Optional.empty(), requested).isEmpty());
        Assertions.assertTrue(translator.translate(
                1L, now, REF, Optional.empty(), inflight).isEmpty());
    }

    @Test
    void tableCreatedAndDroppedHelpers() {
        ConnectorMetaChangeEvent c = translator.tableCreated(7L, now, REF);
        Assertions.assertInstanceOf(ConnectorMetaChangeEvent.TableCreated.class, c);
        Assertions.assertEquals(7L, c.eventId());
        Assertions.assertEquals("hudi.table.created", c.cause());
        ConnectorMetaChangeEvent d = translator.tableDropped(8L, now, "db1", "tx");
        Assertions.assertInstanceOf(ConnectorMetaChangeEvent.TableDropped.class, d);
        Assertions.assertEquals("hudi.table.dropped", d.cause());
    }

    @Test
    void listFailedHelperEmitsVendorEvent() {
        ConnectorMetaChangeEvent e = translator.listFailed(
                9L, now, REF, new RuntimeException("boom"));
        Assertions.assertInstanceOf(ConnectorMetaChangeEvent.VendorEvent.class, e);
        ConnectorMetaChangeEvent.VendorEvent ve = (ConnectorMetaChangeEvent.VendorEvent) e;
        Assertions.assertEquals("hudi.timeline.listFailed", ve.cause());
        Assertions.assertEquals("boom", ve.attributes().get("message"));
        Assertions.assertEquals("RuntimeException", ve.attributes().get("error"));
    }
}
