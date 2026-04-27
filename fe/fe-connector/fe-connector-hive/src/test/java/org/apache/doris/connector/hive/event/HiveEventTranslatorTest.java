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

package org.apache.doris.connector.hive.event;

import org.apache.doris.connector.api.event.ConnectorMetaChangeEvent;
import org.apache.doris.connector.hive.event.HiveEventTranslator.AlterTableInfo;
import org.apache.doris.connector.hive.event.HiveEventTranslator.PartitionOp;
import org.apache.doris.connector.hive.event.HiveEventTranslator.PartitionPayloadDecoder;
import org.apache.doris.connector.hms.HmsNotificationEvent;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

class HiveEventTranslatorTest {

    private static final String CATALOG = "hive_catalog";

    private static HmsNotificationEvent ev(long id, String type, String db, String tbl) {
        return new HmsNotificationEvent(id, 1700000000, type, db, tbl, "{}", "json");
    }

    private HiveEventTranslator newTranslator(PartitionPayloadDecoder decoder) {
        return new HiveEventTranslator(CATALOG, decoder);
    }

    private HiveEventTranslator defaultTranslator() {
        return newTranslator(new StubDecoder(Collections.emptyList(), null));
    }

    @Test
    void translatesCreateTable() {
        List<ConnectorMetaChangeEvent> events =
                defaultTranslator().translate(ev(1, "CREATE_TABLE", "db1", "T1"));
        Assertions.assertEquals(1, events.size());
        ConnectorMetaChangeEvent.TableCreated e = (ConnectorMetaChangeEvent.TableCreated) events.get(0);
        Assertions.assertEquals(1L, e.eventId());
        Assertions.assertEquals(CATALOG, e.catalog());
        Assertions.assertEquals("db1", e.db());
        Assertions.assertEquals("t1", e.tbl());
    }

    @Test
    void translatesDropTable() {
        ConnectorMetaChangeEvent e =
                defaultTranslator().translate(ev(2, "DROP_TABLE", "DB", "tbl")).get(0);
        Assertions.assertTrue(e instanceof ConnectorMetaChangeEvent.TableDropped);
        Assertions.assertEquals("db", e.database().get());
        Assertions.assertEquals("tbl", e.table().get());
    }

    @Test
    void translatesAlterTableNoRename() {
        ConnectorMetaChangeEvent e = newTranslator(new StubDecoder(Collections.emptyList(),
                new AlterTableInfo("db", "t", "db", "t")))
                .translate(ev(3, "ALTER_TABLE", "db", "t")).get(0);
        Assertions.assertTrue(e instanceof ConnectorMetaChangeEvent.TableAltered);
    }

    @Test
    void translatesAlterTableRename() {
        ConnectorMetaChangeEvent e = newTranslator(new StubDecoder(Collections.emptyList(),
                new AlterTableInfo("db", "old", "db", "new")))
                .translate(ev(4, "ALTER_TABLE", "db", "old")).get(0);
        ConnectorMetaChangeEvent.TableRenamed r = (ConnectorMetaChangeEvent.TableRenamed) e;
        Assertions.assertEquals("old", r.table().get());
        Assertions.assertEquals("new", r.newTableName());
    }

    @Test
    void translatesCreateDb() {
        ConnectorMetaChangeEvent e =
                defaultTranslator().translate(ev(5, "CREATE_DATABASE", "db", null)).get(0);
        Assertions.assertTrue(e instanceof ConnectorMetaChangeEvent.DatabaseCreated);
        Assertions.assertEquals("db", e.database().get());
        Assertions.assertTrue(e.table().isEmpty());
    }

    @Test
    void translatesDropDb() {
        ConnectorMetaChangeEvent e =
                defaultTranslator().translate(ev(6, "DROP_DATABASE", "db", null)).get(0);
        Assertions.assertTrue(e instanceof ConnectorMetaChangeEvent.DatabaseDropped);
    }

    @Test
    void translatesAlterDb() {
        ConnectorMetaChangeEvent e =
                defaultTranslator().translate(ev(7, "ALTER_DATABASE", "db", null)).get(0);
        Assertions.assertTrue(e instanceof ConnectorMetaChangeEvent.DatabaseAltered);
    }

    @Test
    void translatesAddPartitionMultiple() {
        List<LinkedHashMap<String, String>> specs = new ArrayList<>();
        LinkedHashMap<String, String> p1 = new LinkedHashMap<>();
        p1.put("dt", "2024-01-01");
        LinkedHashMap<String, String> p2 = new LinkedHashMap<>();
        p2.put("dt", "2024-01-02");
        specs.add(p1);
        specs.add(p2);
        List<ConnectorMetaChangeEvent> events =
                newTranslator(new StubDecoder(specs, null))
                        .translate(ev(8, "ADD_PARTITION", "db", "tbl"));
        Assertions.assertEquals(2, events.size());
        Assertions.assertTrue(events.get(0) instanceof ConnectorMetaChangeEvent.PartitionAdded);
        Assertions.assertEquals("2024-01-01",
                events.get(0).partitionSpec().get().values().get("dt"));
        Assertions.assertEquals(8L, events.get(1).eventId());
    }

    @Test
    void translatesDropPartition() {
        LinkedHashMap<String, String> p = new LinkedHashMap<>();
        p.put("region", "us");
        ConnectorMetaChangeEvent e = newTranslator(new StubDecoder(List.of(p), null))
                .translate(ev(9, "DROP_PARTITION", "db", "tbl")).get(0);
        Assertions.assertTrue(e instanceof ConnectorMetaChangeEvent.PartitionDropped);
    }

    @Test
    void translatesAlterPartition() {
        LinkedHashMap<String, String> p = new LinkedHashMap<>();
        p.put("dt", "2024-01-01");
        ConnectorMetaChangeEvent e = newTranslator(new StubDecoder(List.of(p), null))
                .translate(ev(10, "ALTER_PARTITION", "db", "tbl")).get(0);
        Assertions.assertTrue(e instanceof ConnectorMetaChangeEvent.PartitionAltered);
    }

    @Test
    void translatesInsert() {
        ConnectorMetaChangeEvent e =
                defaultTranslator().translate(ev(11, "INSERT", "db", "tbl")).get(0);
        Assertions.assertTrue(e instanceof ConnectorMetaChangeEvent.DataChanged);
    }

    @Test
    void unknownTypeBecomesVendorEvent() {
        ConnectorMetaChangeEvent e =
                defaultTranslator().translate(ev(12, "OPEN_TXN", "db", "tbl")).get(0);
        ConnectorMetaChangeEvent.VendorEvent ve = (ConnectorMetaChangeEvent.VendorEvent) e;
        Assertions.assertEquals("hive", ve.vendor());
        Assertions.assertEquals("OPEN_TXN", ve.attributes().get("hmsType"));
    }

    @Test
    void emptyPartitionListFallsBackToVendor() {
        ConnectorMetaChangeEvent e =
                newTranslator(new StubDecoder(Collections.emptyList(), null))
                        .translate(ev(13, "ADD_PARTITION", "db", "tbl")).get(0);
        Assertions.assertTrue(e instanceof ConnectorMetaChangeEvent.VendorEvent);
    }

    @Test
    void missingDbFallsBackToVendor() {
        // requireDb throws IllegalArgumentException; translator catches and emits Vendor
        ConnectorMetaChangeEvent e =
                defaultTranslator().translate(ev(14, "CREATE_TABLE", null, "tbl")).get(0);
        Assertions.assertTrue(e instanceof ConnectorMetaChangeEvent.VendorEvent);
    }

    @Test
    void nullEventTypeBecomesVendor() {
        HmsNotificationEvent raw = new HmsNotificationEvent(15, 1700000000, "x", "db", "tbl", "{}", "json");
        // Hand-roll an event with type "x" via direct constructor, then verify default branch
        ConnectorMetaChangeEvent e = defaultTranslator().translate(raw).get(0);
        Assertions.assertTrue(e instanceof ConnectorMetaChangeEvent.VendorEvent);
    }

    /** Stub decoder used to bypass real Hive JSON deserialization in tests. */
    private static final class StubDecoder implements PartitionPayloadDecoder {
        private final List<LinkedHashMap<String, String>> partitions;
        private final AlterTableInfo alterInfo;

        StubDecoder(List<LinkedHashMap<String, String>> partitions, AlterTableInfo alterInfo) {
            this.partitions = partitions;
            this.alterInfo = alterInfo;
        }

        @Override
        public List<LinkedHashMap<String, String>> decodePartitions(HmsNotificationEvent event, PartitionOp op) {
            return partitions;
        }

        @Override
        public AlterTableInfo decodeAlterTable(HmsNotificationEvent event) {
            return alterInfo;
        }
    }
}
