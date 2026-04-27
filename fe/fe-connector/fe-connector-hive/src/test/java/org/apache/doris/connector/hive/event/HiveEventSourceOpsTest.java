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
import org.apache.doris.connector.api.event.EventBatch;
import org.apache.doris.connector.api.event.EventCursor;
import org.apache.doris.connector.api.event.EventFilter;
import org.apache.doris.connector.api.event.EventSourceException;
import org.apache.doris.connector.api.event.TableIdentifier;
import org.apache.doris.connector.hive.event.HiveEventTranslator.AlterTableInfo;
import org.apache.doris.connector.hive.event.HiveEventTranslator.PartitionOp;
import org.apache.doris.connector.hive.event.HiveEventTranslator.PartitionPayloadDecoder;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsClientException;
import org.apache.doris.connector.hms.HmsNotificationEvent;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;

class HiveEventSourceOpsTest {

    private static final String CATALOG = "hive_cat";

    private static HmsNotificationEvent ne(long id, String type, String db, String tbl) {
        return new HmsNotificationEvent(id, 1700000000, type, db, tbl, "{}", "json");
    }

    private HiveEventSourceOps newOps(HmsClient client, PartitionPayloadDecoder decoder) {
        HiveEventTranslator t = new HiveEventTranslator(CATALOG, decoder);
        return new HiveEventSourceOps(client, CATALOG, t);
    }

    private HiveEventSourceOps newOps(HmsClient client) {
        return newOps(client, new EmptyDecoder());
    }

    @Test
    void initialCursorReturnsCurrentEventId() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Mockito.when(client.getCurrentNotificationEventId()).thenReturn(42L);
        Optional<EventCursor> cursor = newOps(client).initialCursor();
        Assertions.assertTrue(cursor.isPresent());
        Assertions.assertEquals(42L, ((HiveEventCursor) cursor.get()).getEventId());
    }

    @Test
    void initialCursorEmptyOnHmsFailure() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Mockito.when(client.getCurrentNotificationEventId())
                .thenThrow(new HmsClientException("boom"));
        Assertions.assertTrue(newOps(client).initialCursor().isEmpty());
    }

    @Test
    void pollWithEmptyResultReturnsEmptyBatch() throws Exception {
        HmsClient client = Mockito.mock(HmsClient.class);
        Mockito.when(client.getNextNotification(Mockito.eq(5L), Mockito.anyInt()))
                .thenReturn(Collections.emptyList());
        EventBatch batch = newOps(client).poll(
                new HiveEventCursor(5L), 100, Duration.ZERO, EventFilter.ALL);
        Assertions.assertEquals(0, batch.events().size());
        Assertions.assertEquals(5L, ((HiveEventCursor) batch.nextCursor()).getEventId());
        Assertions.assertFalse(batch.hasMore());
    }

    @Test
    void pollTranslatesSimpleEventTypes() throws Exception {
        HmsClient client = Mockito.mock(HmsClient.class);
        List<HmsNotificationEvent> raw = List.of(
                ne(11, "CREATE_DATABASE", "db", null),
                ne(12, "CREATE_TABLE", "db", "tbl"),
                ne(13, "INSERT", "db", "tbl"),
                ne(14, "DROP_TABLE", "db", "tbl"));
        Mockito.when(client.getNextNotification(Mockito.eq(10L), Mockito.anyInt())).thenReturn(raw);
        EventBatch batch = newOps(client).poll(
                new HiveEventCursor(10L), 100, Duration.ZERO, EventFilter.ALL);
        Assertions.assertEquals(4, batch.events().size());
        // ascending eventId enforced by EventBatch ctor; double-check
        long prev = -1;
        for (ConnectorMetaChangeEvent e : batch.events()) {
            Assertions.assertTrue(e.eventId() >= prev);
            prev = e.eventId();
        }
        Assertions.assertEquals(14L, ((HiveEventCursor) batch.nextCursor()).getEventId());
    }

    @Test
    void pollExpandsMultiplePartitionsToMultipleEvents() throws Exception {
        HmsClient client = Mockito.mock(HmsClient.class);
        Mockito.when(client.getNextNotification(Mockito.eq(0L), Mockito.anyInt()))
                .thenReturn(List.of(ne(1, "ADD_PARTITION", "db", "tbl")));
        List<LinkedHashMap<String, String>> specs = new ArrayList<>();
        LinkedHashMap<String, String> p1 = new LinkedHashMap<>();
        p1.put("dt", "a");
        LinkedHashMap<String, String> p2 = new LinkedHashMap<>();
        p2.put("dt", "b");
        specs.add(p1);
        specs.add(p2);
        EventBatch batch = newOps(client, new FixedDecoder(specs)).poll(
                new HiveEventCursor(0L), 100, Duration.ZERO, EventFilter.ALL);
        Assertions.assertEquals(2, batch.events().size());
        // both events share the same eventId, ascending invariant allows ties
        Assertions.assertEquals(1L, batch.events().get(0).eventId());
        Assertions.assertEquals(1L, batch.events().get(1).eventId());
        Assertions.assertTrue(batch.events().get(0) instanceof ConnectorMetaChangeEvent.PartitionAdded);
    }

    @Test
    void pollAdvancesCursorEvenWhenAllFilteredOut() throws Exception {
        HmsClient client = Mockito.mock(HmsClient.class);
        Mockito.when(client.getNextNotification(Mockito.eq(0L), Mockito.anyInt()))
                .thenReturn(List.of(ne(7, "CREATE_TABLE", "other_db", "tbl")));
        EventFilter filter = new EventFilter(Set.of("kept_db"), Set.of(), false);
        EventBatch batch = newOps(client).poll(
                new HiveEventCursor(0L), 100, Duration.ZERO, filter);
        Assertions.assertEquals(0, batch.events().size());
        Assertions.assertEquals(7L, ((HiveEventCursor) batch.nextCursor()).getEventId());
    }

    @Test
    void pollFilterByTableIdentifierKeepsMatch() throws Exception {
        HmsClient client = Mockito.mock(HmsClient.class);
        Mockito.when(client.getNextNotification(Mockito.eq(0L), Mockito.anyInt()))
                .thenReturn(List.of(
                        ne(1, "CREATE_TABLE", "db", "skip"),
                        ne(2, "CREATE_TABLE", "db", "keep")));
        EventFilter filter = new EventFilter(
                Collections.emptySet(),
                Set.of(new TableIdentifier("db", "keep")),
                false);
        EventBatch batch = newOps(client).poll(
                new HiveEventCursor(0L), 100, Duration.ZERO, filter);
        Assertions.assertEquals(1, batch.events().size());
        Assertions.assertEquals(2L, batch.events().get(0).eventId());
    }

    @Test
    void pollHasMoreWhenBatchEqualsCap() throws Exception {
        HmsClient client = Mockito.mock(HmsClient.class);
        List<HmsNotificationEvent> raw = new ArrayList<>();
        for (long i = 1; i <= 3; i++) {
            raw.add(ne(i, "CREATE_TABLE", "db", "t" + i));
        }
        Mockito.when(client.getNextNotification(Mockito.eq(0L), Mockito.eq(3))).thenReturn(raw);
        EventBatch batch = newOps(client).poll(
                new HiveEventCursor(0L), 3, Duration.ZERO, EventFilter.ALL);
        Assertions.assertEquals(3, batch.events().size());
        Assertions.assertTrue(batch.hasMore());
    }

    @Test
    void pollCapsAtMaxBatchSize() throws Exception {
        HmsClient client = Mockito.mock(HmsClient.class);
        Mockito.when(client.getNextNotification(Mockito.anyLong(), Mockito.eq(10_000)))
                .thenReturn(Collections.emptyList());
        newOps(client).poll(new HiveEventCursor(0L), 50_000, Duration.ZERO, EventFilter.ALL);
        Mockito.verify(client).getNextNotification(0L, 10_000);
    }

    @Test
    void pollHmsExceptionWrappedAsEventSourceException() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Mockito.when(client.getNextNotification(Mockito.anyLong(), Mockito.anyInt()))
                .thenThrow(new HmsClientException("network down"));
        EventSourceException ex = Assertions.assertThrows(EventSourceException.class,
                () -> newOps(client).poll(new HiveEventCursor(0L), 10, Duration.ZERO, EventFilter.ALL));
        Assertions.assertTrue(ex.getCause() instanceof HmsClientException);
    }

    @Test
    void pollRejectsAlienCursor() {
        HmsClient client = Mockito.mock(HmsClient.class);
        EventCursor alien = new EventCursor() {
            @Override
            public int compareTo(EventCursor o) {
                return 0;
            }

            @Override
            public String describe() {
                return "alien";
            }
        };
        Assertions.assertThrows(EventSourceException.class,
                () -> newOps(client).poll(alien, 10, Duration.ZERO, EventFilter.ALL));
    }

    @Test
    void serializeAndParseCursorRoundTrip() {
        HmsClient client = Mockito.mock(HmsClient.class);
        HiveEventSourceOps ops = newOps(client);
        byte[] blob = ops.serializeCursor(new HiveEventCursor(123456789L));
        Assertions.assertEquals(8, blob.length);
        EventCursor parsed = ops.parseCursor(blob);
        Assertions.assertEquals(123456789L, ((HiveEventCursor) parsed).getEventId());
    }

    @Test
    void parseCursorRejectsWrongLength() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> newOps(client).parseCursor(new byte[]{1, 2, 3}));
    }

    @Test
    void isSelfManagedReturnsFalse() {
        Assertions.assertFalse(newOps(Mockito.mock(HmsClient.class)).isSelfManaged());
    }

    @Test
    void cursorAdvancesEvenWithUnknownEventTypes() throws Exception {
        HmsClient client = Mockito.mock(HmsClient.class);
        Mockito.when(client.getNextNotification(Mockito.eq(0L), Mockito.anyInt()))
                .thenReturn(List.of(
                        ne(1, "OPEN_TXN", null, null),
                        ne(2, "COMMIT_TXN", null, null)));
        EventBatch batch = newOps(client).poll(
                new HiveEventCursor(0L), 100, Duration.ZERO, EventFilter.ALL);
        // Vendor events are still emitted; both eventIds present
        Assertions.assertEquals(2, batch.events().size());
        Assertions.assertEquals(2L, ((HiveEventCursor) batch.nextCursor()).getEventId());
    }

    /** Empty decoder; never produces partitions. */
    private static final class EmptyDecoder implements PartitionPayloadDecoder {
        @Override
        public List<LinkedHashMap<String, String>> decodePartitions(HmsNotificationEvent e, PartitionOp op) {
            return Collections.emptyList();
        }

        @Override
        public AlterTableInfo decodeAlterTable(HmsNotificationEvent e) {
            return null;
        }
    }

    /** Decoder returning a fixed partition list. */
    private static final class FixedDecoder implements PartitionPayloadDecoder {
        private final List<LinkedHashMap<String, String>> specs;

        FixedDecoder(List<LinkedHashMap<String, String>> specs) {
            this.specs = specs;
        }

        @Override
        public List<LinkedHashMap<String, String>> decodePartitions(HmsNotificationEvent e, PartitionOp op) {
            return specs;
        }

        @Override
        public AlterTableInfo decodeAlterTable(HmsNotificationEvent e) {
            return null;
        }
    }
}
