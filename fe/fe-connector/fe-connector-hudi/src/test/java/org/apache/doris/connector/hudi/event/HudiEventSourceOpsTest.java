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
import org.apache.doris.connector.api.event.EventBatch;
import org.apache.doris.connector.api.event.EventCursor;
import org.apache.doris.connector.api.event.EventFilter;
import org.apache.doris.connector.api.event.EventSourceException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

class HudiEventSourceOpsTest {

    private static final String CATALOG = "hudi_cat";

    private HudiEventSourceOps build(HudiTableLister t, HudiTimelineLister tl,
                                     Consumer<ConnectorMetaChangeEvent> pub) {
        return new HudiEventSourceOps(CATALOG, t, tl, pub);
    }

    private HudiEventSourceOps buildEmpty() {
        return build(Collections::emptyList, ref -> Collections.emptyList(), ev -> { });
    }

    @Test
    void isSelfManagedReturnsTrue() {
        Assertions.assertTrue(buildEmpty().isSelfManaged());
    }

    @Test
    void initialCursorReturnsEmptyHudiCursor() {
        EventCursor c = buildEmpty().initialCursor().orElseThrow();
        Assertions.assertInstanceOf(HudiEventCursor.class, c);
        HudiEventCursor hc = (HudiEventCursor) c;
        Assertions.assertEquals(0L, hc.tickId());
        Assertions.assertTrue(hc.lastInstantPerTable().isEmpty());
    }

    @Test
    void pollAlwaysReturnsEmptyBatchCarryingCursor() throws EventSourceException {
        HudiEventSourceOps ops = buildEmpty();
        HudiEventCursor cursor = new HudiEventCursor(7L, Collections.emptyMap());
        EventBatch batch = ops.poll(cursor, 100, Duration.ZERO, EventFilter.ALL);
        Assertions.assertEquals(0, batch.events().size());
        Assertions.assertSame(cursor, batch.nextCursor());
        Assertions.assertFalse(batch.hasMore());
    }

    @Test
    void pollWithNullCursorReturnsCurrentWatcherCursor() throws EventSourceException {
        HudiEventSourceOps ops = buildEmpty();
        EventBatch batch = ops.poll(null, 0, Duration.ZERO, EventFilter.ALL);
        Assertions.assertNotNull(batch.nextCursor());
        Assertions.assertInstanceOf(HudiEventCursor.class, batch.nextCursor());
    }

    @Test
    void pollWithWrongCursorTypeThrows() {
        HudiEventSourceOps ops = buildEmpty();
        EventCursor wrong = new EventCursor() {
            @Override
            public int compareTo(EventCursor o) {
                return 0;
            }

            @Override
            public String describe() {
                return "wrong";
            }
        };
        Assertions.assertThrows(EventSourceException.class,
                () -> ops.poll(wrong, 1, Duration.ZERO, EventFilter.ALL));
    }

    @Test
    void cursorRoundTripsThroughSerialize() {
        HudiEventSourceOps ops = buildEmpty();
        Map<String, String> tables = new TreeMap<>();
        tables.put("db1\u0001t1", "20260422001");
        tables.put("db2\u0001t9", "20260422010");
        HudiEventCursor original = new HudiEventCursor(42L, tables);
        byte[] blob = ops.serializeCursor(original);
        Assertions.assertEquals((byte) 0xCE, blob[0]);
        Assertions.assertEquals((byte) 0x01, blob[1]);
        EventCursor parsed = ops.parseCursor(blob);
        Assertions.assertEquals(original, parsed);
    }

    @Test
    void parseCursorRejectsBadMagic() {
        HudiEventSourceOps ops = buildEmpty();
        byte[] bad = new byte[] {(byte) 0xCC, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        Assertions.assertThrows(IllegalArgumentException.class, () -> ops.parseCursor(bad));
    }

    @Test
    void serializeCursorRejectsWrongType() {
        HudiEventSourceOps ops = buildEmpty();
        EventCursor wrong = new EventCursor() {
            @Override
            public int compareTo(EventCursor o) {
                return 0;
            }

            @Override
            public String describe() {
                return "wrong";
            }
        };
        Assertions.assertThrows(IllegalArgumentException.class, () -> ops.serializeCursor(wrong));
    }

    @Test
    void getSelfManagedTaskReturnsRunnableAndDrivesPublisher() {
        List<ConnectorMetaChangeEvent> sink = new ArrayList<>();
        HudiTableRef ref = new HudiTableRef("db1", "t1", "/wh/db1.db/t1");
        HudiTableLister tableLister = () -> Collections.singletonList(ref);
        HudiTimelineLister timelineLister = r -> Collections.singletonList(
                HudiInstant.completed("20260422001", "commit"));
        HudiEventSourceOps ops = build(tableLister, timelineLister, sink::add);

        Runnable task = ops.getSelfManagedTask();
        Assertions.assertNotNull(task);
        // dispatcher (M2-12) will normally drive this; invoking directly here.
        task.run();
        // expect TableCreated + DataChanged
        Assertions.assertEquals(2, sink.size());
        Assertions.assertInstanceOf(ConnectorMetaChangeEvent.TableCreated.class, sink.get(0));
        Assertions.assertInstanceOf(ConnectorMetaChangeEvent.DataChanged.class, sink.get(1));
    }

    @Test
    void cursorAdvancesAfterRunningSelfManagedTask() {
        HudiTableLister tableLister = Collections::emptyList;
        HudiTimelineLister timelineLister = r -> Collections.emptyList();
        HudiEventSourceOps ops = build(tableLister, timelineLister, ev -> { });
        long before = ((HudiEventCursor) ops.initialCursor().orElseThrow()).tickId();
        ops.getSelfManagedTask().run();
        long after = ((HudiEventCursor) ops.initialCursor().orElseThrow()).tickId();
        Assertions.assertEquals(before + 1, after);
    }

    @Test
    void shutdownSelfManagedIsNoOp() {
        HudiEventSourceOps ops = buildEmpty();
        // should not throw and remains usable afterwards
        ops.shutdownSelfManaged(Duration.ofSeconds(1));
        Assertions.assertTrue(ops.initialCursor().isPresent());
    }

    @Test
    void constructorRejectsNulls() {
        HudiTableLister tl = Collections::emptyList;
        HudiTimelineLister timeline = r -> Collections.emptyList();
        Consumer<ConnectorMetaChangeEvent> pub = ev -> { };
        Assertions.assertThrows(NullPointerException.class,
                () -> new HudiEventSourceOps(null, tl, timeline, pub));
        Assertions.assertThrows(NullPointerException.class,
                () -> new HudiEventSourceOps(CATALOG, null, timeline, pub));
        Assertions.assertThrows(NullPointerException.class,
                () -> new HudiEventSourceOps(CATALOG, tl, null, pub));
        Assertions.assertThrows(NullPointerException.class,
                () -> new HudiEventSourceOps(CATALOG, tl, timeline, null));
    }

    @Test
    void cursorMagicByteIsUniqueAcrossPlugins() {
        HudiEventSourceOps ops = buildEmpty();
        byte[] blob = ops.serializeCursor(new HudiEventCursor(0L, Collections.emptyMap()));
        // 0xCE distinguishes hudi from iceberg (0xCC) / paimon (0xCD)
        Assertions.assertEquals((byte) 0xCE, blob[0]);
        Assertions.assertNotEquals((byte) 0xCC, blob[0]);
        Assertions.assertNotEquals((byte) 0xCD, blob[0]);
    }
}
