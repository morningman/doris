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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class HudiTimelineWatcherTest {

    private static final String CATALOG = "hudi_cat";
    private static final HudiTableRef T1 = new HudiTableRef("db1", "t1", "/wh/db1.db/t1");
    private static final HudiTableRef T2 = new HudiTableRef("db1", "t2", "/wh/db1.db/t2");

    /** In-memory publisher capturing every event. */
    private static final class CapturingPublisher implements java.util.function.Consumer<ConnectorMetaChangeEvent> {
        final List<ConnectorMetaChangeEvent> events = new ArrayList<>();

        @Override
        public void accept(ConnectorMetaChangeEvent e) {
            events.add(e);
        }
    }

    /** Mutable lister stubs. */
    private static final class StubTableLister implements HudiTableLister {
        List<HudiTableRef> tables = new ArrayList<>();
        RuntimeException failure;

        @Override
        public List<HudiTableRef> listTables() {
            if (failure != null) {
                throw failure;
            }
            return new ArrayList<>(tables);
        }
    }

    private static final class StubTimelineLister implements HudiTimelineLister {
        final Map<String, List<HudiInstant>> per = new HashMap<>();
        final Map<String, RuntimeException> failurePer = new HashMap<>();

        @Override
        public List<HudiInstant> listInstants(HudiTableRef ref) {
            RuntimeException f = failurePer.get(ref.key());
            if (f != null) {
                throw f;
            }
            return per.getOrDefault(ref.key(), Collections.emptyList());
        }

        void put(HudiTableRef ref, HudiInstant... insts) {
            per.put(ref.key(), new ArrayList<>(Arrays.asList(insts)));
        }
    }

    private CapturingPublisher pub;
    private StubTableLister tables;
    private StubTimelineLister timeline;
    private HudiTimelineWatcher watcher;

    private void setUp() {
        pub = new CapturingPublisher();
        tables = new StubTableLister();
        timeline = new StubTimelineLister();
        watcher = new HudiTimelineWatcher(CATALOG, tables, timeline, pub);
    }

    @Test
    void firstRunWithNoTablesProducesNoEventsButAdvancesTick() {
        setUp();
        watcher.run();
        Assertions.assertTrue(pub.events.isEmpty());
        Assertions.assertEquals(1L, watcher.currentCursor().tickId());
        Assertions.assertTrue(watcher.currentCursor().lastInstantPerTable().isEmpty());
    }

    @Test
    void firstRunDiscoversTableAndEmitsTableCreated() {
        setUp();
        tables.tables = new ArrayList<>(Collections.singletonList(T1));
        timeline.put(T1, HudiInstant.completed("20260422001", "commit"));
        watcher.run();
        // 1 TableCreated + 1 DataChanged
        Assertions.assertEquals(2, pub.events.size());
        Assertions.assertInstanceOf(ConnectorMetaChangeEvent.TableCreated.class, pub.events.get(0));
        Assertions.assertInstanceOf(ConnectorMetaChangeEvent.DataChanged.class, pub.events.get(1));
        // eventIds strictly ascending
        Assertions.assertTrue(pub.events.get(1).eventId() > pub.events.get(0).eventId());
    }

    @Test
    void seenInstantNotReEmittedOnSecondRun() {
        setUp();
        tables.tables = new ArrayList<>(Collections.singletonList(T1));
        timeline.put(T1, HudiInstant.completed("20260422001", "commit"));
        watcher.run();
        int before = pub.events.size();
        // re-run with identical timeline
        watcher.run();
        Assertions.assertEquals(before, pub.events.size());
        Assertions.assertEquals(2L, watcher.currentCursor().tickId());
    }

    @Test
    void newCommitOnSecondRunEmitsDataChanged() {
        setUp();
        tables.tables = new ArrayList<>(Collections.singletonList(T1));
        timeline.put(T1, HudiInstant.completed("20260422001", "commit"));
        watcher.run();
        int before = pub.events.size();
        timeline.put(T1,
                HudiInstant.completed("20260422001", "commit"),
                HudiInstant.completed("20260422002", "deltacommit"));
        watcher.run();
        Assertions.assertEquals(before + 1, pub.events.size());
        ConnectorMetaChangeEvent last = pub.events.get(pub.events.size() - 1);
        Assertions.assertInstanceOf(ConnectorMetaChangeEvent.DataChanged.class, last);
        Assertions.assertTrue(last.cause().contains("20260422002"));
    }

    @Test
    void tableDroppedEmitsTableDroppedAndPurgesState() {
        setUp();
        tables.tables = new ArrayList<>(Collections.singletonList(T1));
        timeline.put(T1, HudiInstant.completed("20260422001", "commit"));
        watcher.run();
        pub.events.clear();
        // T1 disappears
        tables.tables = new ArrayList<>();
        watcher.run();
        Assertions.assertEquals(1, pub.events.size());
        Assertions.assertInstanceOf(ConnectorMetaChangeEvent.TableDropped.class, pub.events.get(0));
        Assertions.assertFalse(watcher.tableStates().containsKey(T1.key()));
        // cursor's per-table map drops the entry
        Assertions.assertFalse(watcher.currentCursor().lastInstantPerTable().containsKey(T1.key()));
    }

    @Test
    void multipleNewInstantsAreEmittedInChronologicalOrder() {
        setUp();
        tables.tables = new ArrayList<>(Collections.singletonList(T1));
        // intentionally insert out of order; watcher should sort by ts
        timeline.put(T1,
                HudiInstant.completed("20260422002", "commit"),
                HudiInstant.completed("20260422001", "deltacommit"),
                HudiInstant.completed("20260422003", "clean"));
        watcher.run();
        // TableCreated + 3 instants. clean produces 2 events (DataChanged + Vendor)
        long earlier = -1L;
        int dcCount = 0;
        for (ConnectorMetaChangeEvent ev : pub.events) {
            Assertions.assertTrue(ev.eventId() >= earlier,
                    "events not ascending: " + ev + " after id " + earlier);
            earlier = ev.eventId();
            if (ev instanceof ConnectorMetaChangeEvent.DataChanged) {
                dcCount++;
            }
        }
        Assertions.assertEquals(3, dcCount);
    }

    @Test
    void tableListerFailureSkipsTickWithoutCrash() {
        setUp();
        tables.failure = new RuntimeException("hms down");
        watcher.run();
        Assertions.assertTrue(pub.events.isEmpty());
        // tickId still bumped so dispatcher persists progress
        Assertions.assertEquals(1L, watcher.currentCursor().tickId());
    }

    @Test
    void timelineListerFailureEmitsVendorEventAndKeepsCursor() {
        setUp();
        tables.tables = new ArrayList<>(Collections.singletonList(T1));
        timeline.failurePer.put(T1.key(), new RuntimeException("io"));
        watcher.run();
        // TableCreated + listFailed VendorEvent
        Assertions.assertEquals(2, pub.events.size());
        Assertions.assertInstanceOf(ConnectorMetaChangeEvent.VendorEvent.class, pub.events.get(1));
        ConnectorMetaChangeEvent.VendorEvent ve = (ConnectorMetaChangeEvent.VendorEvent) pub.events.get(1);
        Assertions.assertEquals("hudi.timeline.listFailed", ve.cause());
    }

    @Test
    void requestedInstantTrackedButNotEmitted() {
        setUp();
        tables.tables = new ArrayList<>(Collections.singletonList(T1));
        timeline.put(T1, HudiInstant.of("20260422001", "compaction", "REQUESTED"));
        watcher.run();
        // only TableCreated, no DataChanged for the requested instant
        Assertions.assertEquals(1, pub.events.size());
        Assertions.assertInstanceOf(ConnectorMetaChangeEvent.TableCreated.class, pub.events.get(0));
        // when the same instant later transitions to COMPLETED it must fire
        timeline.put(T1,
                HudiInstant.of("20260422001", "compaction", "REQUESTED"),
                HudiInstant.of("20260422001", "compaction", "COMPLETED"));
        watcher.run();
        // additional 2 events: DataChanged + VendorEvent for the compaction
        Assertions.assertEquals(3, pub.events.size());
    }

    @Test
    void cursorTracksLatestCompletedInstantPerTable() {
        setUp();
        tables.tables = new ArrayList<>(Arrays.asList(T1, T2));
        timeline.put(T1,
                HudiInstant.completed("20260422001", "commit"),
                HudiInstant.completed("20260422005", "deltacommit"));
        timeline.put(T2, HudiInstant.completed("20260422010", "commit"));
        watcher.run();
        Map<String, String> last = watcher.currentCursor().lastInstantPerTable();
        Assertions.assertEquals("20260422005", last.get(T1.key()));
        Assertions.assertEquals("20260422010", last.get(T2.key()));
    }

    @Test
    void schemaChangeDetectedAcrossRuns() {
        setUp();
        tables.tables = new ArrayList<>(Collections.singletonList(T1));
        timeline.put(T1,
                HudiInstant.completed("20260422001", "commit").withSchemaHash("h1"));
        watcher.run();
        pub.events.clear();
        // new commit with different schema hash
        timeline.put(T1,
                HudiInstant.completed("20260422001", "commit").withSchemaHash("h1"),
                HudiInstant.completed("20260422002", "commit").withSchemaHash("h2"));
        watcher.run();
        // should produce TableAltered + DataChanged
        Assertions.assertEquals(2, pub.events.size());
        Assertions.assertInstanceOf(ConnectorMetaChangeEvent.TableAltered.class, pub.events.get(0));
        Assertions.assertInstanceOf(ConnectorMetaChangeEvent.DataChanged.class, pub.events.get(1));
    }
}
