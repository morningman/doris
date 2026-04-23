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

package org.apache.doris.connector.api.event;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

class ConnectorMetaChangeEventTest {

    private static final Instant NOW = Instant.parse("2024-01-01T00:00:00Z");
    private static final String CAT = "cat";

    private static PartitionSpec spec() {
        LinkedHashMap<String, String> v = new LinkedHashMap<>();
        v.put("p", "1");
        return new PartitionSpec(v);
    }

    @Test
    void databaseEventsExposeDb() {
        ConnectorMetaChangeEvent e = new ConnectorMetaChangeEvent.DatabaseCreated(1, NOW, CAT, "d", "test");
        Assertions.assertEquals(Optional.of("d"), e.database());
        Assertions.assertEquals(Optional.empty(), e.table());
        Assertions.assertEquals(Optional.empty(), e.partitionSpec());
        Assertions.assertEquals(1, e.eventId());
        Assertions.assertEquals(CAT, e.catalog());

        ConnectorMetaChangeEvent dropped = new ConnectorMetaChangeEvent.DatabaseDropped(2, NOW, CAT, "d", "x");
        ConnectorMetaChangeEvent altered = new ConnectorMetaChangeEvent.DatabaseAltered(3, NOW, CAT, "d", "x");
        Assertions.assertEquals(Optional.of("d"), dropped.database());
        Assertions.assertEquals(Optional.of("d"), altered.database());
    }

    @Test
    void tableEventsExposeTable() {
        ConnectorMetaChangeEvent c = new ConnectorMetaChangeEvent.TableCreated(1, NOW, CAT, "d", "t", "x");
        ConnectorMetaChangeEvent dr = new ConnectorMetaChangeEvent.TableDropped(2, NOW, CAT, "d", "t", "x");
        ConnectorMetaChangeEvent al = new ConnectorMetaChangeEvent.TableAltered(3, NOW, CAT, "d", "t", "x");
        for (ConnectorMetaChangeEvent e : new ConnectorMetaChangeEvent[] {c, dr, al}) {
            Assertions.assertEquals(Optional.of("d"), e.database());
            Assertions.assertEquals(Optional.of("t"), e.table());
            Assertions.assertEquals(Optional.empty(), e.partitionSpec());
        }
    }

    @Test
    void tableRenamedExposesOldNameAsTable() {
        ConnectorMetaChangeEvent.TableRenamed e =
                new ConnectorMetaChangeEvent.TableRenamed(1, NOW, CAT, "d", "old", "new", "rn");
        Assertions.assertEquals(Optional.of("old"), e.table());
        Assertions.assertEquals("new", e.newTableName());
    }

    @Test
    void partitionEventsCarrySpec() {
        PartitionSpec ps = spec();
        ConnectorMetaChangeEvent a = new ConnectorMetaChangeEvent.PartitionAdded(1, NOW, CAT, "d", "t", ps, "x");
        ConnectorMetaChangeEvent dr = new ConnectorMetaChangeEvent.PartitionDropped(2, NOW, CAT, "d", "t", ps, "x");
        ConnectorMetaChangeEvent al = new ConnectorMetaChangeEvent.PartitionAltered(3, NOW, CAT, "d", "t", ps, "x");
        for (ConnectorMetaChangeEvent e : new ConnectorMetaChangeEvent[] {a, dr, al}) {
            Assertions.assertEquals(Optional.of(ps), e.partitionSpec());
            Assertions.assertEquals(Optional.of("t"), e.table());
        }
    }

    @Test
    void dataChangedExposesSnapshotAndSpec() {
        PartitionSpec ps = spec();
        ConnectorMetaChangeEvent.DataChanged e = new ConnectorMetaChangeEvent.DataChanged(
                1, NOW, CAT, "d", "t", Optional.of(42L), Optional.of(ps), "x");
        Assertions.assertEquals(Optional.of(ps), e.partitionSpec());
        Assertions.assertEquals(Optional.of(42L), e.snapshotId());

        ConnectorMetaChangeEvent.DataChanged none = new ConnectorMetaChangeEvent.DataChanged(
                2, NOW, CAT, "d", "t", Optional.empty(), Optional.empty(), "x");
        Assertions.assertEquals(Optional.empty(), none.snapshotId());
        Assertions.assertEquals(Optional.empty(), none.partitionSpec());
    }

    @Test
    void refChangedFields() {
        ConnectorMetaChangeEvent.RefChanged e = new ConnectorMetaChangeEvent.RefChanged(
                1, NOW, CAT, "d", "t", "main", ConnectorMetaChangeEvent.RefKind.BRANCH, Optional.of(7L), "x");
        Assertions.assertEquals("main", e.refName());
        Assertions.assertEquals(ConnectorMetaChangeEvent.RefKind.BRANCH, e.kind());
        Assertions.assertEquals(Optional.of(7L), e.snapshotId());
        Assertions.assertEquals(Optional.empty(), e.partitionSpec());

        ConnectorMetaChangeEvent.RefChanged noSnap = new ConnectorMetaChangeEvent.RefChanged(
                2, NOW, CAT, "d", "t", "v1", ConnectorMetaChangeEvent.RefKind.TAG, Optional.empty(), "x");
        Assertions.assertEquals(Optional.empty(), noSnap.snapshotId());
        Assertions.assertEquals(ConnectorMetaChangeEvent.RefKind.TAG, noSnap.kind());
    }

    @Test
    void vendorEventCopiesAttributes() {
        Map<String, String> in = new LinkedHashMap<>();
        in.put("k", "v");
        ConnectorMetaChangeEvent.VendorEvent e = new ConnectorMetaChangeEvent.VendorEvent(
                1, NOW, CAT, Optional.of("d"), Optional.of("t"), "vendor", in, "x");
        in.put("k2", "v2");
        Assertions.assertEquals(1, e.attributes().size());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> e.attributes().put("x", "y"));
        Assertions.assertEquals(Optional.of("d"), e.database());
        Assertions.assertEquals(Optional.of("t"), e.table());
        Assertions.assertEquals(Optional.empty(), e.partitionSpec());
    }

    @Test
    void refKindHasThreeValues() {
        Assertions.assertEquals(3, ConnectorMetaChangeEvent.RefKind.values().length);
    }

    @Test
    void requiredArgsRejected() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorMetaChangeEvent.DatabaseCreated(1, null, CAT, "d", "x"));
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorMetaChangeEvent.TableCreated(1, NOW, CAT, "d", null, "x"));
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorMetaChangeEvent.PartitionAdded(1, NOW, CAT, "d", "t", null, "x"));
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorMetaChangeEvent.RefChanged(
                        1, NOW, CAT, "d", "t", "r", null, Optional.empty(), "x"));
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorMetaChangeEvent.VendorEvent(
                        1, NOW, CAT, Optional.of("d"), Optional.of("t"), "v", null, "x"));
    }

    @Test
    void allThirteenPermitsAreDistinct() {
        ConnectorMetaChangeEvent[] all = new ConnectorMetaChangeEvent[] {
                new ConnectorMetaChangeEvent.DatabaseCreated(1, NOW, CAT, "d", "x"),
                new ConnectorMetaChangeEvent.DatabaseDropped(2, NOW, CAT, "d", "x"),
                new ConnectorMetaChangeEvent.DatabaseAltered(3, NOW, CAT, "d", "x"),
                new ConnectorMetaChangeEvent.TableCreated(4, NOW, CAT, "d", "t", "x"),
                new ConnectorMetaChangeEvent.TableDropped(5, NOW, CAT, "d", "t", "x"),
                new ConnectorMetaChangeEvent.TableAltered(6, NOW, CAT, "d", "t", "x"),
                new ConnectorMetaChangeEvent.TableRenamed(7, NOW, CAT, "d", "o", "n", "x"),
                new ConnectorMetaChangeEvent.PartitionAdded(8, NOW, CAT, "d", "t", spec(), "x"),
                new ConnectorMetaChangeEvent.PartitionDropped(9, NOW, CAT, "d", "t", spec(), "x"),
                new ConnectorMetaChangeEvent.PartitionAltered(10, NOW, CAT, "d", "t", spec(), "x"),
                new ConnectorMetaChangeEvent.DataChanged(11, NOW, CAT, "d", "t",
                        Optional.empty(), Optional.empty(), "x"),
                new ConnectorMetaChangeEvent.RefChanged(12, NOW, CAT, "d", "t", "main",
                        ConnectorMetaChangeEvent.RefKind.BRANCH, Optional.empty(), "x"),
                new ConnectorMetaChangeEvent.VendorEvent(13, NOW, CAT, Optional.empty(), Optional.empty(),
                        "v", new LinkedHashMap<>(), "x"),
        };
        Assertions.assertEquals(13, all.length);
        for (int i = 0; i < all.length; i++) {
            for (int j = i + 1; j < all.length; j++) {
                Assertions.assertNotSame(all[i].getClass(), all[j].getClass());
            }
        }
    }
}
