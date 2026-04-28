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

package org.apache.doris.connector.iceberg.mtmv;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.mtmv.ConnectorMtmvSnapshot;
import org.apache.doris.connector.api.mtmv.ConnectorPartitionItem;
import org.apache.doris.connector.api.mtmv.ConnectorPartitionType;
import org.apache.doris.connector.api.mtmv.MtmvRefreshHint;
import org.apache.doris.connector.api.timetravel.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

class IcebergMtmvOpsTest {

    private static final String DB = "tpch";
    private static final String TBL = "lineitem";

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.optional(1, "dt", Types.DateType.get()),
            Types.NestedField.optional(2, "ts", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(3, "id", Types.LongType.get()),
            Types.NestedField.optional(4, "region", Types.StringType.get()));

    private static final MtmvRefreshHint HINT = MtmvRefreshHint.of(MtmvRefreshHint.RefreshMode.INCREMENTAL_AUTO);

    // -------- helpers ----------------------------------------------------

    private static Table mockTable(PartitionSpec currentSpec,
                                   Map<Integer, PartitionSpec> historicalSpecs,
                                   Long currentSnapshotId) {
        Table t = Mockito.mock(Table.class);
        Mockito.when(t.schema()).thenReturn(SCHEMA);
        Mockito.when(t.spec()).thenReturn(currentSpec);
        Mockito.when(t.specs()).thenReturn(historicalSpecs);
        if (currentSnapshotId == null) {
            Mockito.when(t.currentSnapshot()).thenReturn(null);
        } else {
            Snapshot s = Mockito.mock(Snapshot.class);
            Mockito.when(s.snapshotId()).thenReturn(currentSnapshotId);
            Mockito.when(t.currentSnapshot()).thenReturn(s);
        }
        return t;
    }

    private static Table mockTable(PartitionSpec spec, Long currentSnapshotId) {
        Map<Integer, PartitionSpec> specs = new LinkedHashMap<>();
        specs.put(spec.specId(), spec);
        return mockTable(spec, specs, currentSnapshotId);
    }

    private static BiFunction<String, String, Table> loader(Table t) {
        return (db, tbl) -> t;
    }

    private static FileScanTask fileTask(StructLike partition) {
        FileScanTask task = Mockito.mock(FileScanTask.class);
        Mockito.when(task.partition()).thenReturn(partition);
        return task;
    }

    private static StructLike singleValueStruct(Object value) {
        return new StructLike() {
            @Override
            public int size() {
                return 1;
            }

            @SuppressWarnings("unchecked")
            @Override
            public <T> T get(int pos, Class<T> javaClass) {
                return (T) value;
            }

            @Override
            public <T> void set(int pos, T v) {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static void wireScan(Table table, FileScanTask... tasks) {
        TableScan scan = Mockito.mock(TableScan.class);
        Mockito.when(table.newScan()).thenReturn(scan);
        Mockito.when(scan.useSnapshot(Mockito.anyLong())).thenReturn(scan);
        Mockito.when(scan.planFiles()).thenReturn(CloseableIterable.withNoopClose(Arrays.asList(tasks)));
    }

    private static ConnectorMvccSnapshot mvccBySnapshotId(long id) {
        return new ConnectorMvccSnapshot() {
            @Override
            public Instant commitTime() {
                return Instant.EPOCH;
            }

            @Override
            public Optional<ConnectorTableVersion> asVersion() {
                return Optional.of(new ConnectorTableVersion.BySnapshotId(id));
            }

            @Override
            public String toOpaqueToken() {
                return Long.toString(id);
            }
        };
    }

    // -------- listPartitions --------------------------------------------

    @Test
    void listPartitionsUnpartitionedReturnsSingleUnpartitionedItem() {
        Table t = mockTable(PartitionSpec.unpartitioned(), 100L);
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        Map<String, ConnectorPartitionItem> result = ops.listPartitions(DB, TBL, Optional.empty());

        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.get("") instanceof ConnectorPartitionItem.UnpartitionedItem);
    }

    @Test
    void listPartitionsIdentitySingleValue() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("region").build();
        Table t = mockTable(spec, 100L);
        wireScan(t, fileTask(singleValueStruct("US")));
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        Map<String, ConnectorPartitionItem> result = ops.listPartitions(DB, TBL, Optional.empty());

        Assertions.assertEquals(1, result.size());
        ConnectorPartitionItem item = result.get("region=US");
        Assertions.assertTrue(item instanceof ConnectorPartitionItem.ListPartitionItem);
        Assertions.assertEquals(Collections.singletonList(Collections.singletonList("US")),
                ((ConnectorPartitionItem.ListPartitionItem) item).values());
    }

    @Test
    void listPartitionsMultipleIdentityValuesDeduped() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("region").build();
        Table t = mockTable(spec, 100L);
        wireScan(t,
                fileTask(singleValueStruct("US")),
                fileTask(singleValueStruct("EU")),
                fileTask(singleValueStruct("US")));
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        Map<String, ConnectorPartitionItem> result = ops.listPartitions(DB, TBL, Optional.empty());

        Assertions.assertEquals(2, result.size());
        Assertions.assertTrue(result.containsKey("region=US"));
        Assertions.assertTrue(result.containsKey("region=EU"));
    }

    @Test
    void listPartitionsMonthTransformProducesHumanString() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("ts").build();
        Table t = mockTable(spec, 100L);
        // months since epoch: Jan 2021 = (2021-1970)*12 + 0 = 612
        wireScan(t, fileTask(singleValueStruct(612)));
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        Map<String, ConnectorPartitionItem> result = ops.listPartitions(DB, TBL, Optional.empty());

        Assertions.assertEquals(1, result.size());
        // partition path looks like "ts_month=2021-01"
        String key = result.keySet().iterator().next();
        Assertions.assertTrue(key.startsWith("ts_month="), key);
        ConnectorPartitionItem.ListPartitionItem item =
                (ConnectorPartitionItem.ListPartitionItem) result.get(key);
        Assertions.assertEquals(1, item.values().size());
        Assertions.assertEquals(1, item.values().get(0).size());
        Assertions.assertEquals("2021-01", item.values().get(0).get(0));
    }

    @Test
    void listPartitionsHourTransformProducesHumanString() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).hour("ts").build();
        Table t = mockTable(spec, 100L);
        // hours since epoch arbitrary (e.g. 2021-01-01T03)
        int hours = (int) java.time.Duration.between(Instant.EPOCH,
                Instant.parse("2021-01-01T03:00:00Z")).toHours();
        wireScan(t, fileTask(singleValueStruct(hours)));
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        Map<String, ConnectorPartitionItem> result = ops.listPartitions(DB, TBL, Optional.empty());

        Assertions.assertEquals(1, result.size());
        String value = ((ConnectorPartitionItem.ListPartitionItem) result.values().iterator().next())
                .values().get(0).get(0);
        Assertions.assertEquals("2021-01-01-03", value);
    }

    @Test
    void listPartitionsAlwaysEmitsListNeverRange() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("ts").build();
        Table t = mockTable(spec, 100L);
        wireScan(t, fileTask(singleValueStruct(18628))); // 2021-01-01 days since epoch
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        Map<String, ConnectorPartitionItem> result = ops.listPartitions(DB, TBL, Optional.empty());

        Assertions.assertFalse(result.isEmpty());
        for (ConnectorPartitionItem v : result.values()) {
            Assertions.assertTrue(v instanceof ConnectorPartitionItem.ListPartitionItem,
                    "expected ListPartitionItem but got " + v.getClass());
        }
    }

    @Test
    void listPartitionsPinsToMvccSnapshot() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("region").build();
        Table t = mockTable(spec, 100L);
        TableScan scan = Mockito.mock(TableScan.class);
        Mockito.when(t.newScan()).thenReturn(scan);
        Mockito.when(scan.useSnapshot(Mockito.anyLong())).thenReturn(scan);
        Mockito.when(scan.planFiles()).thenReturn(CloseableIterable.empty());
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        ops.listPartitions(DB, TBL, Optional.of(mvccBySnapshotId(42L)));

        Mockito.verify(scan).useSnapshot(42L);
    }

    @Test
    void listPartitionsWithoutMvccDoesNotPin() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("region").build();
        Table t = mockTable(spec, 100L);
        TableScan scan = Mockito.mock(TableScan.class);
        Mockito.when(t.newScan()).thenReturn(scan);
        Mockito.when(scan.planFiles()).thenReturn(CloseableIterable.empty());
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        ops.listPartitions(DB, TBL, Optional.empty());

        Mockito.verify(scan, Mockito.never()).useSnapshot(Mockito.anyLong());
    }

    // -------- getPartitionType ------------------------------------------

    @Test
    void getPartitionTypeUnpartitioned() {
        Table t = mockTable(PartitionSpec.unpartitioned(), 1L);
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        Assertions.assertEquals(ConnectorPartitionType.UNPARTITIONED,
                ops.getPartitionType(DB, TBL, Optional.empty()));
    }

    @Test
    void getPartitionTypePartitionedReturnsList() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("ts").build();
        Table t = mockTable(spec, 1L);
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        Assertions.assertEquals(ConnectorPartitionType.LIST,
                ops.getPartitionType(DB, TBL, Optional.empty()));
    }

    // -------- getPartitionColumnNames -----------------------------------

    @Test
    void getPartitionColumnNamesUnpartitioned() {
        Table t = mockTable(PartitionSpec.unpartitioned(), 1L);
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        Assertions.assertTrue(ops.getPartitionColumnNames(DB, TBL, Optional.empty()).isEmpty());
    }

    @Test
    void getPartitionColumnNamesUsesSourceColumn() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("ts").build();
        Table t = mockTable(spec, 1L);
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        Set<String> names = ops.getPartitionColumnNames(DB, TBL, Optional.empty());
        Assertions.assertEquals(Collections.singleton("ts"), names);
    }

    // -------- getPartitionColumns ---------------------------------------

    @Test
    void getPartitionColumnsUnpartitioned() {
        Table t = mockTable(PartitionSpec.unpartitioned(), 1L);
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        Assertions.assertTrue(ops.getPartitionColumns(DB, TBL, Optional.empty()).isEmpty());
    }

    @Test
    void getPartitionColumnsForMonthTransformReturnsIntColumn() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("ts").build();
        Table t = mockTable(spec, 1L);
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        List<ConnectorColumn> cols = ops.getPartitionColumns(DB, TBL, Optional.empty());
        Assertions.assertEquals(1, cols.size());
        Assertions.assertEquals("ts", cols.get(0).getName());
        // month transform returns an INT-shaped result type
        Assertions.assertEquals("INT", cols.get(0).getType().getTypeName());
    }

    // -------- getPartitionSnapshot --------------------------------------

    @Test
    void getPartitionSnapshotPropagatesSnapshotId() {
        Table t = mockTable(PartitionSpec.unpartitioned(), 999L);
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        ConnectorMtmvSnapshot snap = ops.getPartitionSnapshot(DB, TBL, "p1", HINT, Optional.empty());

        Assertions.assertTrue(snap instanceof ConnectorMtmvSnapshot.SnapshotIdMtmvSnapshot);
        Assertions.assertEquals(999L, snap.marker());
    }

    @Test
    void getPartitionSnapshotRejectsNullPartitionName() {
        Table t = mockTable(PartitionSpec.unpartitioned(), 1L);
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        Assertions.assertThrows(NullPointerException.class,
                () -> ops.getPartitionSnapshot(DB, TBL, null, HINT, Optional.empty()));
    }

    @Test
    void getPartitionSnapshotWhenTableHasNoSnapshotReturnsMinusOne() {
        Table t = mockTable(PartitionSpec.unpartitioned(), null);
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        ConnectorMtmvSnapshot snap = ops.getPartitionSnapshot(DB, TBL, "p1", HINT, Optional.empty());
        Assertions.assertEquals(-1L, snap.marker());
    }

    @Test
    void getPartitionSnapshotPrefersMvccSnapshotIdOverCurrent() {
        Table t = mockTable(PartitionSpec.unpartitioned(), 100L);
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        ConnectorMtmvSnapshot snap = ops.getPartitionSnapshot(DB, TBL, "p1", HINT,
                Optional.of(mvccBySnapshotId(7L)));

        Assertions.assertEquals(7L, snap.marker());
    }

    // -------- getTableSnapshot ------------------------------------------

    @Test
    void getTableSnapshotReturnsSnapshotId() {
        Table t = mockTable(PartitionSpec.unpartitioned(), 555L);
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        ConnectorMtmvSnapshot snap = ops.getTableSnapshot(DB, TBL, HINT, Optional.empty());
        Assertions.assertTrue(snap instanceof ConnectorMtmvSnapshot.SnapshotIdMtmvSnapshot);
        Assertions.assertEquals(555L, snap.marker());
    }

    @Test
    void getTableSnapshotWhenEmptyReturnsMinusOne() {
        Table t = mockTable(PartitionSpec.unpartitioned(), null);
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        Assertions.assertEquals(-1L,
                ops.getTableSnapshot(DB, TBL, HINT, Optional.empty()).marker());
    }

    // -------- getNewestUpdateVersionOrTime ------------------------------

    @Test
    void getNewestUpdateVersionOrTimeReturnsSnapshotId() {
        Table t = mockTable(PartitionSpec.unpartitioned(), 321L);
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        Assertions.assertEquals(321L, ops.getNewestUpdateVersionOrTime(DB, TBL));
    }

    @Test
    void getNewestUpdateVersionOrTimeNoSnapshotReturnsMinusOne() {
        Table t = mockTable(PartitionSpec.unpartitioned(), null);
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        Assertions.assertEquals(-1L, ops.getNewestUpdateVersionOrTime(DB, TBL));
    }

    @Test
    void getNewestUpdateVersionOrTimeOnLoadFailureReturnsMinusOne() {
        BiFunction<String, String, Table> badLoader = (db, tbl) -> {
            throw new RuntimeException("boom");
        };
        IcebergMtmvOps ops = new IcebergMtmvOps(badLoader);

        Assertions.assertEquals(-1L, ops.getNewestUpdateVersionOrTime(DB, TBL));
    }

    // -------- isPartitionColumnAllowNull --------------------------------

    @Test
    void isPartitionColumnAllowNullAlwaysTrue() {
        IcebergMtmvOps ops = new IcebergMtmvOps((db, tbl) -> {
            throw new AssertionError("should not load");
        });

        Assertions.assertTrue(ops.isPartitionColumnAllowNull(DB, TBL));
    }

    // -------- isValidRelatedTable ---------------------------------------

    @Test
    void isValidRelatedTableUnpartitionedTrue() {
        Table t = mockTable(PartitionSpec.unpartitioned(), 1L);
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        Assertions.assertTrue(ops.isValidRelatedTable(DB, TBL));
    }

    @Test
    void isValidRelatedTableSingleIdentityTrue() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("region").build();
        Table t = mockTable(spec, 1L);
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        Assertions.assertTrue(ops.isValidRelatedTable(DB, TBL));
    }

    @Test
    void isValidRelatedTableSingleMonthTrue() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).month("ts").build();
        Table t = mockTable(spec, 1L);
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        Assertions.assertTrue(ops.isValidRelatedTable(DB, TBL));
    }

    @Test
    void isValidRelatedTableMultiFieldFalse() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
                .identity("region").identity("id").build();
        Table t = mockTable(spec, 1L);
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        Assertions.assertFalse(ops.isValidRelatedTable(DB, TBL));
    }

    @Test
    void isValidRelatedTableBucketTransformFalse() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).bucket("id", 16).build();
        Table t = mockTable(spec, 1L);
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        Assertions.assertFalse(ops.isValidRelatedTable(DB, TBL));
    }

    @Test
    void isValidRelatedTableTruncateTransformFalse() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).truncate("region", 4).build();
        Table t = mockTable(spec, 1L);
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        Assertions.assertFalse(ops.isValidRelatedTable(DB, TBL));
    }

    @Test
    void isValidRelatedTableLoadFailureFalseNotThrows() {
        BiFunction<String, String, Table> badLoader = (db, tbl) -> {
            throw new RuntimeException("table missing");
        };
        IcebergMtmvOps ops = new IcebergMtmvOps(badLoader);

        Assertions.assertFalse(ops.isValidRelatedTable(DB, TBL));
    }

    @Test
    void isValidRelatedTableHistoricalSpecsWithDifferentSourcesFalse() {
        PartitionSpec spec0 = PartitionSpec.builderFor(SCHEMA).identity("region").build();
        PartitionSpec spec1 = PartitionSpec.builderFor(SCHEMA).withSpecId(1).identity("id").build();
        Map<Integer, PartitionSpec> specs = new LinkedHashMap<>();
        specs.put(0, spec0);
        specs.put(1, spec1);
        Table t = mockTable(spec1, specs, 1L);
        IcebergMtmvOps ops = new IcebergMtmvOps(loader(t));

        Assertions.assertFalse(ops.isValidRelatedTable(DB, TBL));
    }

    // -------- needAutoRefresh --------------------------------------------

    @Test
    void needAutoRefreshAlwaysTrue() {
        IcebergMtmvOps ops = new IcebergMtmvOps((db, tbl) -> {
            throw new AssertionError("should not load");
        });

        Assertions.assertTrue(ops.needAutoRefresh(DB, TBL));
    }

    // -------- constructor guard -----------------------------------------

    @Test
    void constructorRejectsNullLoader() {
        Assertions.assertThrows(NullPointerException.class, () -> new IcebergMtmvOps(null));
    }
}
