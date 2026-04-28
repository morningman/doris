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

package org.apache.doris.connector.paimon.mtmv;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.mtmv.ConnectorMtmvSnapshot;
import org.apache.doris.connector.api.mtmv.ConnectorPartitionItem;
import org.apache.doris.connector.api.mtmv.ConnectorPartitionType;
import org.apache.doris.connector.api.mtmv.MtmvRefreshHint;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class PaimonMtmvOpsTest {

    private static final Logger LOG = LogManager.getLogger(PaimonMtmvOpsTest.class);

    private static final String DB = "tpch";
    private static final String TBL = "lineitem";

    private static final MtmvRefreshHint HINT =
            MtmvRefreshHint.of(MtmvRefreshHint.RefreshMode.INCREMENTAL_AUTO);

    // -------- helpers ----------------------------------------------------

    private static RowType rowType(DataField... fields) {
        return new RowType(Arrays.asList(fields));
    }

    private static Table mockTable(List<String> partitionKeys, RowType rowType, Long latestSnapshotId) {
        Table t = Mockito.mock(Table.class);
        Mockito.when(t.partitionKeys()).thenReturn(partitionKeys);
        Mockito.when(t.rowType()).thenReturn(rowType);
        if (latestSnapshotId == null) {
            Mockito.when(t.latestSnapshot()).thenReturn(Optional.empty());
        } else {
            Snapshot s = Mockito.mock(Snapshot.class);
            Mockito.when(s.id()).thenReturn(latestSnapshotId);
            Mockito.when(t.latestSnapshot()).thenReturn(Optional.of(s));
        }
        return t;
    }

    private static Catalog mockCatalog(Table table) throws Exception {
        Catalog c = Mockito.mock(Catalog.class);
        Mockito.when(c.getTable(Identifier.create(DB, TBL))).thenReturn(table);
        return c;
    }

    private static Catalog mockCatalog(Table table, List<Partition> partitions) throws Exception {
        Catalog c = mockCatalog(table);
        Mockito.when(c.listPartitions(Identifier.create(DB, TBL))).thenReturn(partitions);
        return c;
    }

    private static Partition partition(Map<String, String> spec) {
        return new Partition(spec, 0L, 0L, 0L, 0L, false);
    }

    private static Map<String, String> spec(String... kv) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    // -------- listPartitions --------------------------------------------

    @Test
    void listPartitionsUnpartitionedReturnsSingletonItem() throws Exception {
        Table t = mockTable(Collections.emptyList(),
                rowType(new DataField(0, "id", DataTypes.BIGINT())), 7L);
        Catalog c = mockCatalog(t);
        PaimonMtmvOps ops = new PaimonMtmvOps(c);

        Map<String, ConnectorPartitionItem> result =
                ops.listPartitions(DB, TBL, Optional.empty());

        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.get("") instanceof ConnectorPartitionItem.UnpartitionedItem);
    }

    @Test
    void listPartitionsSinglePartitionKeyEmitsListItem() throws Exception {
        Table t = mockTable(Collections.singletonList("dt"),
                rowType(new DataField(0, "id", DataTypes.BIGINT()),
                        new DataField(1, "dt", DataTypes.STRING())),
                3L);
        Catalog c = mockCatalog(t,
                Arrays.asList(partition(spec("dt", "2024-01-01")),
                        partition(spec("dt", "2024-01-02"))));
        PaimonMtmvOps ops = new PaimonMtmvOps(c);

        Map<String, ConnectorPartitionItem> result =
                ops.listPartitions(DB, TBL, Optional.empty());

        Assertions.assertEquals(2, result.size());
        ConnectorPartitionItem first = result.get("dt=2024-01-01");
        Assertions.assertNotNull(first);
        Assertions.assertTrue(first instanceof ConnectorPartitionItem.ListPartitionItem);
        Assertions.assertEquals(Collections.singletonList(Collections.singletonList("2024-01-01")),
                ((ConnectorPartitionItem.ListPartitionItem) first).values());
        Assertions.assertNotNull(result.get("dt=2024-01-02"));
    }

    @Test
    void listPartitionsMultiKeyJoinsBySlash() throws Exception {
        Table t = mockTable(Arrays.asList("dt", "region"),
                rowType(new DataField(0, "id", DataTypes.BIGINT()),
                        new DataField(1, "dt", DataTypes.STRING()),
                        new DataField(2, "region", DataTypes.STRING())),
                10L);
        Catalog c = mockCatalog(t, Arrays.asList(
                partition(spec("dt", "2024-01-01", "region", "us")),
                partition(spec("dt", "2024-01-02", "region", "eu"))));
        PaimonMtmvOps ops = new PaimonMtmvOps(c);

        Map<String, ConnectorPartitionItem> result =
                ops.listPartitions(DB, TBL, Optional.empty());

        Assertions.assertEquals(2, result.size());
        ConnectorPartitionItem first = result.get("dt=2024-01-01/region=us");
        Assertions.assertNotNull(first);
        Assertions.assertEquals(Collections.singletonList(Arrays.asList("2024-01-01", "us")),
                ((ConnectorPartitionItem.ListPartitionItem) first).values());
        Assertions.assertNotNull(result.get("dt=2024-01-02/region=eu"));
    }

    @Test
    void listPartitionsRendersOrderFollowsPartitionKeys() throws Exception {
        // Partition keys are [dt, region]; spec map insertion order is reversed
        // — the rendered name and tuple must still follow partitionKeys() order.
        Table t = mockTable(Arrays.asList("dt", "region"),
                rowType(new DataField(0, "dt", DataTypes.STRING()),
                        new DataField(1, "region", DataTypes.STRING())),
                1L);
        Map<String, String> reversed = new LinkedHashMap<>();
        reversed.put("region", "us");
        reversed.put("dt", "2024-01-01");
        Catalog c = mockCatalog(t, Collections.singletonList(partition(reversed)));
        PaimonMtmvOps ops = new PaimonMtmvOps(c);

        Map<String, ConnectorPartitionItem> result =
                ops.listPartitions(DB, TBL, Optional.empty());

        Assertions.assertTrue(result.containsKey("dt=2024-01-01/region=us"));
        ConnectorPartitionItem item = result.get("dt=2024-01-01/region=us");
        Assertions.assertEquals(Collections.singletonList(Arrays.asList("2024-01-01", "us")),
                ((ConnectorPartitionItem.ListPartitionItem) item).values());
    }

    @Test
    void listPartitionsEmptyCatalogResultProducesEmptyMap() throws Exception {
        Table t = mockTable(Collections.singletonList("dt"),
                rowType(new DataField(0, "dt", DataTypes.STRING())), 1L);
        Catalog c = mockCatalog(t, Collections.emptyList());
        PaimonMtmvOps ops = new PaimonMtmvOps(c);

        Map<String, ConnectorPartitionItem> result =
                ops.listPartitions(DB, TBL, Optional.empty());

        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    void listPartitionsDedupesByName() throws Exception {
        Table t = mockTable(Collections.singletonList("dt"),
                rowType(new DataField(0, "dt", DataTypes.STRING())), 1L);
        Catalog c = mockCatalog(t, Arrays.asList(
                partition(spec("dt", "2024-01-01")),
                partition(spec("dt", "2024-01-01"))));
        PaimonMtmvOps ops = new PaimonMtmvOps(c);

        Map<String, ConnectorPartitionItem> result =
                ops.listPartitions(DB, TBL, Optional.empty());

        Assertions.assertEquals(1, result.size());
    }

    @Test
    void listPartitionsNullValueRendersAsNullLiteral() throws Exception {
        Table t = mockTable(Collections.singletonList("dt"),
                rowType(new DataField(0, "dt", DataTypes.STRING())), 1L);
        Map<String, String> nullSpec = new LinkedHashMap<>();
        nullSpec.put("dt", null);
        Catalog c = mockCatalog(t, Collections.singletonList(partition(nullSpec)));
        PaimonMtmvOps ops = new PaimonMtmvOps(c);

        Map<String, ConnectorPartitionItem> result =
                ops.listPartitions(DB, TBL, Optional.empty());

        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.containsKey("dt=null"));
    }

    // -------- getPartitionType ------------------------------------------

    @Test
    void getPartitionTypeUnpartitioned() throws Exception {
        Table t = mockTable(Collections.emptyList(),
                rowType(new DataField(0, "id", DataTypes.BIGINT())), 1L);
        PaimonMtmvOps ops = new PaimonMtmvOps(mockCatalog(t));
        Assertions.assertEquals(ConnectorPartitionType.UNPARTITIONED,
                ops.getPartitionType(DB, TBL, Optional.empty()));
    }

    @Test
    void getPartitionTypeListWhenPartitioned() throws Exception {
        Table t = mockTable(Collections.singletonList("dt"),
                rowType(new DataField(0, "dt", DataTypes.STRING())), 1L);
        PaimonMtmvOps ops = new PaimonMtmvOps(mockCatalog(t));
        Assertions.assertEquals(ConnectorPartitionType.LIST,
                ops.getPartitionType(DB, TBL, Optional.empty()));
    }

    // -------- partition columns -----------------------------------------

    @Test
    void getPartitionColumnNamesLowerCased() throws Exception {
        Table t = mockTable(Arrays.asList("DT", "Region"),
                rowType(new DataField(0, "DT", DataTypes.STRING()),
                        new DataField(1, "Region", DataTypes.STRING())),
                1L);
        PaimonMtmvOps ops = new PaimonMtmvOps(mockCatalog(t));
        Set<String> names = ops.getPartitionColumnNames(DB, TBL, Optional.empty());
        Assertions.assertEquals(Set.of("dt", "region"), names);
    }

    @Test
    void getPartitionColumnNamesEmptyWhenUnpartitioned() throws Exception {
        Table t = mockTable(Collections.emptyList(),
                rowType(new DataField(0, "id", DataTypes.BIGINT())), 1L);
        PaimonMtmvOps ops = new PaimonMtmvOps(mockCatalog(t));
        Assertions.assertTrue(
                ops.getPartitionColumnNames(DB, TBL, Optional.empty()).isEmpty());
    }

    @Test
    void getPartitionColumnsTypesAndOrder() throws Exception {
        Table t = mockTable(Arrays.asList("dt", "region"),
                rowType(new DataField(0, "id", DataTypes.BIGINT()),
                        new DataField(1, "dt", DataTypes.STRING()),
                        new DataField(2, "region", DataTypes.STRING())),
                1L);
        PaimonMtmvOps ops = new PaimonMtmvOps(mockCatalog(t));
        List<ConnectorColumn> columns = ops.getPartitionColumns(DB, TBL, Optional.empty());
        Assertions.assertEquals(2, columns.size());
        Assertions.assertEquals("dt", columns.get(0).getName());
        Assertions.assertEquals("STRING", columns.get(0).getType().getTypeName());
        Assertions.assertEquals("region", columns.get(1).getName());
    }

    @Test
    void getPartitionColumnsEmptyWhenUnpartitioned() throws Exception {
        Table t = mockTable(Collections.emptyList(),
                rowType(new DataField(0, "id", DataTypes.BIGINT())), 1L);
        PaimonMtmvOps ops = new PaimonMtmvOps(mockCatalog(t));
        Assertions.assertTrue(
                ops.getPartitionColumns(DB, TBL, Optional.empty()).isEmpty());
    }

    @Test
    void getPartitionColumnsThrowsWhenKeyMissingFromRowType() throws Exception {
        Table t = mockTable(Collections.singletonList("missing"),
                rowType(new DataField(0, "id", DataTypes.BIGINT())), 1L);
        PaimonMtmvOps ops = new PaimonMtmvOps(mockCatalog(t));
        Assertions.assertThrows(IllegalStateException.class,
                () -> ops.getPartitionColumns(DB, TBL, Optional.empty()));
    }

    // -------- snapshots --------------------------------------------------

    @Test
    void getPartitionSnapshotPropagatesLatestId() throws Exception {
        Table t = mockTable(Collections.singletonList("dt"),
                rowType(new DataField(0, "dt", DataTypes.STRING())), 42L);
        PaimonMtmvOps ops = new PaimonMtmvOps(mockCatalog(t));
        ConnectorMtmvSnapshot snap = ops.getPartitionSnapshot(
                DB, TBL, "dt=2024-01-01", HINT, Optional.empty());
        Assertions.assertTrue(snap instanceof ConnectorMtmvSnapshot.SnapshotIdMtmvSnapshot);
        Assertions.assertEquals(42L, snap.marker());
    }

    @Test
    void getPartitionSnapshotMinusOneWhenNoSnapshot() throws Exception {
        Table t = mockTable(Collections.singletonList("dt"),
                rowType(new DataField(0, "dt", DataTypes.STRING())), null);
        PaimonMtmvOps ops = new PaimonMtmvOps(mockCatalog(t));
        ConnectorMtmvSnapshot snap = ops.getPartitionSnapshot(
                DB, TBL, "dt=2024-01-01", HINT, Optional.empty());
        Assertions.assertEquals(-1L, snap.marker());
    }

    @Test
    void getPartitionSnapshotRejectsNullPartitionName() throws Exception {
        Table t = mockTable(Collections.singletonList("dt"),
                rowType(new DataField(0, "dt", DataTypes.STRING())), 1L);
        PaimonMtmvOps ops = new PaimonMtmvOps(mockCatalog(t));
        Assertions.assertThrows(NullPointerException.class,
                () -> ops.getPartitionSnapshot(DB, TBL, null, HINT, Optional.empty()));
    }

    @Test
    void getTableSnapshotPropagatesLatestId() throws Exception {
        Table t = mockTable(Collections.emptyList(),
                rowType(new DataField(0, "id", DataTypes.BIGINT())), 99L);
        PaimonMtmvOps ops = new PaimonMtmvOps(mockCatalog(t));
        ConnectorMtmvSnapshot snap = ops.getTableSnapshot(DB, TBL, HINT, Optional.empty());
        Assertions.assertEquals(99L, snap.marker());
    }

    @Test
    void getTableSnapshotMinusOneWhenNoSnapshot() throws Exception {
        Table t = mockTable(Collections.emptyList(),
                rowType(new DataField(0, "id", DataTypes.BIGINT())), null);
        PaimonMtmvOps ops = new PaimonMtmvOps(mockCatalog(t));
        ConnectorMtmvSnapshot snap = ops.getTableSnapshot(DB, TBL, HINT, Optional.empty());
        Assertions.assertEquals(-1L, snap.marker());
    }

    @Test
    void getNewestUpdateVersionOrTimeReturnsLatestId() throws Exception {
        Table t = mockTable(Collections.emptyList(),
                rowType(new DataField(0, "id", DataTypes.BIGINT())), 17L);
        PaimonMtmvOps ops = new PaimonMtmvOps(mockCatalog(t));
        Assertions.assertEquals(17L, ops.getNewestUpdateVersionOrTime(DB, TBL));
    }

    @Test
    void getNewestUpdateVersionOrTimeMinusOneWhenNoSnapshot() throws Exception {
        Table t = mockTable(Collections.emptyList(),
                rowType(new DataField(0, "id", DataTypes.BIGINT())), null);
        PaimonMtmvOps ops = new PaimonMtmvOps(mockCatalog(t));
        Assertions.assertEquals(-1L, ops.getNewestUpdateVersionOrTime(DB, TBL));
    }

    @Test
    void getNewestUpdateVersionOrTimeMinusOneWhenLoaderThrows() throws Exception {
        Catalog c = Mockito.mock(Catalog.class);
        Mockito.when(c.getTable(Mockito.any()))
                .thenThrow(new Catalog.TableNotExistException(Identifier.create(DB, TBL)));
        PaimonMtmvOps ops = new PaimonMtmvOps(c);
        Assertions.assertEquals(-1L, ops.getNewestUpdateVersionOrTime(DB, TBL));
    }

    // -------- misc -------------------------------------------------------

    @Test
    void isPartitionColumnAllowNullAlwaysTrue() throws Exception {
        Table t = mockTable(Collections.emptyList(),
                rowType(new DataField(0, "id", DataTypes.BIGINT())), 1L);
        PaimonMtmvOps ops = new PaimonMtmvOps(mockCatalog(t));
        Assertions.assertTrue(ops.isPartitionColumnAllowNull(DB, TBL));
    }

    @Test
    void isValidRelatedTableTrueWhenLoaderSucceeds() throws Exception {
        Table t = mockTable(Collections.emptyList(),
                rowType(new DataField(0, "id", DataTypes.BIGINT())), 1L);
        PaimonMtmvOps ops = new PaimonMtmvOps(mockCatalog(t));
        Assertions.assertTrue(ops.isValidRelatedTable(DB, TBL));
    }

    @Test
    void isValidRelatedTableFalseWhenLoaderThrows() throws Exception {
        Catalog c = Mockito.mock(Catalog.class);
        Mockito.when(c.getTable(Mockito.any()))
                .thenThrow(new Catalog.TableNotExistException(Identifier.create(DB, TBL)));
        PaimonMtmvOps ops = new PaimonMtmvOps(c);
        Assertions.assertFalse(ops.isValidRelatedTable(DB, TBL));
    }

    @Test
    void needAutoRefreshAlwaysTrue() throws Exception {
        Table t = mockTable(Collections.emptyList(),
                rowType(new DataField(0, "id", DataTypes.BIGINT())), 1L);
        PaimonMtmvOps ops = new PaimonMtmvOps(mockCatalog(t));
        Assertions.assertTrue(ops.needAutoRefresh(DB, TBL));
    }

    @Test
    void constructorRejectsNullCatalog() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new PaimonMtmvOps(null));
    }
}
