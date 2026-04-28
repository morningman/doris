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

package org.apache.doris.connector.hive.mtmv;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.mtmv.ConnectorMtmvSnapshot;
import org.apache.doris.connector.api.mtmv.ConnectorPartitionItem;
import org.apache.doris.connector.api.mtmv.ConnectorPartitionType;
import org.apache.doris.connector.api.mtmv.MtmvRefreshHint;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsClientException;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class HiveMtmvOpsTest {

    private static final String DB = "tpch";
    private static final String TBL = "lineitem";

    private static ConnectorColumn col(String name, String typeName) {
        return new ConnectorColumn(name, ConnectorType.of(typeName), "", true, null);
    }

    private static HmsTableInfo unpartitionedTable() {
        return HmsTableInfo.builder()
                .dbName(DB).tableName(TBL).tableType("MANAGED_TABLE")
                .partitionKeys(Collections.emptyList())
                .parameters(Collections.emptyMap())
                .build();
    }

    private static HmsTableInfo singleColPartTable(Map<String, String> params) {
        return HmsTableInfo.builder()
                .dbName(DB).tableName(TBL).tableType("MANAGED_TABLE")
                .partitionKeys(Collections.singletonList(col("dt", "string")))
                .parameters(params)
                .build();
    }

    private static HmsTableInfo multiColPartTable() {
        return HmsTableInfo.builder()
                .dbName(DB).tableName(TBL).tableType("MANAGED_TABLE")
                .partitionKeys(Arrays.asList(col("dt", "string"), col("region", "string")))
                .parameters(Collections.emptyMap())
                .build();
    }

    private static HmsPartitionInfo part(List<String> values, Map<String, String> params, int createTime) {
        return new HmsPartitionInfo(values, "/loc", "in", "out", "ser", params, createTime);
    }

    // -------------------------------------------------------- listPartitions

    @Test
    void listPartitionsUnpartitionedReturnsSingletonUnpartitionedItem() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Mockito.when(client.getTable(DB, TBL)).thenReturn(unpartitionedTable());

        Map<String, ConnectorPartitionItem> result =
                new HiveMtmvOps(client).listPartitions(DB, TBL, Optional.empty());

        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.values().iterator().next() instanceof ConnectorPartitionItem.UnpartitionedItem);
        Mockito.verify(client, Mockito.never()).listPartitionNames(Mockito.anyString(), Mockito.anyString(), Mockito.anyInt());
    }

    @Test
    void listPartitionsSingleColumnReturnsListItems() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Mockito.when(client.getTable(DB, TBL)).thenReturn(singleColPartTable(Collections.emptyMap()));
        Mockito.when(client.listPartitionNames(DB, TBL, Integer.MAX_VALUE))
                .thenReturn(Arrays.asList("dt=2024-01-01", "dt=2024-01-02"));
        Mockito.when(client.getPartitions(DB, TBL, Arrays.asList("dt=2024-01-01", "dt=2024-01-02")))
                .thenReturn(Arrays.asList(
                        part(Collections.singletonList("2024-01-01"), Collections.emptyMap(), 100),
                        part(Collections.singletonList("2024-01-02"), Collections.emptyMap(), 200)));

        Map<String, ConnectorPartitionItem> result =
                new HiveMtmvOps(client).listPartitions(DB, TBL, Optional.empty());

        Assertions.assertEquals(2, result.size());
        ConnectorPartitionItem item = result.get("dt=2024-01-01");
        Assertions.assertTrue(item instanceof ConnectorPartitionItem.ListPartitionItem);
        ConnectorPartitionItem.ListPartitionItem lp = (ConnectorPartitionItem.ListPartitionItem) item;
        Assertions.assertEquals(1, lp.values().size());
        Assertions.assertEquals(Collections.singletonList("2024-01-01"), lp.values().get(0));
    }

    @Test
    void listPartitionsMultiColumnReturnsListItemsWithFullTuple() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Mockito.when(client.getTable(DB, TBL)).thenReturn(multiColPartTable());
        Mockito.when(client.listPartitionNames(DB, TBL, Integer.MAX_VALUE))
                .thenReturn(Collections.singletonList("dt=2024-01-01/region=us"));
        Mockito.when(client.getPartitions(DB, TBL, Collections.singletonList("dt=2024-01-01/region=us")))
                .thenReturn(Collections.singletonList(
                        part(Arrays.asList("2024-01-01", "us"), Collections.emptyMap(), 0)));

        Map<String, ConnectorPartitionItem> result =
                new HiveMtmvOps(client).listPartitions(DB, TBL, Optional.empty());

        ConnectorPartitionItem.ListPartitionItem lp =
                (ConnectorPartitionItem.ListPartitionItem) result.get("dt=2024-01-01/region=us");
        Assertions.assertEquals(Arrays.asList("2024-01-01", "us"), lp.values().get(0));
    }

    @Test
    void listPartitionsNeverEmitsRangePartitionItem() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Mockito.when(client.getTable(DB, TBL)).thenReturn(singleColPartTable(Collections.emptyMap()));
        Mockito.when(client.listPartitionNames(DB, TBL, Integer.MAX_VALUE))
                .thenReturn(Collections.singletonList("dt=2024-01-01"));
        Mockito.when(client.getPartitions(Mockito.eq(DB), Mockito.eq(TBL), Mockito.anyList()))
                .thenReturn(Collections.singletonList(
                        part(Collections.singletonList("2024-01-01"), Collections.emptyMap(), 0)));

        for (ConnectorPartitionItem item : new HiveMtmvOps(client)
                .listPartitions(DB, TBL, Optional.empty()).values()) {
            Assertions.assertFalse(item instanceof ConnectorPartitionItem.RangePartitionItem,
                    "Hive must never emit RangePartitionItem");
        }
    }

    @Test
    void listPartitionsNoPartitionsReturnsEmptyMap() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Mockito.when(client.getTable(DB, TBL)).thenReturn(singleColPartTable(Collections.emptyMap()));
        Mockito.when(client.listPartitionNames(DB, TBL, Integer.MAX_VALUE))
                .thenReturn(Collections.emptyList());

        Map<String, ConnectorPartitionItem> result =
                new HiveMtmvOps(client).listPartitions(DB, TBL, Optional.empty());

        Assertions.assertTrue(result.isEmpty());
    }

    // -------------------------------------------------------- getPartitionType

    @Test
    void getPartitionTypeUnpartitioned() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Mockito.when(client.getTable(DB, TBL)).thenReturn(unpartitionedTable());
        Assertions.assertEquals(ConnectorPartitionType.UNPARTITIONED,
                new HiveMtmvOps(client).getPartitionType(DB, TBL, Optional.empty()));
    }

    @Test
    void getPartitionTypeSingleColumnIsList() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Mockito.when(client.getTable(DB, TBL)).thenReturn(singleColPartTable(Collections.emptyMap()));
        Assertions.assertEquals(ConnectorPartitionType.LIST,
                new HiveMtmvOps(client).getPartitionType(DB, TBL, Optional.empty()));
    }

    @Test
    void getPartitionTypeMultiColumnIsList() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Mockito.when(client.getTable(DB, TBL)).thenReturn(multiColPartTable());
        Assertions.assertEquals(ConnectorPartitionType.LIST,
                new HiveMtmvOps(client).getPartitionType(DB, TBL, Optional.empty()));
    }

    // ---------------------------------------------- partition columns / names

    @Test
    void getPartitionColumnNamesLowercased() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Mockito.when(client.getTable(DB, TBL)).thenReturn(HmsTableInfo.builder()
                .dbName(DB).tableName(TBL).tableType("MANAGED_TABLE")
                .partitionKeys(Arrays.asList(col("DT", "string"), col("Region", "string")))
                .build());
        Set<String> names = new HiveMtmvOps(client).getPartitionColumnNames(DB, TBL, Optional.empty());
        Assertions.assertEquals(2, names.size());
        Assertions.assertTrue(names.contains("dt"));
        Assertions.assertTrue(names.contains("region"));
    }

    @Test
    void getPartitionColumnNamesEmptyWhenUnpartitioned() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Mockito.when(client.getTable(DB, TBL)).thenReturn(unpartitionedTable());
        Assertions.assertTrue(new HiveMtmvOps(client)
                .getPartitionColumnNames(DB, TBL, Optional.empty()).isEmpty());
    }

    @Test
    void getPartitionColumnsReturnsTypedColumns() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Mockito.when(client.getTable(DB, TBL)).thenReturn(multiColPartTable());
        List<ConnectorColumn> cols = new HiveMtmvOps(client)
                .getPartitionColumns(DB, TBL, Optional.empty());
        Assertions.assertEquals(2, cols.size());
        Assertions.assertEquals("dt", cols.get(0).getName());
        Assertions.assertEquals("string", cols.get(0).getType().getTypeName());
        Assertions.assertEquals("region", cols.get(1).getName());
    }

    @Test
    void getPartitionColumnsEmptyWhenUnpartitioned() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Mockito.when(client.getTable(DB, TBL)).thenReturn(unpartitionedTable());
        Assertions.assertTrue(new HiveMtmvOps(client)
                .getPartitionColumns(DB, TBL, Optional.empty()).isEmpty());
    }

    // ----------------------------------------------------- partition snapshot

    @Test
    void getPartitionSnapshotPrefersTransientLastDdlTime() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Mockito.when(client.getTable(DB, TBL)).thenReturn(singleColPartTable(Collections.emptyMap()));
        Map<String, String> params = new HashMap<>();
        params.put("transient_lastDdlTime", "1700000000");
        Mockito.when(client.getPartition(DB, TBL, Collections.singletonList("2024-01-01")))
                .thenReturn(part(Collections.singletonList("2024-01-01"), params, 1234));

        ConnectorMtmvSnapshot snap = new HiveMtmvOps(client).getPartitionSnapshot(
                DB, TBL, "dt=2024-01-01",
                MtmvRefreshHint.of(MtmvRefreshHint.RefreshMode.INCREMENTAL_AUTO),
                Optional.empty());

        Assertions.assertTrue(snap instanceof ConnectorMtmvSnapshot.TimestampMtmvSnapshot);
        Assertions.assertEquals(1700000000L * 1000L, snap.marker());
    }

    @Test
    void getPartitionSnapshotFallsBackToCreateTime() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Mockito.when(client.getTable(DB, TBL)).thenReturn(singleColPartTable(Collections.emptyMap()));
        Mockito.when(client.getPartition(DB, TBL, Collections.singletonList("2024-01-01")))
                .thenReturn(part(Collections.singletonList("2024-01-01"), Collections.emptyMap(), 555));

        ConnectorMtmvSnapshot snap = new HiveMtmvOps(client).getPartitionSnapshot(
                DB, TBL, "dt=2024-01-01",
                MtmvRefreshHint.of(MtmvRefreshHint.RefreshMode.INCREMENTAL_AUTO),
                Optional.empty());

        Assertions.assertEquals(555L * 1000L, snap.marker());
    }

    @Test
    void getPartitionSnapshotMultiColumnParsesAllValues() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Mockito.when(client.getTable(DB, TBL)).thenReturn(multiColPartTable());
        Map<String, String> params = new HashMap<>();
        params.put("transient_lastDdlTime", "1700000123");
        Mockito.when(client.getPartition(DB, TBL, Arrays.asList("2024-01-01", "us")))
                .thenReturn(part(Arrays.asList("2024-01-01", "us"), params, 0));

        ConnectorMtmvSnapshot snap = new HiveMtmvOps(client).getPartitionSnapshot(
                DB, TBL, "dt=2024-01-01/region=us",
                MtmvRefreshHint.of(MtmvRefreshHint.RefreshMode.INCREMENTAL_AUTO),
                Optional.empty());

        Assertions.assertEquals(1700000123L * 1000L, snap.marker());
        Mockito.verify(client).getPartition(DB, TBL, Arrays.asList("2024-01-01", "us"));
    }

    // --------------------------------------------------------- table snapshot

    @Test
    void getTableSnapshotUsesTransientLastDdlTime() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Map<String, String> params = new LinkedHashMap<>();
        params.put("transient_lastDdlTime", "1700000050");
        Mockito.when(client.getTable(DB, TBL)).thenReturn(singleColPartTable(params));

        ConnectorMtmvSnapshot snap = new HiveMtmvOps(client).getTableSnapshot(
                DB, TBL, MtmvRefreshHint.of(MtmvRefreshHint.RefreshMode.INCREMENTAL_AUTO),
                Optional.empty());

        Assertions.assertTrue(snap instanceof ConnectorMtmvSnapshot.MaxTimestampMtmvSnapshot);
        Assertions.assertEquals(1700000050L * 1000L, snap.marker());
    }

    @Test
    void getTableSnapshotMissingParameterReturnsZero() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Mockito.when(client.getTable(DB, TBL)).thenReturn(unpartitionedTable());
        ConnectorMtmvSnapshot snap = new HiveMtmvOps(client).getTableSnapshot(
                DB, TBL, MtmvRefreshHint.of(MtmvRefreshHint.RefreshMode.INCREMENTAL_AUTO),
                Optional.empty());
        Assertions.assertEquals(0L, snap.marker());
    }

    @Test
    void getNewestUpdateVersionOrTimeMatchesTableSnapshot() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Map<String, String> params = new HashMap<>();
        params.put("transient_lastDdlTime", "42");
        Mockito.when(client.getTable(DB, TBL)).thenReturn(singleColPartTable(params));

        long v = new HiveMtmvOps(client).getNewestUpdateVersionOrTime(DB, TBL);
        Assertions.assertEquals(42L * 1000L, v);
    }

    @Test
    void getNewestUpdateVersionOrTimeUnparseableReturnsZero() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Map<String, String> params = new HashMap<>();
        params.put("transient_lastDdlTime", "not-a-number");
        Mockito.when(client.getTable(DB, TBL)).thenReturn(singleColPartTable(params));

        Assertions.assertEquals(0L, new HiveMtmvOps(client).getNewestUpdateVersionOrTime(DB, TBL));
    }

    // ------------------------------------------------------------- predicates

    @Test
    void isPartitionColumnAllowNullAlwaysTrue() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Assertions.assertTrue(new HiveMtmvOps(client).isPartitionColumnAllowNull(DB, TBL));
        Mockito.verifyNoInteractions(client);
    }

    @Test
    void isValidRelatedTableTrueForRegularTable() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Mockito.when(client.getTable(DB, TBL)).thenReturn(singleColPartTable(Collections.emptyMap()));
        Assertions.assertTrue(new HiveMtmvOps(client).isValidRelatedTable(DB, TBL));
    }

    @Test
    void isValidRelatedTableFalseForAcidTable() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Map<String, String> params = new HashMap<>();
        params.put("transactional", "true");
        Mockito.when(client.getTable(DB, TBL)).thenReturn(singleColPartTable(params));
        Assertions.assertFalse(new HiveMtmvOps(client).isValidRelatedTable(DB, TBL));
    }

    @Test
    void isValidRelatedTableFalseForAcidTableMixedCase() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Map<String, String> params = new HashMap<>();
        params.put("transactional", "TRUE");
        Mockito.when(client.getTable(DB, TBL)).thenReturn(singleColPartTable(params));
        Assertions.assertFalse(new HiveMtmvOps(client).isValidRelatedTable(DB, TBL));
    }

    @Test
    void isValidRelatedTableFalseOnHmsError() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Mockito.when(client.getTable(DB, TBL)).thenThrow(new HmsClientException("boom"));
        // Must not throw — bridge contract.
        Assertions.assertFalse(new HiveMtmvOps(client).isValidRelatedTable(DB, TBL));
    }

    @Test
    void needAutoRefreshAlwaysTrue() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Assertions.assertTrue(new HiveMtmvOps(client).needAutoRefresh(DB, TBL));
        Mockito.verifyNoInteractions(client);
    }

    @Test
    void constructorRejectsNullClient() {
        Assertions.assertThrows(NullPointerException.class, () -> new HiveMtmvOps(null));
    }
}
