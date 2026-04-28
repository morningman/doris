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

package org.apache.doris.datasource.mtmv;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorTableId;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.mtmv.ConnectorMtmvSnapshot;
import org.apache.doris.connector.api.mtmv.ConnectorPartitionItem;
import org.apache.doris.connector.api.mtmv.ConnectorPartitionType;
import org.apache.doris.connector.api.mtmv.MtmvOps;
import org.apache.doris.connector.api.mtmv.MtmvRefreshHint;
import org.apache.doris.connector.api.timetravel.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.mtmv.MTMVMaxTimestampSnapshot;
import org.apache.doris.mtmv.MTMVSnapshotIdSnapshot;
import org.apache.doris.mtmv.MTMVSnapshotIf;
import org.apache.doris.mtmv.MTMVTimestampSnapshot;
import org.apache.doris.mtmv.MTMVVersionSnapshot;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Unit tests for {@link PluginDrivenMtmvBridge}: forwarding to {@link MtmvOps},
 * type conversions, and capability/empty fallback semantics.
 */
public class PluginDrivenMtmvBridgeTest {

    private static final String DB = "db1";
    private static final String TBL = "t1";

    private MtmvOps ops;
    private ConnectorMetadata metadata;
    private Connector connector;
    private PluginDrivenExternalCatalog catalog;
    private PluginDrivenMtmvBridge bridge;

    @BeforeEach
    public void setUp() {
        ops = Mockito.mock(MtmvOps.class);
        metadata = Mockito.mock(ConnectorMetadata.class);
        Mockito.when(metadata.mtmvOps()).thenReturn(Optional.of(ops));
        connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getCapabilities())
                .thenReturn(EnumSet.of(ConnectorCapability.SUPPORTS_MTMV));
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
        catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn("plugin_cat");
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        Mockito.when(catalog.buildConnectorSession()).thenReturn(null);
        bridge = new PluginDrivenMtmvBridge(catalog, DB, TBL);
    }

    private void disableMtmv() {
        Mockito.when(connector.getCapabilities()).thenReturn(EnumSet.noneOf(ConnectorCapability.class));
    }

    private void emptyOps() {
        Mockito.when(metadata.mtmvOps()).thenReturn(Optional.empty());
    }

    // --------------------------- happy-path forwarding ---------------------------

    @Test
    public void getAndCopyPartitionItemsForwardsAndConvertsEmptyMap() {
        Mockito.when(ops.listPartitions(Mockito.eq(ConnectorTableId.of(DB, TBL)), Mockito.any()))
                .thenReturn(Collections.emptyMap());
        Map<String, PartitionItem> out = bridge.getAndCopyPartitionItems(Optional.empty());
        Assertions.assertNotNull(out);
        Assertions.assertTrue(out.isEmpty());
        Mockito.verify(ops).listPartitions(ConnectorTableId.of(DB, TBL), Optional.empty());
    }

    @Test
    public void getAndCopyPartitionItemsSkipsUnpartitionedItems() {
        Map<String, ConnectorPartitionItem> raw = new LinkedHashMap<>();
        raw.put("__SINGLE__", new ConnectorPartitionItem.UnpartitionedItem());
        Mockito.when(ops.listPartitions(Mockito.any(), Mockito.any())).thenReturn(raw);
        Map<String, PartitionItem> out = bridge.getAndCopyPartitionItems(Optional.empty());
        // Unpartitioned items have no fe-core counterpart → filtered out.
        Assertions.assertTrue(out.isEmpty());
    }

    @Test
    public void getAndCopyPartitionItemsRejectsRangeUntilPartitionKeyWired() {
        Map<String, ConnectorPartitionItem> raw = new LinkedHashMap<>();
        raw.put("p1", new ConnectorPartitionItem.RangePartitionItem("0", "10"));
        Mockito.when(ops.listPartitions(Mockito.any(), Mockito.any())).thenReturn(raw);
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> bridge.getAndCopyPartitionItems(Optional.empty()));
    }

    @Test
    public void getAndCopyPartitionItemsRejectsListUntilPartitionKeyWired() {
        Map<String, ConnectorPartitionItem> raw = new LinkedHashMap<>();
        raw.put("p1", new ConnectorPartitionItem.ListPartitionItem(
                Collections.singletonList(Collections.singletonList("us"))));
        Mockito.when(ops.listPartitions(Mockito.any(), Mockito.any())).thenReturn(raw);
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> bridge.getAndCopyPartitionItems(Optional.empty()));
    }

    @Test
    public void getPartitionTypeForwardsAndMapsRange() {
        Mockito.when(ops.getPartitionType(Mockito.eq(ConnectorTableId.of(DB, TBL)), Mockito.any()))
                .thenReturn(ConnectorPartitionType.RANGE);
        Assertions.assertEquals(PartitionType.RANGE, bridge.getPartitionType(Optional.empty()));
        Mockito.verify(ops).getPartitionType(ConnectorTableId.of(DB, TBL), Optional.empty());
    }

    @Test
    public void getPartitionTypeMapsAllEnumValues() {
        Assertions.assertEquals(PartitionType.UNPARTITIONED,
                PluginDrivenMtmvBridge.convertPartitionType(ConnectorPartitionType.UNPARTITIONED));
        Assertions.assertEquals(PartitionType.RANGE,
                PluginDrivenMtmvBridge.convertPartitionType(ConnectorPartitionType.RANGE));
        Assertions.assertEquals(PartitionType.LIST,
                PluginDrivenMtmvBridge.convertPartitionType(ConnectorPartitionType.LIST));
    }

    @Test
    public void getPartitionColumnNamesForwards() {
        Set<String> cols = new java.util.HashSet<>();
        cols.add("dt");
        Mockito.when(ops.getPartitionColumnNames(Mockito.eq(ConnectorTableId.of(DB, TBL)), Mockito.any()))
                .thenReturn(cols);
        Assertions.assertEquals(cols, bridge.getPartitionColumnNames(Optional.empty()));
    }

    @Test
    public void getPartitionColumnsConvertsConnectorColumns() {
        ConnectorColumn cc = new ConnectorColumn(
                "dt", ConnectorType.of("INT"),
                "", true, null, false);
        Mockito.when(ops.getPartitionColumns(Mockito.eq(ConnectorTableId.of(DB, TBL)), Mockito.any()))
                .thenReturn(Collections.singletonList(cc));
        List<Column> cols = bridge.getPartitionColumns(Optional.empty());
        Assertions.assertEquals(1, cols.size());
        Assertions.assertEquals("dt", cols.get(0).getName());
        Assertions.assertEquals(PrimitiveType.INT, cols.get(0).getType().getPrimitiveType());
    }

    @Test
    public void getPartitionSnapshotForwardsHintAndConvertsSnapshotId() {
        Mockito.when(ops.getPartitionSnapshot(Mockito.eq(ConnectorTableId.of(DB, TBL)), Mockito.eq("p1"),
                        Mockito.any(MtmvRefreshHint.class), Mockito.any()))
                .thenReturn(new ConnectorMtmvSnapshot.SnapshotIdMtmvSnapshot(42L));
        MTMVSnapshotIf out = bridge.getPartitionSnapshot("p1", null, Optional.empty());
        Assertions.assertTrue(out instanceof MTMVSnapshotIdSnapshot);
        Assertions.assertEquals(42L, out.getSnapshotVersion());
        ArgumentCaptor<MtmvRefreshHint> hintCap = ArgumentCaptor.forClass(MtmvRefreshHint.class);
        Mockito.verify(ops).getPartitionSnapshot(Mockito.eq(ConnectorTableId.of(DB, TBL)), Mockito.eq("p1"),
                hintCap.capture(), Mockito.any());
        Assertions.assertEquals(Optional.of("p1"), hintCap.getValue().partitionScope());
    }

    @Test
    public void getTableSnapshotWithContextDelegatesAndConvertsTimestamp() {
        Mockito.when(ops.getTableSnapshot(Mockito.eq(ConnectorTableId.of(DB, TBL)),
                        Mockito.any(MtmvRefreshHint.class), Mockito.any()))
                .thenReturn(new ConnectorMtmvSnapshot.TimestampMtmvSnapshot(123L));
        MTMVSnapshotIf out = bridge.getTableSnapshot(null, Optional.empty());
        Assertions.assertTrue(out instanceof MTMVTimestampSnapshot);
        Assertions.assertEquals(123L, out.getSnapshotVersion());
    }

    @Test
    public void getTableSnapshotConvertsVersionSnapshot() {
        Mockito.when(ops.getTableSnapshot(Mockito.any(),
                        Mockito.any(MtmvRefreshHint.class), Mockito.any()))
                .thenReturn(new ConnectorMtmvSnapshot.VersionMtmvSnapshot(7L));
        MTMVSnapshotIf out = bridge.getTableSnapshot(Optional.empty());
        Assertions.assertTrue(out instanceof MTMVVersionSnapshot);
        Assertions.assertEquals(7L, out.getSnapshotVersion());
    }

    @Test
    public void getTableSnapshotConvertsMaxTimestampSnapshot() {
        Mockito.when(ops.getTableSnapshot(Mockito.any(),
                        Mockito.any(MtmvRefreshHint.class), Mockito.any()))
                .thenReturn(new ConnectorMtmvSnapshot.MaxTimestampMtmvSnapshot(99L));
        MTMVSnapshotIf out = bridge.getTableSnapshot(Optional.empty());
        Assertions.assertTrue(out instanceof MTMVMaxTimestampSnapshot);
        Assertions.assertEquals(99L, out.getSnapshotVersion());
    }

    @Test
    public void getNewestUpdateVersionOrTimeForwards() {
        Mockito.when(ops.getNewestUpdateVersionOrTime(ConnectorTableId.of(DB, TBL))).thenReturn(555L);
        Assertions.assertEquals(555L, bridge.getNewestUpdateVersionOrTime());
    }

    @Test
    public void isPartitionColumnAllowNullForwards() {
        Mockito.when(ops.isPartitionColumnAllowNull(ConnectorTableId.of(DB, TBL))).thenReturn(true);
        Assertions.assertTrue(bridge.isPartitionColumnAllowNull());
    }

    @Test
    public void isValidRelatedTableForwards() {
        Mockito.when(ops.isValidRelatedTable(ConnectorTableId.of(DB, TBL))).thenReturn(true);
        Assertions.assertTrue(bridge.isValidRelatedTable());
    }

    @Test
    public void needAutoRefreshForwards() {
        Mockito.when(ops.needAutoRefresh(ConnectorTableId.of(DB, TBL))).thenReturn(true);
        Assertions.assertTrue(bridge.needAutoRefresh());
    }

    // --------------------------- capability fallback ---------------------------

    @Test
    public void noCapabilityThrowsForActiveMethods() {
        disableMtmv();
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> bridge.getAndCopyPartitionItems(Optional.empty()));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> bridge.getPartitionType(Optional.empty()));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> bridge.getPartitionColumnNames(Optional.empty()));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> bridge.getPartitionColumns(Optional.empty()));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> bridge.getPartitionSnapshot("p1", null, Optional.empty()));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> bridge.getTableSnapshot(null, Optional.empty()));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> bridge.getTableSnapshot(Optional.empty()));
        Assertions.assertThrows(UnsupportedOperationException.class,
                bridge::getNewestUpdateVersionOrTime);
        Assertions.assertThrows(UnsupportedOperationException.class,
                bridge::isPartitionColumnAllowNull);
    }

    @Test
    public void noCapabilityFallbacksToFalseForPredicates() {
        disableMtmv();
        Assertions.assertFalse(bridge.isValidRelatedTable());
        Assertions.assertFalse(bridge.needAutoRefresh());
        Mockito.verifyNoInteractions(ops);
    }

    @Test
    public void emptyMtmvOpsThrowsForActiveMethods() {
        emptyOps();
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> bridge.getPartitionType(Optional.empty()));
        Assertions.assertThrows(UnsupportedOperationException.class,
                bridge::getNewestUpdateVersionOrTime);
    }

    @Test
    public void emptyMtmvOpsFallbacksToFalseForPredicates() {
        emptyOps();
        Assertions.assertFalse(bridge.isValidRelatedTable());
        Assertions.assertFalse(bridge.needAutoRefresh());
    }

    // --------------------------- mvcc snapshot conversion ---------------------------

    @Test
    public void mvccSnapshotEmptyMapsToEmpty() {
        Assertions.assertEquals(Optional.empty(),
                PluginDrivenMtmvBridge.toConnectorSnapshot(Optional.empty()));
    }

    @Test
    public void mvccSnapshotPluginDrivenUnwraps() {
        ConnectorMvccSnapshot inner = new TestSnapshot("token-x");
        Optional<ConnectorMvccSnapshot> got = PluginDrivenMtmvBridge.toConnectorSnapshot(
                Optional.of(new PluginDrivenMvccSnapshot(inner)));
        Assertions.assertTrue(got.isPresent());
        Assertions.assertSame(inner, got.get());
    }

    @Test
    public void mvccSnapshotForeignTypeRejected() {
        MvccSnapshot foreign = new MvccSnapshot() { };
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> PluginDrivenMtmvBridge.toConnectorSnapshot(Optional.of(foreign)));
    }

    @Test
    public void mvccSnapshotFlowsThroughForwardCall() {
        ConnectorMvccSnapshot inner = new TestSnapshot("tok");
        Mockito.when(ops.getNewestUpdateVersionOrTime(ConnectorTableId.of(DB, TBL))).thenReturn(1L);
        Mockito.when(ops.getPartitionType(Mockito.eq(ConnectorTableId.of(DB, TBL)),
                        Mockito.eq(Optional.of(inner))))
                .thenReturn(ConnectorPartitionType.LIST);
        Assertions.assertEquals(PartitionType.LIST,
                bridge.getPartitionType(Optional.of(new PluginDrivenMvccSnapshot(inner))));
    }

    // --------------------------- helpers ---------------------------

    private static final class TestSnapshot implements ConnectorMvccSnapshot {
        private final String token;

        TestSnapshot(String token) {
            this.token = token;
        }

        @Override
        public Instant commitTime() {
            return Instant.EPOCH;
        }

        @Override
        public Optional<ConnectorTableVersion> asVersion() {
            return Optional.empty();
        }

        @Override
        public String toOpaqueToken() {
            return token;
        }
    }
}
