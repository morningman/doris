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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRequest;
import org.apache.doris.connector.api.timetravel.ConnectorRefSpec;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;
import org.apache.doris.connector.api.timetravel.RefKind;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Verifies that {@link PaimonScanPlanProvider#planScan(ConnectorScanRequest)}
 * threads engine-side time-travel coordinates
 * ({@link ConnectorScanRequest#getVersion()},
 * {@link ConnectorScanRequest#getRefSpec()}) and
 * {@link ConnectorScanRequest#getLimit()} into the underlying paimon
 * {@link Table#copy(Map)} / {@link ReadBuilder#withLimit(int)} APIs.
 *
 * <p>Mirrors the engine-side time-travel parity covered by
 * {@code IcebergTimeTravelTest} for iceberg. Uses a Mockito-backed
 * {@link Table} so the test exercises only the SPI routing, not the
 * paimon storage layer.
 */
class PaimonTimeTravelTest {

    private Table baseTable;
    private Table pinnedTable;
    private ReadBuilder readBuilder;
    private TableScan scan;
    private PaimonTableHandle handle;
    private PaimonScanPlanProvider provider;

    @BeforeEach
    void setUp() {
        RowType rowType = new RowType(Collections.singletonList(
                new DataField(0, "id", DataTypes.BIGINT())));

        baseTable = Mockito.mock(Table.class);
        pinnedTable = Mockito.mock(Table.class);
        readBuilder = Mockito.mock(ReadBuilder.class, Mockito.RETURNS_SELF);
        scan = Mockito.mock(TableScan.class);
        TableScan.Plan plan = Mockito.mock(TableScan.Plan.class);

        Mockito.when(baseTable.rowType()).thenReturn(rowType);
        Mockito.when(baseTable.partitionKeys()).thenReturn(Collections.emptyList());
        Mockito.when(baseTable.options()).thenReturn(new HashMap<>());
        Mockito.when(baseTable.copy(Mockito.anyMap())).thenReturn(pinnedTable);
        Mockito.when(baseTable.newReadBuilder()).thenReturn(readBuilder);

        Mockito.when(pinnedTable.rowType()).thenReturn(rowType);
        Mockito.when(pinnedTable.partitionKeys()).thenReturn(Collections.emptyList());
        Mockito.when(pinnedTable.options()).thenReturn(new HashMap<>());
        Mockito.when(pinnedTable.newReadBuilder()).thenReturn(readBuilder);

        Mockito.when(readBuilder.newScan()).thenReturn(scan);
        Mockito.when(readBuilder.withFilter(Mockito.<List<Predicate>>any())).thenReturn(readBuilder);
        Mockito.when(readBuilder.withProjection(Mockito.any(int[].class))).thenReturn(readBuilder);
        Mockito.when(readBuilder.withLimit(Mockito.anyInt())).thenReturn(readBuilder);
        Mockito.when(scan.plan()).thenReturn(plan);
        Mockito.when(plan.splits()).thenReturn(new ArrayList<Split>());

        handle = new PaimonTableHandle("db", "t",
                Collections.emptyList(), Collections.emptyList());
        handle.setPaimonTable(baseTable);
        provider = new PaimonScanPlanProvider(new HashMap<>());
    }

    private ConnectorScanRequest baseRequest() {
        return ConnectorScanRequest.from(null, handle,
                Collections.emptyList(), Optional.empty());
    }

    @Test
    void planScanWithoutVersionDoesNotCopyTable() {
        List<ConnectorScanRange> ranges = provider.planScan(baseRequest());
        Assertions.assertTrue(ranges.isEmpty());
        Mockito.verify(baseTable, Mockito.never()).copy(Mockito.anyMap());
    }

    @Test
    void planScanHonoursVersionBySnapshotId() {
        ConnectorScanRequest req = ConnectorScanRequest.builder()
                .session(null)
                .table(handle)
                .columns(Collections.emptyList())
                .filter(Optional.empty())
                .version(Optional.of(new ConnectorTableVersion.BySnapshotId(42L)))
                .build();

        provider.planScan(req);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, String>> opts = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(baseTable).copy(opts.capture());
        Assertions.assertEquals("42",
                opts.getValue().get(CoreOptions.SCAN_SNAPSHOT_ID.key()));
    }

    @Test
    void planScanHonoursRefSpecBranch() {
        ConnectorScanRequest req = ConnectorScanRequest.builder()
                .session(null)
                .table(handle)
                .columns(Collections.emptyList())
                .filter(Optional.empty())
                .refSpec(Optional.of(ConnectorRefSpec.builder()
                        .name("feature-x").kind(RefKind.BRANCH).build()))
                .build();

        provider.planScan(req);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, String>> opts = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(baseTable).copy(opts.capture());
        Assertions.assertEquals("feature-x",
                opts.getValue().get(CoreOptions.BRANCH.key()));
    }

    @Test
    void planScanHonoursRefSpecTag() {
        ConnectorScanRequest req = ConnectorScanRequest.builder()
                .session(null)
                .table(handle)
                .columns(Collections.emptyList())
                .filter(Optional.empty())
                .refSpec(Optional.of(ConnectorRefSpec.builder()
                        .name("v1").kind(RefKind.TAG).build()))
                .build();

        provider.planScan(req);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, String>> opts = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(baseTable).copy(opts.capture());
        Assertions.assertEquals("v1",
                opts.getValue().get(CoreOptions.SCAN_TAG_NAME.key()));
    }

    @Test
    void planScanHonoursLimit() {
        ConnectorScanRequest req = ConnectorScanRequest.builder()
                .session(null)
                .table(handle)
                .columns(Collections.emptyList())
                .filter(Optional.empty())
                .limit(OptionalLong.of(100L))
                .build();

        provider.planScan(req);

        Mockito.verify(readBuilder).withLimit(100);
    }

    @Test
    void planScanIgnoresUnsupportedVersionSubtype() {
        // ByOpaque is intentionally not driven from the scan path; the
        // plugin must not call Table.copy in that case.
        ConnectorScanRequest req = ConnectorScanRequest.builder()
                .session(null)
                .table(handle)
                .columns(Collections.emptyList())
                .filter(Optional.empty())
                .version(Optional.of(new ConnectorTableVersion.ByOpaque("opaque-token")))
                .build();

        provider.planScan(req);
        Mockito.verify(baseTable, Mockito.never()).copy(Mockito.anyMap());
        Mockito.verify(readBuilder, Mockito.never()).withLimit(Mockito.anyInt());
    }

    /**
     * Ensures CoreOptions keys are stable strings — defends against an
     * accidental rename in a future paimon SDK upgrade silently turning
     * time-travel into a no-op.
     */
    @Test
    void coreOptionsKeysMatchExpectedConstants() {
        Options opts = new Options();
        opts.set(CoreOptions.SCAN_SNAPSHOT_ID, 7L);
        opts.set(CoreOptions.BRANCH, "b1");
        opts.set(CoreOptions.SCAN_TAG_NAME, "t1");
        Map<String, String> raw = opts.toMap();
        Assertions.assertEquals(Arrays.asList("7", "b1", "t1"),
                Arrays.asList(
                        raw.get("scan.snapshot-id"),
                        raw.get("branch"),
                        raw.get("scan.tag-name")));
    }
}
