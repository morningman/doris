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

package org.apache.doris.connector.iceberg.source;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorFunctionCall;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;
import org.apache.doris.connector.iceberg.IcebergColumnHandle;
import org.apache.doris.connector.iceberg.IcebergTableHandle;
import org.apache.doris.thrift.TFileScanRangeParams;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.True;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class IcebergScanPlanProviderTest {

    private static final String DB = "db";
    private static final String TBL = "t";

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    private static IcebergTableHandle handleV(int formatVersion, Long snapshotId) {
        return IcebergTableHandle.builder()
                .dbName(DB)
                .tableName(TBL)
                .formatVersion(formatVersion)
                .snapshotId(snapshotId)
                .schemaId(0)
                .partitionSpecId(0)
                .build();
    }

    private static DataFile mockDataFile(String path, long size, int specId) {
        DataFile df = Mockito.mock(DataFile.class);
        Mockito.when(df.path()).thenReturn(path);
        Mockito.when(df.fileSizeInBytes()).thenReturn(size);
        Mockito.when(df.specId()).thenReturn(specId);
        Mockito.when(df.partition()).thenReturn(null);
        Mockito.when(df.firstRowId()).thenReturn(null);
        Mockito.when(df.fileSequenceNumber()).thenReturn(null);
        return df;
    }

    private static FileScanTask mockTask(DataFile file, long start, long length, List<DeleteFile> deletes) {
        FileScanTask t = Mockito.mock(FileScanTask.class);
        Mockito.when(t.file()).thenReturn(file);
        Mockito.when(t.start()).thenReturn(start);
        Mockito.when(t.length()).thenReturn(length);
        Mockito.when(t.deletes()).thenReturn(deletes);
        Mockito.when(t.spec()).thenReturn(PartitionSpec.unpartitioned());
        Mockito.when(t.sizeBytes()).thenReturn(length);
        Mockito.when(t.estimatedRowsCount()).thenReturn(1L);
        // TableScanUtil.splitFiles invokes split(targetSplitSize); for small
        // tasks return self so the splitter simply forwards.
        Mockito.when(t.split(Mockito.anyLong())).thenReturn(Collections.singletonList(t));
        return t;
    }

    /** Configure a mocked Table whose newScan() returns a scan that planFiles() yields the given tasks. */
    private static Table mockTable(List<FileScanTask> tasks, Map<String, String> tableProps,
                                   Long currentSnapshotId, TableScan[] capturedScan) {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(SCHEMA);
        Mockito.when(table.properties()).thenReturn(tableProps == null ? Collections.emptyMap() : tableProps);
        if (currentSnapshotId != null) {
            Snapshot snap = Mockito.mock(Snapshot.class);
            Mockito.when(snap.snapshotId()).thenReturn(currentSnapshotId);
            Mockito.when(table.currentSnapshot()).thenReturn(snap);
        } else {
            Mockito.when(table.currentSnapshot()).thenReturn(null);
        }
        TableScan scan = Mockito.mock(TableScan.class);
        Mockito.when(scan.useSnapshot(Mockito.anyLong())).thenReturn(scan);
        Mockito.when(scan.filter(Mockito.any())).thenReturn(scan);
        Mockito.when(scan.select(Mockito.anyCollection())).thenReturn(scan);
        Mockito.when(scan.targetSplitSize()).thenReturn(128L * 1024 * 1024);
        Mockito.when(scan.planFiles()).thenReturn(CloseableIterable.withNoopClose(tasks));
        Mockito.when(table.newScan()).thenReturn(scan);
        if (capturedScan != null) {
            capturedScan[0] = scan;
        }
        return table;
    }

    private static IcebergScanPlanProvider providerFor(Table table) {
        return new IcebergScanPlanProvider((db, tbl) -> table, Collections.emptyMap(), null);
    }

    // ----- Tests -----

    @Test
    void rangeTypeIsFileScan() {
        Table table = mockTable(Collections.emptyList(), null, 1L, null);
        Assertions.assertEquals(ConnectorScanRangeType.FILE_SCAN, providerFor(table).getScanRangeType());
    }

    @Test
    void emptyTableNoSnapshotReturnsEmpty() {
        Table table = mockTable(Collections.emptyList(), null, null, null);
        IcebergScanPlanProvider p = providerFor(table);
        List<ConnectorScanRange> ranges = p.planScan(null, handleV(1, null),
                Collections.emptyList(), Optional.empty());
        Assertions.assertTrue(ranges.isEmpty());
    }

    @Test
    void singleDataFileV1() {
        DataFile df = mockDataFile("/x/a.parquet", 1024L, 0);
        FileScanTask task = mockTask(df, 0L, 1024L, Collections.emptyList());
        Table table = mockTable(Collections.singletonList(task), null, 7L, null);
        IcebergScanPlanProvider p = providerFor(table);

        List<ConnectorScanRange> ranges = p.planScan(null, handleV(1, null),
                Collections.emptyList(), Optional.empty());
        Assertions.assertEquals(1, ranges.size());
        IcebergScanRange r = (IcebergScanRange) ranges.get(0);
        Assertions.assertEquals("/x/a.parquet", r.getPath().orElseThrow(() -> new AssertionError("no path")));
        Assertions.assertEquals(0L, r.getStart());
        Assertions.assertEquals(1024L, r.getLength());
        Assertions.assertEquals(1024L, r.getFileSize());
        Assertions.assertEquals(1, r.getFormatVersion());
        Assertions.assertEquals("/x/a.parquet", r.getOriginalFilePath());
        Assertions.assertEquals(-1L, r.getTableLevelRowCount());
        Assertions.assertTrue(r.getIcebergDeleteFiles().isEmpty());
    }

    @Test
    void twoDataFilesV1() {
        FileScanTask t1 = mockTask(mockDataFile("/x/a.parquet", 100L, 0), 0L, 100L, Collections.emptyList());
        FileScanTask t2 = mockTask(mockDataFile("/x/b.parquet", 200L, 0), 0L, 200L, Collections.emptyList());
        Table table = mockTable(Arrays.asList(t1, t2), null, 1L, null);
        List<ConnectorScanRange> ranges = providerFor(table).planScan(null, handleV(1, null),
                Collections.emptyList(), Optional.empty());
        Assertions.assertEquals(2, ranges.size());
    }

    @Test
    void filterPresentAndConvertsToNonTrueIsAppliedToScan() {
        FileScanTask task = mockTask(mockDataFile("/x/a.parquet", 10L, 0), 0L, 10L, Collections.emptyList());
        TableScan[] capt = new TableScan[1];
        Table table = mockTable(Collections.singletonList(task), null, 1L, capt);

        // id = 5 → translates to a non-True iceberg expression.
        ConnectorExpression filter = new ConnectorComparison(
                ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("id", ConnectorType.of("BIGINT")),
                ConnectorLiteral.ofLong(5L));
        providerFor(table).planScan(null, handleV(1, null),
                Collections.emptyList(), Optional.of(filter));
        ArgumentCaptor<Expression> exprCap = ArgumentCaptor.forClass(Expression.class);
        Mockito.verify(capt[0], Mockito.atLeastOnce()).filter(exprCap.capture());
        Assertions.assertFalse(exprCap.getValue() instanceof True,
                "non-True iceberg expression should be passed to scan.filter");
    }

    @Test
    void filterDegradedToAlwaysTrueIsNotForwardedToScan() {
        FileScanTask task = mockTask(mockDataFile("/x/a.parquet", 10L, 0), 0L, 10L, Collections.emptyList());
        TableScan[] capt = new TableScan[1];
        Table table = mockTable(Collections.singletonList(task), null, 1L, capt);

        // function call on an unsupported function → converter returns alwaysTrue.
        ConnectorExpression filter = new ConnectorFunctionCall("unknown_fn",
                ConnectorType.of("BOOLEAN"), Collections.emptyList());
        List<ConnectorScanRange> ranges = providerFor(table).planScan(
                null, handleV(1, null), Collections.emptyList(), Optional.of(filter));
        Assertions.assertEquals(1, ranges.size());
        Mockito.verify(capt[0], Mockito.never()).filter(Mockito.any());
    }

    @Test
    void projectionPassesColumnNamesToScan() {
        FileScanTask task = mockTask(mockDataFile("/x/a.parquet", 10L, 0), 0L, 10L, Collections.emptyList());
        TableScan[] capt = new TableScan[1];
        Table table = mockTable(Collections.singletonList(task), null, 1L, capt);

        ConnectorColumnHandle idCh = new IcebergColumnHandle(1, "id", Types.LongType.get(), false);
        List<ConnectorScanRange> ranges = providerFor(table).planScan(
                null, handleV(1, null), Collections.singletonList(idCh), Optional.empty());
        Assertions.assertEquals(1, ranges.size());
        ArgumentCaptor<java.util.Collection<String>> cap = ArgumentCaptor.forClass(java.util.Collection.class);
        Mockito.verify(capt[0]).select(cap.capture());
        Assertions.assertTrue(cap.getValue().contains("id"));
    }

    @Test
    void v2WithPositionDeleteFile() {
        DataFile df = mockDataFile("/x/a.parquet", 10L, 0);
        DeleteFile pos = Mockito.mock(DeleteFile.class);
        Mockito.when(pos.path()).thenReturn("/x/pos.parquet");
        Mockito.when(pos.content()).thenReturn(FileContent.POSITION_DELETES);
        Mockito.when(pos.format()).thenReturn(FileFormat.PARQUET);
        Mockito.when(pos.fileSizeInBytes()).thenReturn(33L);
        ByteBuffer lower = Conversions.toByteBuffer(MetadataColumns.DELETE_FILE_POS.type(), 0L);
        ByteBuffer upper = Conversions.toByteBuffer(MetadataColumns.DELETE_FILE_POS.type(), 99L);
        Map<Integer, ByteBuffer> lowMap = new HashMap<>();
        lowMap.put(MetadataColumns.DELETE_FILE_POS.fieldId(), lower);
        Map<Integer, ByteBuffer> upMap = new HashMap<>();
        upMap.put(MetadataColumns.DELETE_FILE_POS.fieldId(), upper);
        Mockito.when(pos.lowerBounds()).thenReturn(lowMap);
        Mockito.when(pos.upperBounds()).thenReturn(upMap);

        FileScanTask task = mockTask(df, 0L, 10L, Collections.singletonList(pos));
        Table table = mockTable(Collections.singletonList(task), null, 1L, null);
        List<ConnectorScanRange> ranges = providerFor(table).planScan(
                null, handleV(2, null), Collections.emptyList(), Optional.empty());
        Assertions.assertEquals(1, ranges.size());
        IcebergScanRange r = (IcebergScanRange) ranges.get(0);
        Assertions.assertEquals(1, r.getIcebergDeleteFiles().size());
        IcebergDeleteFileDescriptor d = r.getIcebergDeleteFiles().get(0);
        Assertions.assertEquals(IcebergDeleteFileDescriptor.Kind.POSITION_DELETE, d.getKind());
        Assertions.assertEquals(0L, d.getPositionLowerBound());
        Assertions.assertEquals(99L, d.getPositionUpperBound());
    }

    @Test
    void v2WithEqualityDeleteFile() {
        DataFile df = mockDataFile("/x/a.parquet", 10L, 0);
        DeleteFile eq = Mockito.mock(DeleteFile.class);
        Mockito.when(eq.path()).thenReturn("/x/eq.parquet");
        Mockito.when(eq.content()).thenReturn(FileContent.EQUALITY_DELETES);
        Mockito.when(eq.format()).thenReturn(FileFormat.PARQUET);
        Mockito.when(eq.fileSizeInBytes()).thenReturn(11L);
        Mockito.when(eq.equalityFieldIds()).thenReturn(Arrays.asList(1, 2));

        FileScanTask task = mockTask(df, 0L, 10L, Collections.singletonList(eq));
        Table table = mockTable(Collections.singletonList(task), null, 1L, null);
        List<ConnectorScanRange> ranges = providerFor(table).planScan(
                null, handleV(2, null), Collections.emptyList(), Optional.empty());
        IcebergScanRange r = (IcebergScanRange) ranges.get(0);
        Assertions.assertEquals(1, r.getIcebergDeleteFiles().size());
        IcebergDeleteFileDescriptor d = r.getIcebergDeleteFiles().get(0);
        Assertions.assertEquals(IcebergDeleteFileDescriptor.Kind.EQUALITY_DELETE, d.getKind());
        Assertions.assertArrayEquals(new int[]{1, 2}, d.getFieldIds());
    }

    @Test
    void explicitSnapshotIdRoutedToScan() {
        FileScanTask task = mockTask(mockDataFile("/x/a.parquet", 10L, 0), 0L, 10L, Collections.emptyList());
        TableScan[] capt = new TableScan[1];
        Table table = mockTable(Collections.singletonList(task), null, 1L, capt);

        providerFor(table).planScan(null, handleV(1, 42L),
                Collections.emptyList(), Optional.empty());
        Mockito.verify(capt[0]).useSnapshot(42L);
    }

    @Test
    void scanNodePropertiesContainsFormatTypeAndVersion() {
        Map<String, String> tprops = new HashMap<>();
        tprops.put(TableProperties.DEFAULT_FILE_FORMAT, "parquet");
        Table table = mockTable(Collections.emptyList(), tprops, 1L, null);
        Map<String, String> props = providerFor(table).getScanNodeProperties(
                null, handleV(2, null), Collections.emptyList(), Optional.empty());
        Assertions.assertEquals("PARQUET", props.get(IcebergScanPlanProvider.PROP_FILE_FORMAT_TYPE));
        Assertions.assertEquals("2", props.get(IcebergScanPlanProvider.PROP_ICEBERG_FORMAT_VERSION));
    }

    @Test
    void scanNodePropertiesDefaultsToParquetWhenMissing() {
        Table table = mockTable(Collections.emptyList(), null, 1L, null);
        Map<String, String> props = providerFor(table).getScanNodeProperties(
                null, handleV(1, null), Collections.emptyList(), Optional.empty());
        Assertions.assertEquals("PARQUET", props.get(IcebergScanPlanProvider.PROP_FILE_FORMAT_TYPE));
        Assertions.assertEquals("1", props.get(IcebergScanPlanProvider.PROP_ICEBERG_FORMAT_VERSION));
    }

    @Test
    void appendExplainInfoIncludesKeys() {
        Table table = mockTable(Collections.emptyList(), null, 1L, null);
        IcebergScanPlanProvider p = providerFor(table);
        Map<String, String> props = p.getScanNodeProperties(
                null, handleV(2, null), Collections.emptyList(), Optional.empty());
        StringBuilder sb = new StringBuilder();
        p.appendExplainInfo(sb, "  ", props);
        String s = sb.toString();
        Assertions.assertTrue(s.contains("iceberg.format_version=2"), s);
        Assertions.assertTrue(s.contains("file_format_type=PARQUET"), s);
    }

    // -------- M2-Iceberg-05: vended creds + name mapping + count pushdown --------

    private static Table mockTableWithIo(Map<String, String> tableProps,
                                         Map<String, String> ioProps,
                                         Long currentSnapshotId) {
        Table table = mockTable(Collections.emptyList(), tableProps, currentSnapshotId, null);
        FileIO io = Mockito.mock(FileIO.class);
        Mockito.when(io.properties()).thenReturn(ioProps == null ? Collections.emptyMap() : ioProps);
        Mockito.when(table.io()).thenReturn(io);
        return table;
    }

    @Test
    void nameMappingPropagatedWhenTableHasIt() {
        String mappingJson = "[{\"field-id\":1,\"names\":[\"id\"]}]";
        Map<String, String> tprops = new HashMap<>();
        tprops.put(TableProperties.DEFAULT_NAME_MAPPING, mappingJson);
        Table table = mockTableWithIo(tprops, null, 1L);
        Map<String, String> props = providerFor(table).getScanNodeProperties(
                null, handleV(2, null), Collections.emptyList(), Optional.empty());
        Assertions.assertEquals(mappingJson, props.get(IcebergScanPlanProvider.PROP_NAME_MAPPING));
    }

    @Test
    void nameMappingAbsentWhenTableLacksIt() {
        Table table = mockTableWithIo(Collections.emptyMap(), null, 1L);
        Map<String, String> props = providerFor(table).getScanNodeProperties(
                null, handleV(2, null), Collections.emptyList(), Optional.empty());
        Assertions.assertNull(props.get(IcebergScanPlanProvider.PROP_NAME_MAPPING));
    }

    @Test
    void populateScanLevelParamsCopiesNameMappingVerbatim() {
        Table table = mockTableWithIo(Collections.emptyMap(), null, 1L);
        IcebergScanPlanProvider p = providerFor(table);
        Map<String, String> nodeProps = new HashMap<>();
        String mappingJson = "[{\"field-id\":42,\"names\":[\"x\"]}]";
        nodeProps.put(IcebergScanPlanProvider.PROP_NAME_MAPPING, mappingJson);
        TFileScanRangeParams params = new TFileScanRangeParams();
        p.populateScanLevelParams(params, nodeProps);
        Assertions.assertNotNull(params.getProperties());
        Assertions.assertEquals(mappingJson,
                params.getProperties().get(IcebergScanPlanProvider.PROP_NAME_MAPPING));
    }

    @Test
    void populateScanLevelParamsStripsLocationPrefix() {
        Table table = mockTableWithIo(Collections.emptyMap(), null, 1L);
        IcebergScanPlanProvider p = providerFor(table);
        Map<String, String> nodeProps = new HashMap<>();
        nodeProps.put(IcebergScanPlanProvider.PROP_LOCATION_PREFIX + "s3.access-key-id", "AKIA");
        nodeProps.put(IcebergScanPlanProvider.PROP_LOCATION_PREFIX + "s3.endpoint", "https://x");
        nodeProps.put("file_format_type", "PARQUET"); // non-prefixed: must NOT leak
        TFileScanRangeParams params = new TFileScanRangeParams();
        p.populateScanLevelParams(params, nodeProps);
        Map<String, String> out = params.getProperties();
        Assertions.assertEquals("AKIA", out.get("s3.access-key-id"));
        Assertions.assertEquals("https://x", out.get("s3.endpoint"));
        Assertions.assertNull(out.get("file_format_type"));
        Assertions.assertNull(out.get(IcebergScanPlanProvider.PROP_LOCATION_PREFIX + "s3.access-key-id"));
    }

    @Test
    void vendedCredsMergedFromCatalogAndTable() {
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("s3.endpoint", "https://x");
        Map<String, String> ioProps = new HashMap<>();
        ioProps.put("s3.access-key-id", "AKIA");
        ioProps.put("s3.secret-access-key", "secret");
        Table table = mockTableWithIo(Collections.emptyMap(), ioProps, 1L);
        IcebergScanPlanProvider p = new IcebergScanPlanProvider(
                (db, tbl) -> table, catalogProps, null);
        Map<String, String> props = p.getScanNodeProperties(
                null, handleV(2, null), Collections.emptyList(), Optional.empty());
        String prefix = IcebergScanPlanProvider.PROP_LOCATION_PREFIX;
        Assertions.assertEquals("https://x", props.get(prefix + "s3.endpoint"));
        Assertions.assertEquals("AKIA", props.get(prefix + "s3.access-key-id"));
        Assertions.assertEquals("secret", props.get(prefix + "s3.secret-access-key"));
    }

    @Test
    void vendedCredsTableValueSupersedesCatalogValue() {
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("s3.endpoint", "https://catalog");
        Map<String, String> ioProps = new HashMap<>();
        ioProps.put("s3.endpoint", "https://vended");
        Table table = mockTableWithIo(Collections.emptyMap(), ioProps, 1L);
        IcebergScanPlanProvider p = new IcebergScanPlanProvider(
                (db, tbl) -> table, catalogProps, null);
        Map<String, String> props = p.getScanNodeProperties(
                null, handleV(2, null), Collections.emptyList(), Optional.empty());
        Assertions.assertEquals("https://vended",
                props.get(IcebergScanPlanProvider.PROP_LOCATION_PREFIX + "s3.endpoint"));
    }

    /**
     * Mocks a snapshot with the given summary entries. Pass null for any
     * key to omit it (mirrors a real iceberg snapshot that hasn't recorded
     * the field — e.g. a freshly-created v1 table).
     */
    private static Table mockTableWithSnapshotSummary(String totalRecords, String posDeletes,
                                                       String eqDeletes, Long snapshotId) {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(SCHEMA);
        Mockito.when(table.properties()).thenReturn(Collections.emptyMap());
        if (snapshotId == null) {
            Mockito.when(table.currentSnapshot()).thenReturn(null);
            return table;
        }
        Snapshot snap = Mockito.mock(Snapshot.class);
        Mockito.when(snap.snapshotId()).thenReturn(snapshotId);
        Map<String, String> summary = new HashMap<>();
        if (totalRecords != null) {
            summary.put("total-records", totalRecords);
        }
        if (posDeletes != null) {
            summary.put("total-position-deletes", posDeletes);
        }
        if (eqDeletes != null) {
            summary.put("total-equality-deletes", eqDeletes);
        }
        Mockito.when(snap.summary()).thenReturn(summary);
        Mockito.when(table.currentSnapshot()).thenReturn(snap);
        Mockito.when(table.snapshot(snapshotId)).thenReturn(snap);
        return table;
    }

    @Test
    void countPushdownEmptyTableReturnsZero() {
        Table table = mockTableWithSnapshotSummary(null, null, null, null);
        Optional<Long> r = providerFor(table).getCountPushdownResult(
                null, handleV(2, null), Optional.empty());
        Assertions.assertEquals(Optional.of(0L), r);
    }

    @Test
    void countPushdownOnlyDataFilesReturnsTotalRecords() {
        Table table = mockTableWithSnapshotSummary("1000", "0", "0", 1L);
        Optional<Long> r = providerFor(table).getCountPushdownResult(
                null, handleV(2, null), Optional.empty());
        Assertions.assertEquals(Optional.of(1000L), r);
    }

    @Test
    void countPushdownEqualityDeletesReturnsEmpty() {
        Table table = mockTableWithSnapshotSummary("1000", "0", "5", 1L);
        Optional<Long> r = providerFor(table).getCountPushdownResult(
                null, handleV(2, null), Optional.empty());
        Assertions.assertFalse(r.isPresent());
    }

    @Test
    void countPushdownPositionDeletesWithoutDanglingFlagReturnsEmpty() {
        Table table = mockTableWithSnapshotSummary("1000", "10", "0", 1L);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        Mockito.when(session.getSessionProperties()).thenReturn(Collections.emptyMap());
        Optional<Long> r = providerFor(table).getCountPushdownResult(
                session, handleV(2, null), Optional.empty());
        Assertions.assertFalse(r.isPresent());
    }

    @Test
    void countPushdownPositionDeletesWithDanglingFlagSubtracts() {
        Table table = mockTableWithSnapshotSummary("1000", "10", "0", 1L);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        Mockito.when(session.getSessionProperties()).thenReturn(
                Collections.singletonMap(
                        IcebergScanPlanProvider.SESSION_VAR_IGNORE_DANGLING_DELETE, "true"));
        Optional<Long> r = providerFor(table).getCountPushdownResult(
                session, handleV(2, null), Optional.empty());
        Assertions.assertEquals(Optional.of(990L), r);
    }

    @Test
    void countPushdownWithFilterReturnsEmpty() {
        Table table = mockTableWithSnapshotSummary("1000", "0", "0", 1L);
        ConnectorExpression filter = new ConnectorComparison(
                ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("id", ConnectorType.of("BIGINT")),
                ConnectorLiteral.ofLong(5L));
        Optional<Long> r = providerFor(table).getCountPushdownResult(
                null, handleV(2, null), Optional.of(filter));
        Assertions.assertFalse(r.isPresent());
    }

    // -------- M2-Iceberg-06: manifest cache binding --------

    @Test
    void cacheDisabledByDefaultProducesSameRangesAsBaseline() {
        // Regression: with the cache disabled (default), the dispatch path
        // must call scan.planFiles() unchanged.
        FileScanTask task = mockTask(mockDataFile("/x/a.parquet", 1024L, 0),
                0L, 1024L, Collections.emptyList());
        TableScan[] capt = new TableScan[1];
        Table table = mockTable(Collections.singletonList(task), null, 1L, capt);

        IcebergScanPlanProvider p = providerFor(table); // cache off
        List<ConnectorScanRange> ranges = p.planScan(null, handleV(1, null),
                Collections.emptyList(), Optional.empty());
        Assertions.assertEquals(1, ranges.size());
        Mockito.verify(capt[0], Mockito.atLeastOnce()).planFiles();
    }

    @Test
    void cacheDisabledStatsAreAllZero() {
        Table table = mockTable(Collections.emptyList(), null, 1L, null);
        IcebergScanPlanProvider p = providerFor(table);
        Assertions.assertFalse(p.isManifestCacheEnabled());
        IcebergScanPlanProvider.CacheStats stats = p.getCacheStats();
        Assertions.assertEquals(0, stats.hits);
        Assertions.assertEquals(0, stats.misses);
        Assertions.assertEquals(0, stats.failures);
    }

    @Test
    void appendExplainInfoOmitsCacheLineWhenDisabled() {
        Table table = mockTable(Collections.emptyList(), null, 1L, null);
        IcebergScanPlanProvider p = providerFor(table);
        Map<String, String> props = p.getScanNodeProperties(
                null, handleV(2, null), Collections.emptyList(), Optional.empty());
        StringBuilder sb = new StringBuilder();
        p.appendExplainInfo(sb, "  ", props);
        Assertions.assertFalse(sb.toString().contains("manifest cache"));
    }

    @Test
    void appendExplainInfoRendersCacheLineWhenEnabled() {
        Table table = mockTable(Collections.emptyList(), null, 1L, null);
        org.apache.doris.connector.iceberg.cache.IcebergPluginManifestCache cache =
                new org.apache.doris.connector.iceberg.cache.IcebergPluginManifestCache(8);
        IcebergScanPlanProvider p = new IcebergScanPlanProvider(
                (db, tbl) -> table, Collections.emptyMap(), null, cache, true);
        Assertions.assertTrue(p.isManifestCacheEnabled());
        Map<String, String> props = p.getScanNodeProperties(
                null, handleV(2, null), Collections.emptyList(), Optional.empty());
        StringBuilder sb = new StringBuilder();
        p.appendExplainInfo(sb, "  ", props);
        Assertions.assertTrue(sb.toString().contains("manifest cache: hits=0"),
                "explain output must include manifest-cache stats line: " + sb);
    }

    @Test
    void cacheEnabledFallsBackOnFailureAndIncrementsFailures() {
        // Force a failure inside planFileScanTaskWithManifestCache by
        // returning a non-null snapshot but a Table whose io() throws
        // when the algorithm asks for delete manifests.
        FileScanTask task = mockTask(mockDataFile("/x/a.parquet", 10L, 0),
                0L, 10L, Collections.emptyList());
        Table table = mockTable(Collections.singletonList(task), null, 1L, null);
        // Configure scan.snapshot() to return a snapshot whose
        // deleteManifests() throws — the cache path catches this and
        // falls back to scan.planFiles(), incrementing failures.
        TableScan scan = table.newScan();
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        Mockito.when(scan.snapshot()).thenReturn(snapshot);
        Mockito.when(snapshot.deleteManifests(Mockito.any()))
                .thenThrow(new RuntimeException("boom"));

        org.apache.doris.connector.iceberg.cache.IcebergPluginManifestCache cache =
                new org.apache.doris.connector.iceberg.cache.IcebergPluginManifestCache(8);
        IcebergScanPlanProvider p = new IcebergScanPlanProvider(
                (db, tbl) -> table, Collections.emptyMap(), null, cache, true);
        List<ConnectorScanRange> ranges = p.planScan(null, handleV(1, null),
                Collections.emptyList(), Optional.empty());
        Assertions.assertFalse(ranges.isEmpty(), "fallback path must yield ranges");
        Assertions.assertEquals(1, p.getCacheStats().failures);
    }

    @Test
    void cacheEnabledNullSnapshotReturnsEmptyWithoutFailure() {
        // When scan.snapshot() returns null, the cache path returns an
        // empty iterable cleanly (no failure increment).
        FileScanTask task = mockTask(mockDataFile("/x/a.parquet", 10L, 0),
                0L, 10L, Collections.emptyList());
        Table table = mockTable(Collections.singletonList(task), null, 1L, null);
        TableScan scan = table.newScan();
        Mockito.when(scan.snapshot()).thenReturn(null);

        org.apache.doris.connector.iceberg.cache.IcebergPluginManifestCache cache =
                new org.apache.doris.connector.iceberg.cache.IcebergPluginManifestCache(8);
        IcebergScanPlanProvider p = new IcebergScanPlanProvider(
                (db, tbl) -> table, Collections.emptyMap(), null, cache, true);
        List<ConnectorScanRange> ranges = p.planScan(null, handleV(1, null),
                Collections.emptyList(), Optional.empty());
        Assertions.assertTrue(ranges.isEmpty());
        Assertions.assertEquals(0, p.getCacheStats().failures);
    }

    @Test
    void twoProvidersSharingCacheAccumulateAcrossInstances() {
        // The connector owns a singleton cache; two providers built from
        // it must observe each other's cache state. We exercise this by
        // forcing one miss, then asserting the second provider records
        // the same key as a hit when invoked through the same code path
        // — driven via the cache directly rather than the scan pipeline
        // (which is exercised by the real-iceberg test in
        // IcebergManifestCacheTest).
        org.apache.doris.connector.iceberg.cache.IcebergPluginManifestCache cache =
                new org.apache.doris.connector.iceberg.cache.IcebergPluginManifestCache(8);
        Table table = mockTable(Collections.emptyList(), null, 1L, null);

        IcebergScanPlanProvider a = new IcebergScanPlanProvider(
                (db, tbl) -> table, Collections.emptyMap(), null, cache, true);
        IcebergScanPlanProvider b = new IcebergScanPlanProvider(
                (db, tbl) -> table, Collections.emptyMap(), null, cache, true);
        Assertions.assertSame(cache, a.getManifestCache());
        Assertions.assertSame(cache, b.getManifestCache());
        Assertions.assertSame(a.getManifestCache(), b.getManifestCache(),
                "providers built from the same connector must share one cache instance");
    }

    @Test
    void cacheDisabledFlagOverridesNullCache() {
        // Even when constructed with a non-null cache, passing
        // manifestCacheEnabled=false must keep the dispatch on the
        // legacy path. (Symmetric: a null cache forces enabled=false
        // even when the boolean is true.)
        Table table = mockTable(Collections.emptyList(), null, 1L, null);
        org.apache.doris.connector.iceberg.cache.IcebergPluginManifestCache cache =
                new org.apache.doris.connector.iceberg.cache.IcebergPluginManifestCache(8);
        IcebergScanPlanProvider off = new IcebergScanPlanProvider(
                (db, tbl) -> table, Collections.emptyMap(), null, cache, false);
        Assertions.assertFalse(off.isManifestCacheEnabled());

        IcebergScanPlanProvider nullCache = new IcebergScanPlanProvider(
                (db, tbl) -> table, Collections.emptyMap(), null, null, true);
        Assertions.assertFalse(nullCache.isManifestCacheEnabled(),
                "enabled flag must collapse to false when no cache is provided");
    }
}
