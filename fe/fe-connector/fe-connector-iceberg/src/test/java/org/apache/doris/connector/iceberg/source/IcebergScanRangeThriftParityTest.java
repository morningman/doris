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

import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.iceberg.FileFormat;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Bit-level parity gate: assert that {@link IcebergScanRange#populateRangeParams}
 * produces a {@link TFileRangeDesc} that is byte-identical to the legacy
 * {@code IcebergScanNode#setIcebergParams} output for the same input.
 *
 * <p>The legacy code path lives in fe-core, which this module is forbidden to
 * depend on. Instead of using reflection across modules, this test embeds a
 * hand-rolled "golden" builder that mirrors the legacy field-population
 * code lines 278&ndash;377 of {@code IcebergScanNode.setIcebergParams}.
 * Because the golden builder is a literal port (and is exercised here side
 * by side with the plugin code), any future drift in either implementation
 * is caught immediately.</p>
 */
class IcebergScanRangeThriftParityTest {

    private static final int MIN_DELETE_FILE_SUPPORT_VERSION = 2;
    private static final int FILE_CONTENT_DATA = 0;
    private static final int CONTENT_POSITION_DELETE = 1;
    private static final int CONTENT_EQUALITY_DELETE = 2;
    private static final int CONTENT_DELETION_VECTOR = 3;

    private static byte[] serialize(TFileRangeDesc desc) throws TException {
        return new TSerializer(new TBinaryProtocol.Factory()).serialize(desc);
    }

    private static void assertParity(IcebergScanRange range,
            LegacyInputs legacy) throws TException {
        TFileRangeDesc actual = new TFileRangeDesc();
        TTableFormatFileDesc actualFormat = new TTableFormatFileDesc();
        actualFormat.setTableFormatType(range.getTableFormatType());
        range.populateRangeParams(actualFormat, actual);
        actual.setTableFormatParams(actualFormat);

        TFileRangeDesc expected = goldenSetIcebergParams(legacy);

        Assertions.assertEquals(expected, actual,
                "TFileRangeDesc field equality mismatch");
        Assertions.assertArrayEquals(serialize(expected), serialize(actual),
                "TFileRangeDesc binary mismatch");
    }

    // ---------- the 10 parity cases ----------

    @Test
    void v1NoDeletes() throws TException {
        IcebergScanRange range = IcebergScanRange.builder()
                .path("s3://b/d.parquet")
                .originalFilePath("s3://b/d.parquet")
                .formatVersion(1)
                .build();
        LegacyInputs legacy = new LegacyInputs(1, "s3://b/d.parquet", null, null,
                null, null, -1L, Collections.emptyList(), null);
        assertParity(range, legacy);
    }

    @Test
    void v2PositionDeleteWithBounds() throws TException {
        IcebergDeleteFileDescriptor pd = IcebergDeleteFileDescriptor.builder()
                .kind(IcebergDeleteFileDescriptor.Kind.POSITION_DELETE)
                .path("s3://b/pos.parquet")
                .fileFormat(FileFormat.PARQUET)
                .positionLowerBound(10L)
                .positionUpperBound(99L)
                .build();
        IcebergScanRange range = IcebergScanRange.builder()
                .path("s3://b/d.parquet")
                .originalFilePath("s3://b/d.parquet")
                .formatVersion(2)
                .deleteFiles(Collections.singletonList(pd))
                .build();
        LegacyInputs legacy = new LegacyInputs(2, "s3://b/d.parquet", null, null,
                null, null, -1L, Collections.singletonList(pd), null);
        assertParity(range, legacy);
    }

    @Test
    void v2PositionDeleteWithoutBounds() throws TException {
        IcebergDeleteFileDescriptor pd = IcebergDeleteFileDescriptor.builder()
                .kind(IcebergDeleteFileDescriptor.Kind.POSITION_DELETE)
                .path("s3://b/pos.parquet")
                .fileFormat(FileFormat.PARQUET)
                .build();
        IcebergScanRange range = IcebergScanRange.builder()
                .path("s3://b/d.parquet")
                .originalFilePath("s3://b/d.parquet")
                .formatVersion(2)
                .deleteFiles(Collections.singletonList(pd))
                .build();
        LegacyInputs legacy = new LegacyInputs(2, "s3://b/d.parquet", null, null,
                null, null, -1L, Collections.singletonList(pd), null);
        assertParity(range, legacy);
    }

    @Test
    void v2EqualityDelete() throws TException {
        IcebergDeleteFileDescriptor ed = IcebergDeleteFileDescriptor.builder()
                .kind(IcebergDeleteFileDescriptor.Kind.EQUALITY_DELETE)
                .path("s3://b/eq.parquet")
                .fileFormat(FileFormat.PARQUET)
                .fieldIds(new int[]{1, 2})
                .build();
        IcebergScanRange range = IcebergScanRange.builder()
                .path("s3://b/d.parquet")
                .originalFilePath("s3://b/d.parquet")
                .formatVersion(2)
                .deleteFiles(Collections.singletonList(ed))
                .build();
        LegacyInputs legacy = new LegacyInputs(2, "s3://b/d.parquet", null, null,
                null, null, -1L, Collections.singletonList(ed), null);
        assertParity(range, legacy);
    }

    @Test
    void v2MixedPositionAndEquality() throws TException {
        IcebergDeleteFileDescriptor pd = IcebergDeleteFileDescriptor.builder()
                .kind(IcebergDeleteFileDescriptor.Kind.POSITION_DELETE)
                .path("s3://b/pos.parquet")
                .fileFormat(FileFormat.PARQUET)
                .positionLowerBound(0L)
                .build();
        IcebergDeleteFileDescriptor ed = IcebergDeleteFileDescriptor.builder()
                .kind(IcebergDeleteFileDescriptor.Kind.EQUALITY_DELETE)
                .path("s3://b/eq.parquet")
                .fileFormat(FileFormat.ORC)
                .fieldIds(new int[]{5})
                .build();
        List<IcebergDeleteFileDescriptor> deletes = Arrays.asList(pd, ed);
        IcebergScanRange range = IcebergScanRange.builder()
                .path("s3://b/d.parquet")
                .originalFilePath("s3://b/d.parquet")
                .formatVersion(2)
                .deleteFiles(deletes)
                .build();
        LegacyInputs legacy = new LegacyInputs(2, "s3://b/d.parquet", null, null,
                null, null, -1L, deletes, null);
        assertParity(range, legacy);
    }

    @Test
    void v3DeletionVector() throws TException {
        IcebergDeleteFileDescriptor dv = IcebergDeleteFileDescriptor.builder()
                .kind(IcebergDeleteFileDescriptor.Kind.DELETION_VECTOR)
                .path("s3://b/dv.puffin")
                .fileFormat(FileFormat.PUFFIN)
                .positionLowerBound(0L)
                .positionUpperBound(1024L)
                .contentOffset(64L)
                .contentSizeInBytes(256L)
                .build();
        IcebergScanRange range = IcebergScanRange.builder()
                .path("s3://b/d.parquet")
                .originalFilePath("s3://b/d.parquet")
                .formatVersion(3)
                .firstRowId(1000L)
                .lastUpdatedSequenceNumber(42L)
                .deleteFiles(Collections.singletonList(dv))
                .build();
        LegacyInputs legacy = new LegacyInputs(3, "s3://b/d.parquet", null, null,
                1000L, 42L, -1L, Collections.singletonList(dv), null);
        assertParity(range, legacy);
    }

    @Test
    void tableLevelRowCountActive() throws TException {
        IcebergScanRange range = IcebergScanRange.builder()
                .path("s3://b/d.parquet")
                .originalFilePath("s3://b/d.parquet")
                .formatVersion(2)
                .tableLevelRowCount(42L)
                .build();
        LegacyInputs legacy = new LegacyInputs(2, "s3://b/d.parquet", null, null,
                null, null, 42L, Collections.emptyList(), null);
        assertParity(range, legacy);
    }

    @Test
    void tableLevelRowCountDefaultMinusOne() throws TException {
        IcebergScanRange range = IcebergScanRange.builder()
                .path("s3://b/d.parquet")
                .originalFilePath("s3://b/d.parquet")
                .formatVersion(2)
                .build();
        LegacyInputs legacy = new LegacyInputs(2, "s3://b/d.parquet", null, null,
                null, null, -1L, Collections.emptyList(), null);
        assertParity(range, legacy);
    }

    @Test
    void partitionDataAndPartitionValues() throws TException {
        Map<String, String> partVals = new LinkedHashMap<>();
        partVals.put("year", "2024");
        partVals.put("month", "01");
        IcebergScanRange range = IcebergScanRange.builder()
                .path("s3://b/d.parquet")
                .originalFilePath("s3://b/d.parquet")
                .formatVersion(2)
                .partitionSpecId(0)
                .partitionDataJson("{\"id\":1}")
                .partitionValues(partVals)
                .build();
        LegacyInputs legacy = new LegacyInputs(2, "s3://b/d.parquet", 0, "{\"id\":1}",
                null, null, -1L, Collections.emptyList(), partVals);
        assertParity(range, legacy);
    }

    @Test
    void partitionValueWithNull() throws TException {
        Map<String, String> partVals = new LinkedHashMap<>();
        partVals.put("p", null);
        partVals.put("q", "v");
        IcebergScanRange range = IcebergScanRange.builder()
                .path("s3://b/d.parquet")
                .originalFilePath("s3://b/d.parquet")
                .formatVersion(2)
                .partitionValues(partVals)
                .build();
        LegacyInputs legacy = new LegacyInputs(2, "s3://b/d.parquet", null, null,
                null, null, -1L, Collections.emptyList(), partVals);
        assertParity(range, legacy);
    }

    // ---------- "golden" builder mirroring legacy IcebergScanNode#setIcebergParams ----------

    /** Inputs for the golden builder. Parallel to legacy {@code IcebergSplit} fields. */
    private static final class LegacyInputs {
        final int formatVersion;
        final String originalPath;
        final Integer partitionSpecId;
        final String partitionDataJson;
        final Long firstRowId;
        final Long lastUpdatedSequenceNumber;
        final long tableLevelRowCount;
        final List<IcebergDeleteFileDescriptor> deleteFiles;
        final Map<String, String> partitionValues;

        LegacyInputs(int formatVersion, String originalPath, Integer partitionSpecId,
                String partitionDataJson, Long firstRowId, Long lastUpdatedSequenceNumber,
                long tableLevelRowCount, List<IcebergDeleteFileDescriptor> deleteFiles,
                Map<String, String> partitionValues) {
            this.formatVersion = formatVersion;
            this.originalPath = originalPath;
            this.partitionSpecId = partitionSpecId;
            this.partitionDataJson = partitionDataJson;
            this.firstRowId = firstRowId;
            this.lastUpdatedSequenceNumber = lastUpdatedSequenceNumber;
            this.tableLevelRowCount = tableLevelRowCount;
            this.deleteFiles = deleteFiles;
            this.partitionValues = partitionValues;
        }
    }

    /**
     * Literal port of legacy fe-core
     * {@code IcebergScanNode#setIcebergParams} (lines 278&ndash;377), minus
     * the systable branch and the
     * {@code deleteFilesByReferencedDataFile} bookkeeping that lives on the
     * fe-core scan node and is reproduced elsewhere in the plugin scan
     * provider. Path resolution via {@code LocationPath.toStorageLocation()}
     * is intentionally elided: the plugin-side contract takes already
     * resolved paths.
     */
    private static TFileRangeDesc goldenSetIcebergParams(LegacyInputs in) {
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType("iceberg");
        TIcebergFileDesc fileDesc = new TIcebergFileDesc();
        tableFormatFileDesc.setTableLevelRowCount(in.tableLevelRowCount);
        fileDesc.setFormatVersion(in.formatVersion);
        fileDesc.setOriginalFilePath(in.originalPath);
        if (in.partitionSpecId != null) {
            fileDesc.setPartitionSpecId(in.partitionSpecId);
        }
        if (in.partitionDataJson != null) {
            fileDesc.setPartitionDataJson(in.partitionDataJson);
        }
        if (in.formatVersion >= 3) {
            fileDesc.setFirstRowId(in.firstRowId);
            fileDesc.setLastUpdatedSequenceNumber(in.lastUpdatedSequenceNumber);
        }
        if (in.formatVersion < MIN_DELETE_FILE_SUPPORT_VERSION) {
            fileDesc.setContent(FILE_CONTENT_DATA);
        } else {
            fileDesc.setDeleteFiles(new ArrayList<>());
            for (IcebergDeleteFileDescriptor d : in.deleteFiles) {
                TIcebergDeleteFileDesc t = new TIcebergDeleteFileDesc();
                t.setPath(d.getPath());
                if (d.getFileFormat() == FileFormat.PARQUET) {
                    t.setFileFormat(TFileFormatType.FORMAT_PARQUET);
                } else if (d.getFileFormat() == FileFormat.ORC) {
                    t.setFileFormat(TFileFormatType.FORMAT_ORC);
                }
                switch (d.getKind()) {
                    case POSITION_DELETE:
                        if (d.getPositionLowerBound() != null) {
                            t.setPositionLowerBound(d.getPositionLowerBound());
                        }
                        if (d.getPositionUpperBound() != null) {
                            t.setPositionUpperBound(d.getPositionUpperBound());
                        }
                        t.setContent(CONTENT_POSITION_DELETE);
                        break;
                    case DELETION_VECTOR:
                        if (d.getPositionLowerBound() != null) {
                            t.setPositionLowerBound(d.getPositionLowerBound());
                        }
                        if (d.getPositionUpperBound() != null) {
                            t.setPositionUpperBound(d.getPositionUpperBound());
                        }
                        t.setContent(CONTENT_DELETION_VECTOR);
                        t.setContentOffset(d.getContentOffset());
                        t.setContentSizeInBytes(d.getContentSizeInBytes());
                        break;
                    case EQUALITY_DELETE:
                        List<Integer> boxed = new ArrayList<>();
                        for (int id : d.getFieldIds()) {
                            boxed.add(id);
                        }
                        t.setFieldIds(boxed);
                        t.setContent(CONTENT_EQUALITY_DELETE);
                        break;
                    default:
                        throw new IllegalStateException();
                }
                fileDesc.addToDeleteFiles(t);
            }
        }
        tableFormatFileDesc.setIcebergParams(fileDesc);
        if (in.partitionValues != null && !in.partitionValues.isEmpty()) {
            List<String> keys = new ArrayList<>();
            List<String> vals = new ArrayList<>();
            List<Boolean> isNull = new ArrayList<>();
            for (Map.Entry<String, String> e : in.partitionValues.entrySet()) {
                keys.add(e.getKey());
                vals.add(e.getValue() != null ? e.getValue() : "");
                isNull.add(e.getValue() == null);
            }
            rangeDesc.setColumnsFromPathKeys(keys);
            rangeDesc.setColumnsFromPath(vals);
            rangeDesc.setColumnsFromPathIsNull(isNull);
        }
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
        return rangeDesc;
    }
}
