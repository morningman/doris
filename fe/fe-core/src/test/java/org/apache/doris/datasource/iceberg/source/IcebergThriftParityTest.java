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

package org.apache.doris.datasource.iceberg.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.iceberg.FileFormat;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Bit-level parity gate, fe-core side: drives the <em>real</em> legacy
 * {@link IcebergScanNode#setIcebergParams(TFileRangeDesc, IcebergSplit)}
 * (via reflection) and asserts the resulting {@link TFileRangeDesc}
 * serialises byte-identically to a "golden" expected layout that mirrors
 * the plugin-side {@code IcebergScanRange#populateRangeParams} field
 * ordering (see fe-connector-iceberg
 * {@code IcebergScanRangeThriftParityTest} for the plugin-side equivalent).
 *
 * <p>Together these two tests form a transitive parity proof:
 * <pre>
 *   real-legacy bytes  ==  golden bytes  ==  plugin bytes
 * </pre>
 * If either side drifts, exactly one of the two test files will start
 * failing and pinpoint the divergence. Cross-module direct comparison is
 * not possible because fe-connector-* MUST NOT depend on fe-core
 * (banned-dependency rule, see fe-connector-iceberg/pom.xml).
 *
 * <p>Scope: parity is asserted for the {@code TIcebergFileDesc} fields
 * that the plugin populates today. The systable branch and the
 * non-equality-delete bookkeeping side-effect (legacy-only state on
 * {@code IcebergScanNode}) are intentionally outside the parity contract;
 * those run through other code paths.
 */
public class IcebergThriftParityTest {

    private static final int MIN_DELETE_FILE_SUPPORT_VERSION = 2;
    private static final int FILE_CONTENT_DATA = 0;
    private static final int CONTENT_POSITION_DELETE = 1;
    private static final int CONTENT_EQUALITY_DELETE = 2;
    private static final int CONTENT_DELETION_VECTOR = 3;

    private static final String TABLE_FORMAT_TYPE = "iceberg";

    private static class TestIcebergScanNode extends IcebergScanNode {
        TestIcebergScanNode() {
            super(new PlanNodeId(0), new TupleDescriptor(new TupleId(0)),
                    new SessionVariable(), ScanContext.EMPTY);
        }

        @Override
        public boolean isBatchMode() {
            return false;
        }
    }

    private static byte[] serialize(TFileRangeDesc desc) throws Exception {
        return new TSerializer(new TBinaryProtocol.Factory()).serialize(desc);
    }

    /**
     * Inputs sufficient to drive both the legacy {@code IcebergSplit}
     * pipeline and the equivalent golden builder. Field semantics mirror
     * {@code IcebergSplit} 1:1; the tests own building both sides from
     * the same instance so that any drift surfaces here.
     */
    private static final class Scenario {
        final int formatVersion;
        final String dataPath;
        Integer partitionSpecId;
        String partitionDataJson;
        Long firstRowId;
        Long lastUpdatedSequenceNumber;
        long tableLevelRowCount = -1L;
        boolean tableLevelPushDownActive;
        Map<String, String> partitionValues;
        final List<IcebergDeleteFileFilter> deleteFilters = new ArrayList<>();

        Scenario(int formatVersion, String dataPath) {
            this.formatVersion = formatVersion;
            this.dataPath = Objects.requireNonNull(dataPath);
        }
    }

    private static TFileRangeDesc runLegacy(Scenario s) throws Exception {
        TestIcebergScanNode node = new TestIcebergScanNode();
        Field formatVersionField = IcebergScanNode.class.getDeclaredField("formatVersion");
        formatVersionField.setAccessible(true);
        formatVersionField.setInt(node, s.formatVersion);
        if (s.tableLevelPushDownActive) {
            Field flag = IcebergScanNode.class.getDeclaredField("tableLevelPushDownCount");
            flag.setAccessible(true);
            flag.setBoolean(node, true);
        }

        IcebergSplit split = new IcebergSplit(LocationPath.of(s.dataPath), 0, 128, 128, new String[0],
                s.formatVersion, Collections.emptyMap(), new ArrayList<>(), s.dataPath);
        split.setTableFormatType(TableFormatType.ICEBERG);
        split.setPartitionSpecId(s.partitionSpecId);
        split.setPartitionDataJson(s.partitionDataJson);
        split.setFirstRowId(s.firstRowId);
        split.setLastUpdatedSequenceNumber(s.lastUpdatedSequenceNumber);
        split.setTableLevelRowCount(s.tableLevelRowCount);
        split.setIcebergPartitionValues(s.partitionValues);
        if (!s.deleteFilters.isEmpty()) {
            split.setDeleteFileFilters(new ArrayList<>(), s.deleteFilters);
        }

        Method method = IcebergScanNode.class.getDeclaredMethod("setIcebergParams",
                TFileRangeDesc.class, IcebergSplit.class);
        method.setAccessible(true);
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        method.invoke(node, rangeDesc, split);
        return rangeDesc;
    }

    /**
     * Mirrors the field ordering and conditional logic of the plugin's
     * {@code IcebergScanRange#populateRangeParams}. Delete-file paths are
     * resolved through {@link LocationPath} to match the legacy
     * {@code setIcebergParams} path-resolution behaviour (the plugin
     * receives already-resolved paths from its caller; for parity the
     * golden builder applies the same resolution legacy applies).
     */
    private static TFileRangeDesc goldenForPluginShape(Scenario s) {
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        formatDesc.setTableFormatType(TABLE_FORMAT_TYPE);

        long advertisedRowCount = s.tableLevelPushDownActive ? s.tableLevelRowCount : -1L;
        formatDesc.setTableLevelRowCount(advertisedRowCount);

        TIcebergFileDesc fileDesc = new TIcebergFileDesc();
        fileDesc.setFormatVersion(s.formatVersion);
        fileDesc.setOriginalFilePath(s.dataPath);
        if (s.partitionSpecId != null) {
            fileDesc.setPartitionSpecId(s.partitionSpecId);
        }
        if (s.partitionDataJson != null) {
            fileDesc.setPartitionDataJson(s.partitionDataJson);
        }
        if (s.formatVersion >= 3) {
            fileDesc.setFirstRowId(s.firstRowId);
            fileDesc.setLastUpdatedSequenceNumber(s.lastUpdatedSequenceNumber);
        }
        if (s.formatVersion < MIN_DELETE_FILE_SUPPORT_VERSION) {
            fileDesc.setContent(FILE_CONTENT_DATA);
        } else {
            fileDesc.setDeleteFiles(new ArrayList<>());
            for (IcebergDeleteFileFilter filter : s.deleteFilters) {
                fileDesc.addToDeleteFiles(toThriftGolden(filter));
            }
        }
        formatDesc.setIcebergParams(fileDesc);

        if (s.partitionValues != null && !s.partitionValues.isEmpty()) {
            List<String> keys = new ArrayList<>();
            List<String> values = new ArrayList<>();
            List<Boolean> isNull = new ArrayList<>();
            for (Map.Entry<String, String> entry : s.partitionValues.entrySet()) {
                keys.add(entry.getKey());
                values.add(entry.getValue() != null ? entry.getValue() : "");
                isNull.add(entry.getValue() == null);
            }
            rangeDesc.setColumnsFromPathKeys(keys);
            rangeDesc.setColumnsFromPath(values);
            rangeDesc.setColumnsFromPathIsNull(isNull);
        }
        rangeDesc.setTableFormatParams(formatDesc);
        return rangeDesc;
    }

    private static TIcebergDeleteFileDesc toThriftGolden(IcebergDeleteFileFilter filter) {
        TIcebergDeleteFileDesc t = new TIcebergDeleteFileDesc();
        // Match legacy path resolution.
        String resolvedPath = LocationPath.of(filter.getDeleteFilePath(), Collections.emptyMap())
                .toStorageLocation().toString();
        t.setPath(resolvedPath);
        if (filter.getFileformat() == FileFormat.PARQUET) {
            t.setFileFormat(TFileFormatType.FORMAT_PARQUET);
        } else if (filter.getFileformat() == FileFormat.ORC) {
            t.setFileFormat(TFileFormatType.FORMAT_ORC);
        }
        if (filter instanceof IcebergDeleteFileFilter.DeletionVector) {
            IcebergDeleteFileFilter.DeletionVector dv = (IcebergDeleteFileFilter.DeletionVector) filter;
            if (dv.getPositionLowerBound().isPresent()) {
                t.setPositionLowerBound(dv.getPositionLowerBound().getAsLong());
            }
            if (dv.getPositionUpperBound().isPresent()) {
                t.setPositionUpperBound(dv.getPositionUpperBound().getAsLong());
            }
            t.setContent(CONTENT_DELETION_VECTOR);
            t.setContentOffset(dv.getContentOffset());
            t.setContentSizeInBytes(dv.getContentLength());
        } else if (filter instanceof IcebergDeleteFileFilter.PositionDelete) {
            IcebergDeleteFileFilter.PositionDelete pd = (IcebergDeleteFileFilter.PositionDelete) filter;
            if (pd.getPositionLowerBound().isPresent()) {
                t.setPositionLowerBound(pd.getPositionLowerBound().getAsLong());
            }
            if (pd.getPositionUpperBound().isPresent()) {
                t.setPositionUpperBound(pd.getPositionUpperBound().getAsLong());
            }
            t.setContent(CONTENT_POSITION_DELETE);
        } else if (filter instanceof IcebergDeleteFileFilter.EqualityDelete) {
            IcebergDeleteFileFilter.EqualityDelete eq = (IcebergDeleteFileFilter.EqualityDelete) filter;
            t.setFieldIds(new ArrayList<>(eq.getFieldIds()));
            t.setContent(CONTENT_EQUALITY_DELETE);
        } else {
            throw new IllegalStateException("Unknown delete filter: " + filter.getClass());
        }
        return t;
    }

    private static void assertParity(Scenario s) throws Exception {
        TFileRangeDesc actual = runLegacy(s);
        TFileRangeDesc expected = goldenForPluginShape(s);
        Assert.assertEquals("TFileRangeDesc field equality mismatch",
                expected, actual);
        Assert.assertArrayEquals("TFileRangeDesc binary mismatch",
                serialize(expected), serialize(actual));
    }

    // ---------------------------------------------------------------------
    // Scenarios required by M2-Iceberg-08 brief.
    // ---------------------------------------------------------------------

    @Test
    public void v1PureDataFileNoDeletes() throws Exception {
        Scenario s = new Scenario(1, "file:///tmp/d.parquet");
        assertParity(s);
    }

    @Test
    public void v2PositionDeletesOnlyParquet() throws Exception {
        Scenario s = new Scenario(2, "file:///tmp/d.parquet");
        s.deleteFilters.add(new IcebergDeleteFileFilter.PositionDelete(
                "file:///tmp/pos.parquet", 10L, 99L, 256L, FileFormat.PARQUET));
        assertParity(s);
    }

    @Test
    public void v2PositionDeletesNoBoundsOrc() throws Exception {
        Scenario s = new Scenario(2, "file:///tmp/d.orc");
        s.deleteFilters.add(new IcebergDeleteFileFilter.PositionDelete(
                "file:///tmp/pos.orc", -1L, -1L, 256L, FileFormat.ORC));
        assertParity(s);
    }

    @Test
    public void v2EqualityDeletesOnly() throws Exception {
        Scenario s = new Scenario(2, "file:///tmp/d.parquet");
        s.deleteFilters.add(new IcebergDeleteFileFilter.EqualityDelete(
                "file:///tmp/eq.parquet", Arrays.asList(1, 2, 3), 64L, FileFormat.PARQUET));
        assertParity(s);
    }

    @Test
    public void v2MixedPositionAndEqualityDeletes() throws Exception {
        Scenario s = new Scenario(2, "file:///tmp/d.parquet");
        s.deleteFilters.add(new IcebergDeleteFileFilter.PositionDelete(
                "file:///tmp/pos.parquet", 0L, 1023L, 256L, FileFormat.PARQUET));
        s.deleteFilters.add(new IcebergDeleteFileFilter.EqualityDelete(
                "file:///tmp/eq.orc", Arrays.asList(5), 33L, FileFormat.ORC));
        assertParity(s);
    }

    @Test
    public void v2PartitionSpecIdNonZero() throws Exception {
        Scenario s = new Scenario(2, "file:///tmp/d.parquet");
        s.partitionSpecId = 7;
        s.partitionDataJson = "{\"region\":\"us-east-1\"}";
        assertParity(s);
    }

    @Test
    public void v2WithPartitionValuesIncludingNull() throws Exception {
        Scenario s = new Scenario(2, "file:///tmp/d.parquet");
        s.partitionSpecId = 1;
        s.partitionDataJson = "{\"y\":2024}";
        Map<String, String> partVals = new LinkedHashMap<>();
        partVals.put("year", "2024");
        partVals.put("month", null);
        partVals.put("day", "01");
        s.partitionValues = partVals;
        assertParity(s);
    }

    @Test
    public void v2TableLevelRowCountActive() throws Exception {
        Scenario s = new Scenario(2, "file:///tmp/d.parquet");
        s.tableLevelRowCount = 9999L;
        s.tableLevelPushDownActive = true;
        assertParity(s);
    }

    @Test
    public void v2TableLevelRowCountInactiveDefaultsMinusOne() throws Exception {
        Scenario s = new Scenario(2, "file:///tmp/d.parquet");
        // tableLevelPushDownCount == false: legacy must emit -1 regardless of
        // any value carried by IcebergSplit.tableLevelRowCount.
        s.tableLevelRowCount = 42L;
        s.tableLevelPushDownActive = false;
        assertParity(s);
    }

    @Test
    public void v3DeletionVectorWithRowLineage() throws Exception {
        Scenario s = new Scenario(3, "file:///tmp/d.parquet");
        s.firstRowId = 1000L;
        s.lastUpdatedSequenceNumber = 42L;
        s.deleteFilters.add(new IcebergDeleteFileFilter.DeletionVector(
                "file:///tmp/dv.puffin", 0L, 1024L, 1024L, 64L, 256L));
        assertParity(s);
    }
}
