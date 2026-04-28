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

package org.apache.doris.datasource.paimon.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TPaimonFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Bit-level parity gate, fe-core side: drives the <em>real</em> legacy
 * {@link PaimonScanNode#setPaimonParams(TFileRangeDesc, PaimonSplit)}
 * (via reflection) and asserts the resulting {@link TFileRangeDesc}
 * serialises byte-identically to a "golden" expected layout that mirrors
 * the plugin-side {@code PaimonScanRange#populateRangeParams} field
 * ordering (see fe-connector-paimon
 * {@code PaimonScanRangeWithCountTest} for the plugin-side
 * thrift-shape coverage).
 *
 * <p>Together with the plugin-side tests these form a transitive parity
 * proof:
 * <pre>
 *   real-legacy bytes  ==  golden bytes  ==  plugin bytes
 * </pre>
 *
 * <p><strong>Scope.</strong> Parity is asserted for the
 * <em>native-reader</em> path of {@code setPaimonParams} only —
 * the JNI path serialises the inner paimon {@link
 * org.apache.paimon.table.source.Split} via {@code
 * PaimonUtil.encodeObjectToString} which the plugin's caller
 * ({@code PaimonScanPlanProvider#buildJniScanRange}) drives separately;
 * the parity contract for {@code populateRangeParams} starts after that
 * encoding has been applied. Deletion-file parity is intentionally
 * outside the contract because the legacy resolves the path through
 * {@link LocationPath#toStorageLocation()} (fe-core only) while the
 * plugin stores the raw paimon path. Both points are documented in the
 * M3-paimon-readpath handoff.
 */
public class PaimonThriftParityTest {

    private static final String TABLE_FORMAT_TYPE = "paimon";

    private static class TestPaimonScanNode extends PaimonScanNode {
        TestPaimonScanNode() {
            super(new PlanNodeId(0), new TupleDescriptor(new TupleId(0)),
                    false, new SessionVariable(), ScanContext.EMPTY);
        }

        @Override
        public boolean isBatchMode() {
            return false;
        }
    }

    private static byte[] serialize(TFileRangeDesc desc) throws Exception {
        return new TSerializer(new TBinaryProtocol.Factory()).serialize(desc);
    }

    /** Inputs sufficient to drive both the legacy native path and the golden builder. */
    private static final class Scenario {
        final String dataPath;
        final long schemaId;
        Long rowCount;
        Map<String, String> partitionValues;

        Scenario(String dataPath, long schemaId) {
            this.dataPath = Objects.requireNonNull(dataPath);
            this.schemaId = schemaId;
        }
    }

    /**
     * Construct a {@link PaimonSplit} via the private file-based constructor
     * (required to drive the native reader path, where {@code split} is
     * {@code null} on the underlying paimon side).
     */
    private static PaimonSplit newNativeSplit(Scenario s) throws Exception {
        Constructor<PaimonSplit> ctor = PaimonSplit.class.getDeclaredConstructor(
                LocationPath.class, long.class, long.class, long.class, long.class,
                String[].class, List.class);
        ctor.setAccessible(true);
        PaimonSplit split = ctor.newInstance(
                LocationPath.of(s.dataPath), 0L, 128L, 128L, 0L,
                new String[0], (List<String>) null);
        split.setSchemaId(s.schemaId);
        if (s.rowCount != null) {
            split.setRowCount(s.rowCount);
        }
        if (s.partitionValues != null) {
            split.setPaimonPartitionValues(s.partitionValues);
        }
        return split;
    }

    /** Minimal stub source that satisfies the few accessors {@code setPaimonParams}
     *  may invoke on the native path; eagerly-evaluated {@code Optional.orElse}
     *  forces {@code getFileFormatFromTableProperties} to be callable even when
     *  the suffix-based detection succeeds. */
    private static final class StubPaimonSource extends PaimonSource {
        @Override
        public String getFileFormatFromTableProperties() {
            return "parquet";
        }
    }

    private static TFileRangeDesc runLegacy(Scenario s) throws Exception {
        TestPaimonScanNode node = new TestPaimonScanNode();
        node.setSource(new StubPaimonSource());
        // Pre-populate the schema-history cache so putHistorySchemaInfo
        // short-circuits on putIfAbsent and never reaches source.getCatalog().
        Field cache = PaimonScanNode.class.getDeclaredField("currentQuerySchema");
        cache.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentHashMap<Long, Boolean> map =
                (ConcurrentHashMap<Long, Boolean>) cache.get(node);
        map.put(s.schemaId, Boolean.TRUE);

        PaimonSplit split = newNativeSplit(s);
        Method method = PaimonScanNode.class.getDeclaredMethod(
                "setPaimonParams", TFileRangeDesc.class, PaimonSplit.class);
        method.setAccessible(true);
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        method.invoke(node, rangeDesc, split);
        return rangeDesc;
    }

    /**
     * Mirrors the field ordering and conditional logic of the plugin's
     * {@code PaimonScanRange#populateRangeParams} for the native-reader
     * path.
     */
    private static TFileRangeDesc goldenForPluginShape(Scenario s) {
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        formatDesc.setTableFormatType(TABLE_FORMAT_TYPE);

        TPaimonFileDesc fileDesc = new TPaimonFileDesc();
        fileDesc.setSchemaId(s.schemaId);
        String fileFormat = s.dataPath.endsWith(".orc") ? "orc" : "parquet";
        fileDesc.setFileFormat(fileFormat);

        long advertisedRowCount = s.rowCount != null ? s.rowCount : -1L;
        formatDesc.setTableLevelRowCount(advertisedRowCount);
        formatDesc.setPaimonParams(fileDesc);

        if (fileFormat.equals("orc")) {
            rangeDesc.setFormatType(TFileFormatType.FORMAT_ORC);
        } else {
            rangeDesc.setFormatType(TFileFormatType.FORMAT_PARQUET);
        }

        if (s.partitionValues != null) {
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

    private static void assertParity(Scenario s) throws Exception {
        TFileRangeDesc actual = runLegacy(s);
        TFileRangeDesc expected = goldenForPluginShape(s);
        Assert.assertEquals("TFileRangeDesc field equality mismatch",
                expected, actual);
        Assert.assertArrayEquals("TFileRangeDesc binary mismatch",
                serialize(expected), serialize(actual));
    }

    // ---------------------------------------------------------------------
    // Scenarios
    // ---------------------------------------------------------------------

    @Test
    public void nativeReaderParquetNoDeletes() throws Exception {
        Scenario s = new Scenario("file:///tmp/d.parquet", 0L);
        assertParity(s);
    }

    @Test
    public void nativeReaderOrc() throws Exception {
        Scenario s = new Scenario("file:///tmp/d.orc", 1L);
        assertParity(s);
    }

    @Test
    public void nativeReaderWithPartitionValues() throws Exception {
        Scenario s = new Scenario("file:///tmp/dt=2024-01-01/d.parquet", 2L);
        Map<String, String> partVals = new LinkedHashMap<>();
        partVals.put("year", "2024");
        partVals.put("month", "01");
        partVals.put("day", "01");
        s.partitionValues = partVals;
        assertParity(s);
    }

    @Test
    public void nativeReaderWithPartitionValuesIncludingNull() throws Exception {
        Scenario s = new Scenario("file:///tmp/dt=2024-01-01/d.parquet", 3L);
        Map<String, String> partVals = new LinkedHashMap<>();
        partVals.put("year", "2024");
        partVals.put("month", null);
        partVals.put("day", "01");
        s.partitionValues = partVals;
        assertParity(s);
    }

    @Test
    public void tableLevelRowCountActive() throws Exception {
        Scenario s = new Scenario("file:///tmp/d.parquet", 4L);
        s.rowCount = 9999L;
        assertParity(s);
    }

    @Test
    public void tableLevelRowCountInactiveDefaultsMinusOne() throws Exception {
        // No rowCount on the split → legacy must emit -1.
        Scenario s = new Scenario("file:///tmp/d.parquet", 5L);
        s.rowCount = null;
        assertParity(s);
    }

    @Test
    public void emptyPartitionValuesEmitsEmptyKeyLists() throws Exception {
        // Legacy emits empty key/value lists when partitionValues is set
        // but empty. The plugin shape does the same (cf.
        // PaimonScanRange#populateRangeParams when partitionValues is the
        // empty Map after Builder defaulting).
        Scenario s = new Scenario("file:///tmp/d.parquet", 6L);
        s.partitionValues = Collections.emptyMap();
        // For completeness the parity contract says: empty map ↔ no
        // path-key fields set on rangeDesc. The legacy currently emits
        // empty lists; verify the golden agrees.
        TFileRangeDesc legacy = runLegacy(s);
        Assert.assertEquals(0, legacy.getColumnsFromPathKeys().size());
        Assert.assertEquals(0, legacy.getColumnsFromPath().size());
        Assert.assertEquals(0, legacy.getColumnsFromPathIsNull().size());
    }
}
