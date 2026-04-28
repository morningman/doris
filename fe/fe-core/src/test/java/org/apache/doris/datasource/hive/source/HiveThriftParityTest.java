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

package org.apache.doris.datasource.hive.source;

import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.hive.AcidInfo;
import org.apache.doris.datasource.hive.AcidInfo.DeleteDeltaInfo;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;
import org.apache.doris.thrift.TTransactionalHiveDeleteDeltaDesc;
import org.apache.doris.thrift.TTransactionalHiveDesc;

import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Bit-level parity gate, fe-core side: drives the <em>real</em> legacy
 * {@link HiveScanNode#setScanParams(TFileRangeDesc, org.apache.doris.spi.Split)}
 * (via reflection) and asserts the resulting {@link TFileRangeDesc}
 * serialises byte-identically to a "golden" expected layout that mirrors
 * the plugin-side {@code HiveScanRange#populateRangeParams} field
 * ordering (see {@code fe-connector-hive HiveScanRangeShapeTest} for the
 * plugin-side shape coverage).
 *
 * <p>Together with the plugin-side tests these form a transitive parity
 * proof:
 * <pre>
 *   real-legacy bytes  ==  golden bytes  ==  plugin bytes
 * </pre>
 *
 * <p><strong>Scope.</strong> Hive has no manifest-level row-count
 * pushdown, no time-travel snapshots and no branch/tag refSpec, so the
 * parity contract is intentionally narrow:
 * {@code tableLevelRowCount = -1} on every range, plus
 * {@link TTransactionalHiveDesc} (partition + delete deltas with
 * directory + file names) for ACID splits. Bucketed-scan parity is
 * out of scope because legacy {@link HiveScanNode} does not advertise
 * bucket ids on {@link TFileRangeDesc} either; see the M3-hive-readpath
 * handoff.
 */
public class HiveThriftParityTest {

    private static final String TF_HIVE = TableFormatType.HIVE.value();
    private static final String TF_TXN_HIVE = TableFormatType.TRANSACTIONAL_HIVE.value();

    private static byte[] serialize(TFileRangeDesc desc) throws Exception {
        return new TSerializer(new TBinaryProtocol.Factory()).serialize(desc);
    }

    /**
     * Allocates a {@link HiveScanNode} without invoking its constructor
     * (which would dereference {@code desc.getTable()} as
     * {@code HMSExternalTable}). {@link HiveScanNode#setScanParams} only
     * reads from the supplied split, so an uninitialised node is
     * sufficient to drive the parity contract.
     */
    @SuppressWarnings("unchecked")
    private static HiveScanNode allocateNode() throws Exception {
        Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
        Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        Object unsafe = unsafeField.get(null);
        Method allocateInstance = unsafeClass.getMethod("allocateInstance", Class.class);
        return (HiveScanNode) allocateInstance.invoke(unsafe, HiveScanNode.class);
    }

    /** Constructs a {@link HiveSplit} via its private constructor. */
    private static HiveSplit newSplit(AcidInfo acidInfo) throws Exception {
        Constructor<HiveSplit> ctor = HiveSplit.class.getDeclaredConstructor(
                LocationPath.class, long.class, long.class, long.class,
                long.class, String[].class, List.class, AcidInfo.class);
        ctor.setAccessible(true);
        return ctor.newInstance(
                LocationPath.of("hdfs://nn/db/t/000000_0"),
                0L, 128L, 128L, 0L,
                new String[0], (List<String>) null, acidInfo);
    }

    private static TFileRangeDesc runLegacy(HiveSplit split) throws Exception {
        HiveScanNode node = allocateNode();
        Method method = HiveScanNode.class.getDeclaredMethod(
                "setScanParams", TFileRangeDesc.class, org.apache.doris.spi.Split.class);
        method.setAccessible(true);
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        method.invoke(node, rangeDesc, split);
        return rangeDesc;
    }

    /**
     * Mirrors the field ordering and conditional logic of the plugin's
     * {@code HiveScanRange#populateRangeParams} (combined with the
     * framework-side {@code formatDesc.tableFormatType} prelude that
     * {@code PluginDrivenScanNode#setScanParams} performs before
     * delegating).
     */
    private static TFileRangeDesc goldenForPluginShape(AcidInfo acidInfo) {
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        if (acidInfo == null) {
            formatDesc.setTableFormatType(TF_HIVE);
            formatDesc.setTableLevelRowCount(-1L);
        } else {
            formatDesc.setTableFormatType(TF_TXN_HIVE);
            formatDesc.setTableLevelRowCount(-1L);
            TTransactionalHiveDesc txn = new TTransactionalHiveDesc();
            txn.setPartition(acidInfo.getPartitionLocation());
            List<TTransactionalHiveDeleteDeltaDesc> deltas = new ArrayList<>();
            for (DeleteDeltaInfo info : acidInfo.getDeleteDeltas()) {
                TTransactionalHiveDeleteDeltaDesc d = new TTransactionalHiveDeleteDeltaDesc();
                d.setDirectoryLocation(info.getDirectoryLocation());
                d.setFileNames(info.getFileNames());
                deltas.add(d);
            }
            txn.setDeleteDeltas(deltas);
            formatDesc.setTransactionalHiveParams(txn);
        }
        rangeDesc.setTableFormatParams(formatDesc);
        return rangeDesc;
    }

    private static void assertParity(AcidInfo acidInfo) throws Exception {
        HiveSplit split = newSplit(acidInfo);
        TFileRangeDesc actual = runLegacy(split);
        TFileRangeDesc expected = goldenForPluginShape(acidInfo);
        Assert.assertEquals("TFileRangeDesc field equality mismatch",
                expected, actual);
        Assert.assertArrayEquals("TFileRangeDesc binary mismatch",
                serialize(expected), serialize(actual));
    }

    // ---------------------------------------------------------------------
    // Scenarios
    // ---------------------------------------------------------------------

    @Test
    public void nonTransactionalHiveAdvertisesMinusOneRowCount() throws Exception {
        assertParity(null);
    }

    @Test
    public void transactionalHiveNoDeltas() throws Exception {
        AcidInfo info = new AcidInfo(
                "hdfs://nn/db/t/p=1", Collections.emptyList());
        assertParity(info);
    }

    @Test
    public void transactionalHiveSingleDeltaWithFiles() throws Exception {
        AcidInfo info = new AcidInfo(
                "hdfs://nn/db/t/p=1",
                Collections.singletonList(new DeleteDeltaInfo(
                        "hdfs://nn/db/t/p=1/delete_delta_005_005_0000",
                        Arrays.asList("bucket_00000", "bucket_00001"))));
        assertParity(info);
    }

    @Test
    public void transactionalHiveMultipleDeltasWithMixedFiles() throws Exception {
        AcidInfo info = new AcidInfo(
                "hdfs://nn/db/t/p=2",
                Arrays.asList(
                        new DeleteDeltaInfo(
                                "hdfs://nn/db/t/p=2/delete_delta_005_005_0000",
                                Arrays.asList("bucket_00000", "bucket_00001")),
                        new DeleteDeltaInfo(
                                "hdfs://nn/db/t/p=2/delete_delta_006_006_0000",
                                Collections.singletonList("bucket_00000"))));
        assertParity(info);
    }

    @Test
    public void transactionalHiveDeltaWithEmptyFileNamesList() throws Exception {
        AcidInfo info = new AcidInfo(
                "hdfs://nn/db/t/p=3",
                Collections.singletonList(new DeleteDeltaInfo(
                        "hdfs://nn/db/t/p=3/delete_delta_007_007_0000",
                        Collections.emptyList())));
        assertParity(info);
    }

    @Test
    public void nonHiveSplitIsUntouchedByLegacy() throws Exception {
        // Legacy short-circuits when split is not a HiveSplit; the
        // plugin path never produces such a split, but document the
        // legacy contract here so any future change to the guard is
        // caught.
        HiveScanNode node = allocateNode();
        Method method = HiveScanNode.class.getDeclaredMethod(
                "setScanParams", TFileRangeDesc.class, org.apache.doris.spi.Split.class);
        method.setAccessible(true);
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        method.invoke(node, rangeDesc, (org.apache.doris.spi.Split) null);
        Assert.assertFalse(rangeDesc.isSetTableFormatParams());
    }
}
