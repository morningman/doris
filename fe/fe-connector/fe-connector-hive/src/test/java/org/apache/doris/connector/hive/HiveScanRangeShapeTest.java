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

package org.apache.doris.connector.hive;

import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;
import org.apache.doris.thrift.TTransactionalHiveDeleteDeltaDesc;
import org.apache.doris.thrift.TTransactionalHiveDesc;

import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Plugin-side shape coverage for {@link HiveScanRange#populateRangeParams}.
 *
 * <p>Mirrors the legacy {@code HiveScanNode#setScanParams} wire layout
 * (table-level row count + transactional-hive params). The cross-module
 * fe-core parity test ({@code HiveThriftParityTest}) drives the real
 * legacy via reflection and asserts byte-identity against the same
 * golden builders, closing the parity loop.
 */
public class HiveScanRangeShapeTest {

    private static byte[] serialize(TTableFormatFileDesc desc) throws Exception {
        return new TSerializer(new TBinaryProtocol.Factory()).serialize(desc);
    }

    /**
     * Drives {@link HiveScanRange#populateRangeParams} after applying the
     * framework-side {@code formatDesc.tableFormatType} prelude that
     * {@code PluginDrivenScanNode#setScanParams} performs before delegation.
     */
    private static TTableFormatFileDesc populate(HiveScanRange range) {
        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        formatDesc.setTableFormatType(range.getTableFormatType());
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        range.populateRangeParams(formatDesc, rangeDesc);
        return formatDesc;
    }

    @Test
    public void nonTransactionalHiveAdvertisesMinusOneRowCount() {
        HiveScanRange range = HiveScanRange.builder()
                .path("hdfs://nn/db/t/000000_0")
                .start(0).length(1024).fileSize(1024)
                .fileFormat("parquet")
                .build();
        TTableFormatFileDesc desc = populate(range);
        Assertions.assertEquals("hive", desc.getTableFormatType());
        Assertions.assertTrue(desc.isSetTableLevelRowCount());
        Assertions.assertEquals(-1L, desc.getTableLevelRowCount());
        Assertions.assertFalse(desc.isSetTransactionalHiveParams());
    }

    @Test
    public void textFormatNonTransactionalHiveStillEmitsRowCount() {
        HiveScanRange range = HiveScanRange.builder()
                .path("hdfs://nn/db/t/000000_0")
                .fileFormat("text")
                .build();
        TTableFormatFileDesc desc = populate(range);
        Assertions.assertEquals(-1L, desc.getTableLevelRowCount());
        Assertions.assertFalse(desc.isSetTransactionalHiveParams());
    }

    @Test
    public void partitionValuesDoNotAffectFormatDescBody() throws Exception {
        Map<String, String> partVals = new LinkedHashMap<>();
        partVals.put("year", "2024");
        partVals.put("month", "01");
        HiveScanRange ranged = HiveScanRange.builder()
                .path("hdfs://nn/db/t/year=2024/month=01/000000_0")
                .partitionValues(partVals)
                .build();
        HiveScanRange plain = HiveScanRange.builder()
                .path("hdfs://nn/db/t/year=2024/month=01/000000_0")
                .build();
        Assertions.assertArrayEquals(serialize(populate(plain)), serialize(populate(ranged)));
    }

    @Test
    public void transactionalHiveWithPartitionOnlyNoDeltas() {
        HiveScanRange range = HiveScanRange.builder()
                .path("hdfs://nn/db/t/p=1/base_001/000000_0")
                .acidInfo("hdfs://nn/db/t/p=1", Collections.emptyList())
                .build();
        TTableFormatFileDesc desc = populate(range);
        Assertions.assertEquals("transactional_hive", desc.getTableFormatType());
        Assertions.assertEquals(-1L, desc.getTableLevelRowCount());
        TTransactionalHiveDesc txn = desc.getTransactionalHiveParams();
        Assertions.assertEquals("hdfs://nn/db/t/p=1", txn.getPartition());
        Assertions.assertNotNull(txn.getDeleteDeltas());
        Assertions.assertEquals(0, txn.getDeleteDeltas().size());
    }

    @Test
    public void transactionalHiveWithDirectoryOnlyDeltaPreservesNoFileNames() {
        HiveScanRange range = HiveScanRange.builder()
                .path("hdfs://nn/db/t/p=1/base_001/000000_0")
                .acidInfo("hdfs://nn/db/t/p=1", Collections.singletonList(
                        "hdfs://nn/db/t/p=1/delete_delta_005_005_0000"))
                .build();
        TTableFormatFileDesc desc = populate(range);
        TTransactionalHiveDesc txn = desc.getTransactionalHiveParams();
        List<TTransactionalHiveDeleteDeltaDesc> deltas = txn.getDeleteDeltas();
        Assertions.assertEquals(1, deltas.size());
        Assertions.assertEquals("hdfs://nn/db/t/p=1/delete_delta_005_005_0000",
                deltas.get(0).getDirectoryLocation());
        Assertions.assertFalse(deltas.get(0).isSetFileNames());
    }

    @Test
    public void transactionalHiveWithFileNamesPropagatesAllNames() {
        HiveScanRange range = HiveScanRange.builder()
                .path("hdfs://nn/db/t/p=1/base_001/000000_0")
                .acidInfo("hdfs://nn/db/t/p=1", Arrays.asList(
                        "hdfs://nn/db/t/p=1/delete_delta_005_005_0000|bucket_00000,bucket_00001",
                        "hdfs://nn/db/t/p=1/delete_delta_006_006_0000|bucket_00000"))
                .build();
        TTableFormatFileDesc desc = populate(range);
        TTransactionalHiveDesc txn = desc.getTransactionalHiveParams();
        List<TTransactionalHiveDeleteDeltaDesc> deltas = txn.getDeleteDeltas();
        Assertions.assertEquals(2, deltas.size());
        Assertions.assertEquals(Arrays.asList("bucket_00000", "bucket_00001"),
                deltas.get(0).getFileNames());
        Assertions.assertEquals(Collections.singletonList("bucket_00000"),
                deltas.get(1).getFileNames());
    }

    @Test
    public void transactionalHiveEmptyFileNamesAfterPipeProducesEmptyList() {
        HiveScanRange range = HiveScanRange.builder()
                .path("hdfs://nn/db/t/p=1/base_001/000000_0")
                .acidInfo("hdfs://nn/db/t/p=1", Collections.singletonList(
                        "hdfs://nn/db/t/p=1/delete_delta_005_005_0000|"))
                .build();
        TTableFormatFileDesc desc = populate(range);
        List<TTransactionalHiveDeleteDeltaDesc> deltas =
                desc.getTransactionalHiveParams().getDeleteDeltas();
        Assertions.assertTrue(deltas.get(0).isSetFileNames());
        Assertions.assertEquals(0, deltas.get(0).getFileNames().size());
    }

    @Test
    public void serialisationIsStableAcrossEquivalentBuilders() throws Exception {
        HiveScanRange a = HiveScanRange.builder()
                .path("hdfs://nn/db/t/000000_0").fileFormat("orc").build();
        HiveScanRange b = HiveScanRange.builder()
                .path("hdfs://nn/db/t/different_000000_0").fileFormat("orc").build();
        // populateRangeParams shape is independent of file path / size.
        Assertions.assertArrayEquals(serialize(populate(a)), serialize(populate(b)));
    }
}
