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

import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Covers the immutable copy semantics of
 * {@link PaimonScanRange#withTableLevelRowCount(long)} and
 * {@link PaimonScanRange#toBuilder()}: every field except
 * {@code tableLevelRowCount} must round-trip identically to the
 * original, the original must remain unmodified, and the new count
 * must surface in {@link PaimonScanRange#populateRangeParams}.
 *
 * <p>Mirrors {@code IcebergScanRangeWithCountTest} so the engine-side
 * COUNT-pushdown distribution path
 * ({@code PluginDrivenScanNode#applyCountToRanges}) works identically
 * for both connectors.
 */
class PaimonScanRangeWithCountTest {

    private static PaimonScanRange newNativeRange(long initialCount) {
        Map<String, String> partitionValues = new LinkedHashMap<>();
        partitionValues.put("dt", "2024-01-01");
        return new PaimonScanRange.Builder()
                .path("s3://bucket/dt=2024-01-01/data-1.parquet")
                .start(0)
                .length(2048)
                .fileSize(4096)
                .fileFormat("parquet")
                .partitionValues(partitionValues)
                .schemaId(7L)
                .deletionFile("s3://bucket/dt=2024-01-01/dv-1.bin", 0L, 64L)
                .selfSplitWeight(0L)
                .tableLevelRowCount(initialCount)
                .build();
    }

    @Test
    void withTableLevelRowCountReturnsNewInstance() {
        PaimonScanRange original = newNativeRange(-1L);
        PaimonScanRange copy = original.withTableLevelRowCount(123L);
        Assertions.assertNotSame(original, copy);
        Assertions.assertEquals(-1L, original.getTableLevelRowCount(),
                "original must not be mutated");
        Assertions.assertEquals(123L, copy.getTableLevelRowCount());
    }

    @Test
    void withTableLevelRowCountPreservesEveryOtherField() {
        PaimonScanRange original = newNativeRange(0L);
        PaimonScanRange copy = original.withTableLevelRowCount(987L);

        Assertions.assertEquals(original.getPath(), copy.getPath());
        Assertions.assertEquals(original.getStart(), copy.getStart());
        Assertions.assertEquals(original.getLength(), copy.getLength());
        Assertions.assertEquals(original.getFileSize(), copy.getFileSize());
        Assertions.assertEquals(original.getFileFormat(), copy.getFileFormat());
        Assertions.assertEquals(original.getPartitionValues(), copy.getPartitionValues());
        Assertions.assertEquals(original.getTableFormatType(), copy.getTableFormatType());
        Assertions.assertEquals(original.getRangeType(), copy.getRangeType());
        Assertions.assertEquals(original.getSelfSplitWeight(), copy.getSelfSplitWeight());
        // Properties should match on every key except (potentially) the count;
        // the native path doesn't surface row_count via property at all so the
        // maps are identical when the original carried -1.
        Assertions.assertEquals(original.getProperties().get("paimon.schema_id"),
                copy.getProperties().get("paimon.schema_id"));
        Assertions.assertEquals(original.getProperties().get("paimon.deletion_file.path"),
                copy.getProperties().get("paimon.deletion_file.path"));
    }

    @Test
    void copiedRangeFlowsCountIntoThriftDescriptor() {
        PaimonScanRange copy = newNativeRange(-1L).withTableLevelRowCount(42L);
        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        copy.populateRangeParams(formatDesc, rangeDesc);
        Assertions.assertEquals(42L, formatDesc.getTableLevelRowCount());
    }

    @Test
    void toBuilderProducesIndependentBuilder() {
        PaimonScanRange original = newNativeRange(5L);
        PaimonScanRange rebuilt = original.toBuilder().build();
        Assertions.assertNotSame(original, rebuilt);
        Assertions.assertEquals(original.getTableLevelRowCount(),
                rebuilt.getTableLevelRowCount());
        Assertions.assertEquals(original.getPath(), rebuilt.getPath());
        Assertions.assertEquals(original.getProperties(), rebuilt.getProperties());
    }

    @Test
    void defaultTableLevelRowCountIsMinusOne() {
        PaimonScanRange range = new PaimonScanRange.Builder()
                .path("s3://b/d/data.parquet")
                .start(0)
                .length(100)
                .fileSize(100)
                .fileFormat("parquet")
                .build();
        Assertions.assertEquals(-1L, range.getTableLevelRowCount());
        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        range.populateRangeParams(formatDesc, rangeDesc);
        Assertions.assertEquals(-1L, formatDesc.getTableLevelRowCount());
    }
}
