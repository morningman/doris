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

import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Covers the immutable copy semantics of
 * {@link IcebergScanRange#withTableLevelRowCount(long)} and
 * {@link IcebergScanRange#toBuilder()}: every field except
 * {@code tableLevelRowCount} must round-trip identically to the
 * original, the original must remain unmodified, and the new count
 * must surface in {@link IcebergScanRange#populateRangeParams}.
 */
class IcebergScanRangeWithCountTest {

    private static final IcebergDeleteFileDescriptor POSITION_DELETE =
            IcebergDeleteFileDescriptor.builder()
                    .kind(IcebergDeleteFileDescriptor.Kind.POSITION_DELETE)
                    .path("s3://bucket/dt/pos-1.parquet")
                    .fileFormat(org.apache.iceberg.FileFormat.PARQUET)
                    .build();

    private static IcebergScanRange newV2Range(long initialCount) {
        Map<String, String> partitionValues = new LinkedHashMap<>();
        partitionValues.put("dt", "2024-01-01");
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("p1", "v1");
        return IcebergScanRange.builder()
                .path("s3://bucket/dt=2024-01-01/data-1.parquet")
                .start(0)
                .length(2048)
                .fileSize(4096)
                .formatVersion(2)
                .partitionSpecId(7)
                .partitionDataJson("{\"dt\":\"2024-01-01\"}")
                .partitionValues(partitionValues)
                .originalFilePath("s3://raw/dt=2024-01-01/data-1.parquet")
                .deleteFiles(Collections.singletonList(POSITION_DELETE))
                .tableLevelRowCount(initialCount)
                .properties(properties)
                .build();
    }

    @Test
    void withTableLevelRowCountReturnsNewInstance() {
        IcebergScanRange original = newV2Range(-1L);
        IcebergScanRange copy = original.withTableLevelRowCount(123L);
        Assertions.assertNotSame(original, copy);
        Assertions.assertEquals(-1L, original.getTableLevelRowCount(),
                "original must not be mutated");
        Assertions.assertEquals(123L, copy.getTableLevelRowCount());
    }

    @Test
    void withTableLevelRowCountPreservesEveryOtherField() {
        IcebergScanRange original = newV2Range(0L);
        IcebergScanRange copy = original.withTableLevelRowCount(987L);

        Assertions.assertEquals(original.getPath(), copy.getPath());
        Assertions.assertEquals(original.getStart(), copy.getStart());
        Assertions.assertEquals(original.getLength(), copy.getLength());
        Assertions.assertEquals(original.getFileSize(), copy.getFileSize());
        Assertions.assertEquals(original.getFormatVersion(), copy.getFormatVersion());
        Assertions.assertEquals(original.getPartitionSpecId(), copy.getPartitionSpecId());
        Assertions.assertEquals(original.getPartitionDataJson(), copy.getPartitionDataJson());
        Assertions.assertEquals(original.getPartitionValues(), copy.getPartitionValues());
        Assertions.assertEquals(original.getOriginalFilePath(), copy.getOriginalFilePath());
        Assertions.assertEquals(original.getIcebergDeleteFiles(), copy.getIcebergDeleteFiles());
        Assertions.assertEquals(original.getProperties(), copy.getProperties());
        Assertions.assertEquals(original.getTableFormatType(), copy.getTableFormatType());
        Assertions.assertEquals(original.getRangeType(), copy.getRangeType());
    }

    @Test
    void copiedRangeFlowsCountIntoThriftDescriptor() {
        IcebergScanRange copy = newV2Range(-1L).withTableLevelRowCount(42L);
        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        copy.populateRangeParams(formatDesc, rangeDesc);
        Assertions.assertEquals(42L, formatDesc.getTableLevelRowCount());
    }

    @Test
    void toBuilderProducesIndependentBuilder() {
        IcebergScanRange original = newV2Range(5L);
        IcebergScanRange rebuilt = original.toBuilder().build();
        Assertions.assertNotSame(original, rebuilt);
        Assertions.assertEquals(original.getTableLevelRowCount(),
                rebuilt.getTableLevelRowCount());
        Assertions.assertEquals(original.getPath(), rebuilt.getPath());
    }

    @Test
    void v3RangeRoundTripsOptionalFields() {
        IcebergScanRange v3 = IcebergScanRange.builder()
                .path("s3://b/d/data-2.parquet")
                .start(0)
                .length(1024)
                .fileSize(1024)
                .formatVersion(3)
                .originalFilePath("s3://b/d/data-2.parquet")
                .firstRowId(1000L)
                .lastUpdatedSequenceNumber(17L)
                .build();
        IcebergScanRange copy = v3.withTableLevelRowCount(31L);
        Assertions.assertEquals(31L, copy.getTableLevelRowCount());
        Assertions.assertEquals(1000L, copy.getFirstRowId());
        Assertions.assertEquals(17L, copy.getLastUpdatedSequenceNumber());
        Assertions.assertEquals(3, copy.getFormatVersion());
    }

    @Test
    void deleteFileListCopiedByReferenceIsImmutable() {
        IcebergScanRange original = newV2Range(0L);
        IcebergScanRange copy = original.withTableLevelRowCount(10L);
        List<IcebergDeleteFileDescriptor> deletes = copy.getIcebergDeleteFiles();
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> deletes.add(POSITION_DELETE),
                "delete-file list must remain immutable on copies");
    }
}
