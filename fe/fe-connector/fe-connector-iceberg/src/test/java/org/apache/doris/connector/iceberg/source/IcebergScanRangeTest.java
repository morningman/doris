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

import org.apache.doris.connector.api.scan.ConnectorScanRangeType;

import org.apache.iceberg.FileFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class IcebergScanRangeTest {

    private static IcebergDeleteFileDescriptor positionDelete() {
        return IcebergDeleteFileDescriptor.builder()
                .kind(IcebergDeleteFileDescriptor.Kind.POSITION_DELETE)
                .path("s3://b/pos.parquet")
                .fileFormat(FileFormat.PARQUET)
                .build();
    }

    @Test
    void buildExposesAllFields() {
        Map<String, String> partVals = new LinkedHashMap<>();
        partVals.put("p", "1");
        IcebergScanRange r = IcebergScanRange.builder()
                .path("s3://b/data.parquet")
                .start(0)
                .length(1024)
                .fileSize(2048)
                .formatVersion(2)
                .partitionSpecId(0)
                .partitionDataJson("{\"id\":1}")
                .partitionValues(partVals)
                .originalFilePath("s3://b/data.parquet")
                .deleteFiles(Collections.singletonList(positionDelete()))
                .tableLevelRowCount(7L)
                .properties(Collections.singletonMap("k", "v"))
                .build();
        Assertions.assertEquals(ConnectorScanRangeType.FILE_SCAN, r.getRangeType());
        Assertions.assertTrue(r.getPath().isPresent());
        Assertions.assertEquals("s3://b/data.parquet", r.getPath().get());
        Assertions.assertEquals(0, r.getStart());
        Assertions.assertEquals(1024L, r.getLength());
        Assertions.assertEquals(2048L, r.getFileSize());
        Assertions.assertEquals("iceberg", r.getTableFormatType());
        Assertions.assertEquals(2, r.getFormatVersion());
        Assertions.assertEquals(Integer.valueOf(0), r.getPartitionSpecId());
        Assertions.assertEquals("{\"id\":1}", r.getPartitionDataJson());
        Assertions.assertEquals("s3://b/data.parquet", r.getOriginalFilePath());
        Assertions.assertEquals(7L, r.getTableLevelRowCount());
        Assertions.assertEquals("v", r.getProperties().get("k"));
        Assertions.assertEquals(1, r.getIcebergDeleteFiles().size());
        Assertions.assertEquals(partVals, r.getPartitionValues());
        Assertions.assertTrue(r.getDeleteFiles().isEmpty());
    }

    @Test
    void requiredFieldsAreEnforced() {
        Assertions.assertThrows(NullPointerException.class,
                () -> IcebergScanRange.builder().originalFilePath("o").build());
        Assertions.assertThrows(NullPointerException.class,
                () -> IcebergScanRange.builder().path("p").build());
    }

    @Test
    void v3RequiresRowIdAndSequenceNumber() {
        IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class,
                () -> IcebergScanRange.builder()
                        .path("p")
                        .originalFilePath("o")
                        .formatVersion(3)
                        .build());
        Assertions.assertTrue(ex.getMessage().contains("v3"));
    }

    @Test
    void deleteFileListIsDefensivelyCopied() {
        List<IcebergDeleteFileDescriptor> source = new ArrayList<>();
        source.add(positionDelete());
        IcebergScanRange r = IcebergScanRange.builder()
                .path("p").originalFilePath("o").formatVersion(2)
                .deleteFiles(source)
                .build();
        source.add(positionDelete());
        Assertions.assertEquals(1, r.getIcebergDeleteFiles().size());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> r.getIcebergDeleteFiles().add(positionDelete()));
    }

    @Test
    void partitionValuesIsDefensivelyCopied() {
        Map<String, String> source = new HashMap<>();
        source.put("p", "1");
        IcebergScanRange r = IcebergScanRange.builder()
                .path("p").originalFilePath("o").formatVersion(1)
                .partitionValues(source)
                .build();
        source.put("p", "2");
        Assertions.assertEquals("1", r.getPartitionValues().get("p"));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> r.getPartitionValues().put("x", "y"));
    }

    @Test
    void defaultsAreLegacyAligned() {
        IcebergScanRange r = IcebergScanRange.builder()
                .path("p").originalFilePath("o").formatVersion(1).build();
        Assertions.assertEquals(-1L, r.getTableLevelRowCount());
        Assertions.assertEquals(-1L, r.getLength());
        Assertions.assertEquals(-1L, r.getFileSize());
        Assertions.assertTrue(r.getPartitionValues().isEmpty());
        Assertions.assertTrue(r.getIcebergDeleteFiles().isEmpty());
        Assertions.assertTrue(r.getProperties().isEmpty());
        Assertions.assertNull(r.getPartitionSpecId());
        Assertions.assertNull(r.getPartitionDataJson());
        Assertions.assertNull(r.getFirstRowId());
        Assertions.assertNull(r.getLastUpdatedSequenceNumber());
    }

    @Test
    void toStringContainsCoreFields() {
        IcebergScanRange r = IcebergScanRange.builder()
                .path("s3://b/x.parquet").originalFilePath("s3://b/x.parquet")
                .formatVersion(2)
                .deleteFiles(Arrays.asList(positionDelete()))
                .build();
        String s = r.toString();
        Assertions.assertTrue(s.contains("s3://b/x.parquet"));
        Assertions.assertTrue(s.contains("formatVersion=2"));
        Assertions.assertTrue(s.contains("deleteFiles=["));
    }
}
