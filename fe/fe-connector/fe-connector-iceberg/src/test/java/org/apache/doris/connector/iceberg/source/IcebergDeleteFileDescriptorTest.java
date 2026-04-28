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

import org.apache.iceberg.FileFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class IcebergDeleteFileDescriptorTest {

    @Test
    void positionDeleteBuilderRoundTrip() {
        IcebergDeleteFileDescriptor d = IcebergDeleteFileDescriptor.builder()
                .kind(IcebergDeleteFileDescriptor.Kind.POSITION_DELETE)
                .path("s3://b/pos.parquet")
                .fileFormat(FileFormat.PARQUET)
                .positionLowerBound(10L)
                .positionUpperBound(99L)
                .build();
        Assertions.assertEquals(IcebergDeleteFileDescriptor.Kind.POSITION_DELETE, d.getKind());
        Assertions.assertEquals(1, d.getKind().wireValue());
        Assertions.assertEquals("s3://b/pos.parquet", d.getPath());
        Assertions.assertEquals(FileFormat.PARQUET, d.getFileFormat());
        Assertions.assertEquals(10L, d.getPositionLowerBound());
        Assertions.assertEquals(99L, d.getPositionUpperBound());
        Assertions.assertNull(d.getFieldIds());
    }

    @Test
    void deletionVectorCarriesOffsetAndSize() {
        IcebergDeleteFileDescriptor d = IcebergDeleteFileDescriptor.builder()
                .kind(IcebergDeleteFileDescriptor.Kind.DELETION_VECTOR)
                .path("s3://b/dv.puffin")
                .fileFormat(FileFormat.PUFFIN)
                .positionLowerBound(0L)
                .positionUpperBound(1024L)
                .contentOffset(64L)
                .contentSizeInBytes(256L)
                .build();
        Assertions.assertEquals(3, d.getKind().wireValue());
        Assertions.assertEquals(64L, d.getContentOffset());
        Assertions.assertEquals(256L, d.getContentSizeInBytes());
        Assertions.assertEquals(FileFormat.PUFFIN, d.getFileFormat());
    }

    @Test
    void equalityDeleteRequiresFieldIds() {
        IcebergDeleteFileDescriptor d = IcebergDeleteFileDescriptor.builder()
                .kind(IcebergDeleteFileDescriptor.Kind.EQUALITY_DELETE)
                .path("s3://b/eq.parquet")
                .fileFormat(FileFormat.PARQUET)
                .fieldIds(new int[]{1, 2, 3})
                .build();
        Assertions.assertArrayEquals(new int[]{1, 2, 3}, d.getFieldIds());
        Assertions.assertEquals(2, d.getKind().wireValue());
    }

    @Test
    void equalityDeleteWithoutFieldIdsFailsFast() {
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> IcebergDeleteFileDescriptor.builder()
                        .kind(IcebergDeleteFileDescriptor.Kind.EQUALITY_DELETE)
                        .path("p")
                        .fileFormat(FileFormat.PARQUET)
                        .build());
        Assertions.assertTrue(ex.getMessage().contains("fieldIds"));
    }

    @Test
    void fieldIdsArrayIsDefensivelyCopied() {
        int[] src = {7, 8};
        IcebergDeleteFileDescriptor d = IcebergDeleteFileDescriptor.builder()
                .kind(IcebergDeleteFileDescriptor.Kind.EQUALITY_DELETE)
                .path("p")
                .fileFormat(FileFormat.PARQUET)
                .fieldIds(src)
                .build();
        src[0] = 99;
        Assertions.assertArrayEquals(new int[]{7, 8}, d.getFieldIds());
        int[] view = d.getFieldIds();
        view[0] = 100;
        Assertions.assertArrayEquals(new int[]{7, 8}, d.getFieldIds());
        Assertions.assertNotSame(view, d.getFieldIds());
    }

    @Test
    void equalsAndHashCodeIncludeFieldIds() {
        IcebergDeleteFileDescriptor a = IcebergDeleteFileDescriptor.builder()
                .kind(IcebergDeleteFileDescriptor.Kind.EQUALITY_DELETE)
                .path("p")
                .fileFormat(FileFormat.PARQUET)
                .fieldIds(new int[]{1, 2})
                .build();
        IcebergDeleteFileDescriptor b = IcebergDeleteFileDescriptor.builder()
                .kind(IcebergDeleteFileDescriptor.Kind.EQUALITY_DELETE)
                .path("p")
                .fileFormat(FileFormat.PARQUET)
                .fieldIds(new int[]{1, 2})
                .build();
        IcebergDeleteFileDescriptor c = IcebergDeleteFileDescriptor.builder()
                .kind(IcebergDeleteFileDescriptor.Kind.EQUALITY_DELETE)
                .path("p")
                .fileFormat(FileFormat.PARQUET)
                .fieldIds(new int[]{1, 3})
                .build();
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        Assertions.assertNotEquals(a, c);
    }

    @Test
    void requiredFieldsAreEnforced() {
        Assertions.assertThrows(NullPointerException.class,
                () -> IcebergDeleteFileDescriptor.builder().path("p").fileFormat(FileFormat.PARQUET).build());
        Assertions.assertThrows(NullPointerException.class,
                () -> IcebergDeleteFileDescriptor.builder()
                        .kind(IcebergDeleteFileDescriptor.Kind.POSITION_DELETE)
                        .fileFormat(FileFormat.PARQUET).build());
        Assertions.assertThrows(NullPointerException.class,
                () -> IcebergDeleteFileDescriptor.builder()
                        .kind(IcebergDeleteFileDescriptor.Kind.POSITION_DELETE)
                        .path("p").build());
    }

    @Test
    void toStringContainsAllFields() {
        IcebergDeleteFileDescriptor d = IcebergDeleteFileDescriptor.builder()
                .kind(IcebergDeleteFileDescriptor.Kind.DELETION_VECTOR)
                .path("p")
                .fileFormat(FileFormat.PUFFIN)
                .contentOffset(8L)
                .contentSizeInBytes(16L)
                .build();
        String s = d.toString();
        Assertions.assertTrue(s.contains("DELETION_VECTOR"));
        Assertions.assertTrue(s.contains("path='p'"));
        Assertions.assertTrue(s.contains("PUFFIN"));
        Assertions.assertTrue(s.contains("contentOffset=8"));
        Assertions.assertTrue(s.contains("contentSizeInBytes=16"));
    }
}
