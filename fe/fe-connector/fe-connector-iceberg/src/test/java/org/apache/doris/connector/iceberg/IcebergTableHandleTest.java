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

package org.apache.doris.connector.iceberg;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class IcebergTableHandleTest {

    @Test
    void minimalConstructorLeavesIcebergFieldsUnset() {
        IcebergTableHandle h = new IcebergTableHandle("db", "tbl");
        Assertions.assertEquals("db", h.getDbName());
        Assertions.assertEquals("tbl", h.getTableName());
        Assertions.assertNull(h.getSnapshotId());
        Assertions.assertNull(h.getRefSpec());
        Assertions.assertNull(h.getFormatVersion());
        Assertions.assertNull(h.getSchemaId());
        Assertions.assertNull(h.getPartitionSpecId());
        Assertions.assertNull(h.getMetadataLocation());
    }

    @Test
    void allArgsConstructorPopulatesEveryField() {
        IcebergTableHandle h = new IcebergTableHandle("db", "tbl",
                42L, "branch:main", 2, 7, 3, "s3://meta/v3.json");
        Assertions.assertEquals(42L, h.getSnapshotId());
        Assertions.assertEquals("branch:main", h.getRefSpec());
        Assertions.assertEquals(2, h.getFormatVersion());
        Assertions.assertEquals(7, h.getSchemaId());
        Assertions.assertEquals(3, h.getPartitionSpecId());
        Assertions.assertEquals("s3://meta/v3.json", h.getMetadataLocation());
    }

    @Test
    void builderRoundTripsAllFields() {
        IcebergTableHandle h = IcebergTableHandle.builder()
                .dbName("d").tableName("t")
                .snapshotId(99L).refSpec("tag:v1")
                .formatVersion(2).schemaId(5)
                .partitionSpecId(0).metadataLocation("loc")
                .build();
        IcebergTableHandle copy = h.toBuilder().build();
        Assertions.assertEquals(h, copy);
        Assertions.assertEquals(h.hashCode(), copy.hashCode());
        Assertions.assertNotSame(h, copy);
    }

    @Test
    void equalsAndHashCodeCoverNewFields() {
        IcebergTableHandle base = new IcebergTableHandle("db", "tbl",
                1L, "ref", 2, 3, 4, "m");
        IcebergTableHandle same = new IcebergTableHandle("db", "tbl",
                1L, "ref", 2, 3, 4, "m");
        Assertions.assertEquals(base, same);
        Assertions.assertEquals(base.hashCode(), same.hashCode());

        Assertions.assertNotEquals(base,
                new IcebergTableHandle("db", "tbl", 2L, "ref", 2, 3, 4, "m"));
        Assertions.assertNotEquals(base,
                new IcebergTableHandle("db", "tbl", 1L, "other", 2, 3, 4, "m"));
        Assertions.assertNotEquals(base,
                new IcebergTableHandle("db", "tbl", 1L, "ref", 1, 3, 4, "m"));
        Assertions.assertNotEquals(base,
                new IcebergTableHandle("db", "tbl", 1L, "ref", 2, 99, 4, "m"));
        Assertions.assertNotEquals(base,
                new IcebergTableHandle("db", "tbl", 1L, "ref", 2, 3, 0, "m"));
        Assertions.assertNotEquals(base,
                new IcebergTableHandle("db", "tbl", 1L, "ref", 2, 3, 4, "n"));
        Assertions.assertNotEquals(base, "not a handle");
    }

    @Test
    void toStringIncludesEveryField() {
        IcebergTableHandle h = new IcebergTableHandle("db", "tbl",
                42L, "branch:main", 2, 7, 3, "s3://meta");
        String s = h.toString();
        Assertions.assertTrue(s.contains("db.tbl"), s);
        Assertions.assertTrue(s.contains("snapshotId=42"), s);
        Assertions.assertTrue(s.contains("refSpec=branch:main"), s);
        Assertions.assertTrue(s.contains("formatVersion=2"), s);
        Assertions.assertTrue(s.contains("schemaId=7"), s);
        Assertions.assertTrue(s.contains("partitionSpecId=3"), s);
        Assertions.assertTrue(s.contains("metadataLocation=s3://meta"), s);
    }

    @Test
    void nullableSnapshotAndRefSpecAreAccepted() {
        IcebergTableHandle h = IcebergTableHandle.builder()
                .dbName("d").tableName("t")
                .snapshotId(null).refSpec(null)
                .formatVersion(2).schemaId(0)
                .partitionSpecId(0).metadataLocation("x")
                .build();
        Assertions.assertNull(h.getSnapshotId());
        Assertions.assertNull(h.getRefSpec());
        Assertions.assertEquals(2, h.getFormatVersion());
        Assertions.assertNotNull(h.toString());
    }

    @Test
    void requireNonNullForIdentityFields() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new IcebergTableHandle(null, "t"));
        Assertions.assertThrows(NullPointerException.class,
                () -> new IcebergTableHandle("d", null));
    }
}
