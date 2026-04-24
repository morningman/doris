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

package org.apache.doris.connector.iceberg.systable;

import org.apache.doris.connector.api.scan.ConnectorScanRangeType;

import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

class IcebergMetadataScanPlanProviderTest {

    @Test
    void accessorsExposeIdentityAndType() {
        IcebergMetadataScanPlanProvider provider = new IcebergMetadataScanPlanProvider(
                "db", "tbl", MetadataTableType.SNAPSHOTS, () -> null);
        Assertions.assertEquals("db", provider.database());
        Assertions.assertEquals("tbl", provider.table());
        Assertions.assertEquals(MetadataTableType.SNAPSHOTS, provider.metadataTableType());
        Assertions.assertEquals(ConnectorScanRangeType.FILE_SCAN, provider.getScanRangeType());
    }

    @Test
    void resolveMetadataTableInvokesSupplierExactlyOnce() {
        AtomicInteger calls = new AtomicInteger();
        Supplier<Table> supplier = () -> {
            calls.incrementAndGet();
            throw new IllegalStateException("supplier-invoked");
        };
        IcebergMetadataScanPlanProvider provider = new IcebergMetadataScanPlanProvider(
                "db", "tbl", MetadataTableType.HISTORY, supplier);
        Assertions.assertThrows(IllegalStateException.class, provider::resolveMetadataTable);
        Assertions.assertEquals(1, calls.get());
    }

    @Test
    void resolveMetadataTableRejectsNullBaseTable() {
        IcebergMetadataScanPlanProvider provider = new IcebergMetadataScanPlanProvider(
                "db", "tbl", MetadataTableType.REFS, () -> null);
        Assertions.assertThrows(NullPointerException.class, provider::resolveMetadataTable);
    }

    @Test
    void planScanReturnsEmptyListWithoutInvokingSupplier() {
        AtomicInteger calls = new AtomicInteger();
        IcebergMetadataScanPlanProvider provider = new IcebergMetadataScanPlanProvider(
                "db", "tbl", MetadataTableType.PARTITIONS,
                () -> {
                    calls.incrementAndGet();
                    return null;
                });
        Assertions.assertTrue(provider.planScan(
                null, null, Collections.emptyList(), Optional.empty()).isEmpty());
        Assertions.assertEquals(0, calls.get(),
                "planScan must remain a no-op until M1-15 wires the fe-core ScanNode");
    }

    @Test
    void nullArgumentsRejected() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new IcebergMetadataScanPlanProvider(
                        null, "tbl", MetadataTableType.FILES, () -> null));
        Assertions.assertThrows(NullPointerException.class,
                () -> new IcebergMetadataScanPlanProvider(
                        "db", null, MetadataTableType.FILES, () -> null));
        Assertions.assertThrows(NullPointerException.class,
                () -> new IcebergMetadataScanPlanProvider(
                        "db", "tbl", null, () -> null));
        Assertions.assertThrows(NullPointerException.class,
                () -> new IcebergMetadataScanPlanProvider(
                        "db", "tbl", MetadataTableType.FILES, null));
    }
}
