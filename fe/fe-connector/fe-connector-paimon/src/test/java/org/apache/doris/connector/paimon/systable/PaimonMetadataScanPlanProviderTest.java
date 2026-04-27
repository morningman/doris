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

package org.apache.doris.connector.paimon.systable;

import org.apache.doris.connector.api.scan.ConnectorScanRangeType;

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

class PaimonMetadataScanPlanProviderTest {

    @Test
    void accessorsExposeIdentityAndType() {
        PaimonMetadataScanPlanProvider provider = new PaimonMetadataScanPlanProvider(
                "db", "tbl", "snapshots", () -> null);
        Assertions.assertEquals("db", provider.database());
        Assertions.assertEquals("tbl", provider.table());
        Assertions.assertEquals("snapshots", provider.systemTableName());
        Assertions.assertEquals(ConnectorScanRangeType.FILE_SCAN, provider.getScanRangeType());
    }

    @Test
    void resolveSystemTableInvokesSupplierExactlyOnce() {
        AtomicInteger calls = new AtomicInteger();
        Supplier<Table> supplier = () -> {
            calls.incrementAndGet();
            throw new IllegalStateException("supplier-invoked");
        };
        PaimonMetadataScanPlanProvider provider = new PaimonMetadataScanPlanProvider(
                "db", "tbl", "schemas", supplier);
        Assertions.assertThrows(IllegalStateException.class, provider::resolveSystemTable);
        Assertions.assertEquals(1, calls.get());
    }

    @Test
    void resolveSystemTableRejectsNullBaseTable() {
        PaimonMetadataScanPlanProvider provider = new PaimonMetadataScanPlanProvider(
                "db", "tbl", "tags", () -> null);
        Assertions.assertThrows(NullPointerException.class, provider::resolveSystemTable);
    }

    @Test
    void resolveSystemTableRejectsNonFileStoreTable() {
        Table generic = Mockito.mock(Table.class);
        PaimonMetadataScanPlanProvider provider = new PaimonMetadataScanPlanProvider(
                "db", "tbl", "snapshots", () -> generic);
        IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class,
                provider::resolveSystemTable);
        Assertions.assertTrue(ex.getMessage().contains("FileStoreTable"),
                "message should mention FileStoreTable, got: " + ex.getMessage());
    }

    @Test
    void resolveSystemTableThrowsOnUnknownSysTableName() {
        // SystemTableLoader.load returns null for unknown sys-table names; the
        // provider must surface that as IllegalStateException rather than
        // returning null to the caller.
        FileStoreTable mock = Mockito.mock(FileStoreTable.class);
        PaimonMetadataScanPlanProvider provider = new PaimonMetadataScanPlanProvider(
                "db", "tbl", "totally_not_a_paimon_sys_table", () -> mock);
        IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class,
                provider::resolveSystemTable);
        Assertions.assertTrue(ex.getMessage().contains("totally_not_a_paimon_sys_table"),
                "message should mention the offending name, got: " + ex.getMessage());
    }

    @Test
    void planScanReturnsEmptyListWithoutInvokingSupplier() {
        AtomicInteger calls = new AtomicInteger();
        PaimonMetadataScanPlanProvider provider = new PaimonMetadataScanPlanProvider(
                "db", "tbl", "partitions",
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
    void systemTableNameIsLowerCasedAtConstruction() {
        PaimonMetadataScanPlanProvider provider = new PaimonMetadataScanPlanProvider(
                "db", "tbl", "TABLE_INDEXES", () -> null);
        Assertions.assertEquals("table_indexes", provider.systemTableName());
    }

    @Test
    void nullArgumentsRejected() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new PaimonMetadataScanPlanProvider(
                        null, "tbl", "files", () -> null));
        Assertions.assertThrows(NullPointerException.class,
                () -> new PaimonMetadataScanPlanProvider(
                        "db", null, "files", () -> null));
        Assertions.assertThrows(NullPointerException.class,
                () -> new PaimonMetadataScanPlanProvider(
                        "db", "tbl", null, () -> null));
        Assertions.assertThrows(NullPointerException.class,
                () -> new PaimonMetadataScanPlanProvider(
                        "db", "tbl", "files", null));
    }
}
