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

import org.apache.doris.connector.api.systable.SysTableExecutionMode;
import org.apache.doris.connector.api.systable.SysTableSpec;

import org.apache.iceberg.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

class IcebergSystemTableOpsTest {

    private static final BiFunction<String, String, Table> NEVER_CALL =
            (db, t) -> {
                throw new AssertionError("base table loader must not be called during ops construction or lookup");
            };

    @Test
    void publishesSevenSpecsInExpectedOrder() {
        IcebergSystemTableOps ops = new IcebergSystemTableOps(NEVER_CALL);
        List<SysTableSpec> specs = ops.listSysTables("db", "tbl");
        Assertions.assertEquals(7, specs.size());
        List<String> expected = Arrays.asList(
                "snapshots", "history", "files", "entries", "manifests", "refs", "partitions");
        for (int i = 0; i < expected.size(); i++) {
            Assertions.assertEquals(expected.get(i), specs.get(i).name(),
                    "spec at index " + i);
        }
    }

    @Test
    void listSysTableSuffixesReturnsTheSameSevenNames() {
        IcebergSystemTableOps ops = new IcebergSystemTableOps(NEVER_CALL);
        Set<String> suffixes = ops.listSysTableSuffixes("db", "tbl");
        Assertions.assertEquals(
                new LinkedHashSet<>(Arrays.asList(
                        "snapshots", "history", "files", "entries", "manifests", "refs", "partitions")),
                suffixes);
    }

    @Test
    void listSysTableSuffixesIsImmutable() {
        IcebergSystemTableOps ops = new IcebergSystemTableOps(NEVER_CALL);
        Set<String> suffixes = ops.listSysTableSuffixes("db", "tbl");
        Assertions.assertThrows(UnsupportedOperationException.class, () -> suffixes.add("foo"));
    }

    @Test
    void getSysTableReturnsSpecForEachKnownName() {
        IcebergSystemTableOps ops = new IcebergSystemTableOps(NEVER_CALL);
        for (String name : Arrays.asList(
                "snapshots", "history", "files", "entries", "manifests", "refs", "partitions")) {
            Optional<SysTableSpec> spec = ops.getSysTable("db", "tbl", name);
            Assertions.assertTrue(spec.isPresent(), "missing spec for " + name);
            Assertions.assertEquals(name, spec.get().name());
            Assertions.assertEquals(SysTableExecutionMode.NATIVE, spec.get().mode());
            Assertions.assertFalse(spec.get().acceptsTableVersion(),
                    name + " must declare acceptsTableVersion=false (full-history view)");
            Assertions.assertTrue(spec.get().nativeFactory().isPresent(),
                    name + " must publish a NativeSysTableScanFactory");
        }
    }

    @Test
    void getSysTableIsCaseInsensitive() {
        IcebergSystemTableOps ops = new IcebergSystemTableOps(NEVER_CALL);
        Assertions.assertTrue(ops.getSysTable("db", "tbl", "SNAPSHOTS").isPresent());
        Assertions.assertTrue(ops.getSysTable("db", "tbl", "History").isPresent());
    }

    @Test
    void unknownSysTableReturnsEmpty() {
        IcebergSystemTableOps ops = new IcebergSystemTableOps(NEVER_CALL);
        Assertions.assertEquals(Optional.empty(),
                ops.getSysTable("db", "tbl", "position_deletes"));
        Assertions.assertEquals(Optional.empty(),
                ops.getSysTable("db", "tbl", "all_data_files"));
        Assertions.assertEquals(Optional.empty(),
                ops.getSysTable("db", "tbl", "totally-not-a-thing"));
    }

    @Test
    void supportsSysTableDelegatesToGetSysTable() {
        IcebergSystemTableOps ops = new IcebergSystemTableOps(NEVER_CALL);
        Assertions.assertTrue(ops.supportsSysTable("db", "tbl", "snapshots"));
        Assertions.assertFalse(ops.supportsSysTable("db", "tbl", "position_deletes"));
    }

    @Test
    void allSchemasAreNonEmpty() {
        IcebergSystemTableOps ops = new IcebergSystemTableOps(NEVER_CALL);
        for (SysTableSpec spec : ops.listSysTables("db", "tbl")) {
            Assertions.assertFalse(spec.schema().getColumns().isEmpty(),
                    spec.name() + " schema is empty");
            Assertions.assertEquals("ICEBERG_METADATA", spec.schema().getTableFormatType());
        }
    }

    @Test
    void specsAreSharedAcrossListSysTablesCalls() {
        IcebergSystemTableOps ops = new IcebergSystemTableOps(NEVER_CALL);
        Assertions.assertSame(
                ops.getSysTable("db", "tbl", "snapshots").get(),
                ops.getSysTable("db", "tbl", "snapshots").get());
    }

    @Test
    void nullArgumentsRejected() {
        IcebergSystemTableOps ops = new IcebergSystemTableOps(NEVER_CALL);
        Assertions.assertThrows(NullPointerException.class,
                () -> ops.getSysTable(null, "tbl", "snapshots"));
        Assertions.assertThrows(NullPointerException.class,
                () -> ops.getSysTable("db", null, "snapshots"));
        Assertions.assertThrows(NullPointerException.class,
                () -> ops.getSysTable("db", "tbl", null));
        Assertions.assertThrows(NullPointerException.class,
                () -> ops.listSysTables(null, "tbl"));
        Assertions.assertThrows(NullPointerException.class,
                () -> ops.listSysTableSuffixes("db", null));
        Assertions.assertThrows(NullPointerException.class,
                () -> new IcebergSystemTableOps(null));
    }

    @Test
    void baseTableLoaderInvokedOnlyWhenScanResolved() {
        AtomicInteger calls = new AtomicInteger();
        IcebergSystemTableOps ops = new IcebergSystemTableOps((db, t) -> {
            calls.incrementAndGet();
            throw new IllegalStateException("loader called");
        });
        ops.listSysTables("db", "tbl");
        ops.getSysTable("db", "tbl", "snapshots");
        Assertions.assertEquals(0, calls.get(),
                "spec construction and lookup must not load the base iceberg table");
    }

    @Test
    void nativeFactoryProducesProviderForKnownType() {
        IcebergSystemTableOps ops = new IcebergSystemTableOps((db, t) -> null);
        SysTableSpec spec = ops.getSysTable("db", "tbl", "snapshots").orElseThrow();
        Assertions.assertNotNull(spec.nativeFactory().get().create(
                "db", "tbl", "snapshots", Optional.empty()));
    }

    @Test
    void schemaColumnCountMatchesIcebergMetadataLayout() {
        IcebergSystemTableOps ops = new IcebergSystemTableOps(NEVER_CALL);
        // Sanity: each known sys table has a non-empty hard-coded layout.
        // Exact counts mirror the columns we publish in IcebergMetadataTables.
        Assertions.assertEquals(6,
                ops.getSysTable("db", "tbl", "snapshots").get().schema().getColumns().size());
        Assertions.assertEquals(4,
                ops.getSysTable("db", "tbl", "history").get().schema().getColumns().size());
        Assertions.assertEquals(6,
                ops.getSysTable("db", "tbl", "refs").get().schema().getColumns().size());
        Assertions.assertEquals(11,
                ops.getSysTable("db", "tbl", "manifests").get().schema().getColumns().size());
        Assertions.assertEquals(5,
                ops.getSysTable("db", "tbl", "entries").get().schema().getColumns().size());
        Assertions.assertEquals(14,
                ops.getSysTable("db", "tbl", "files").get().schema().getColumns().size());
        Assertions.assertEquals(11,
                ops.getSysTable("db", "tbl", "partitions").get().schema().getColumns().size());
    }

    @Test
    void emptyDatabaseAndTableArgumentsAccepted() {
        // Sys-table spec lookup is independent of the (db, table) identity for
        // iceberg — every iceberg main table exposes the same seven sys tables.
        IcebergSystemTableOps ops = new IcebergSystemTableOps(NEVER_CALL);
        Assertions.assertEquals(7, ops.listSysTables("", "").size());
        Assertions.assertTrue(ops.getSysTable("", "", "history").isPresent());
    }

    @Test
    void listSysTablesReturnsImmutableView() {
        IcebergSystemTableOps ops = new IcebergSystemTableOps(NEVER_CALL);
        List<SysTableSpec> specs = ops.listSysTables("db", "tbl");
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> specs.add(null));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> specs.removeAll(Collections.emptyList()));
    }
}
