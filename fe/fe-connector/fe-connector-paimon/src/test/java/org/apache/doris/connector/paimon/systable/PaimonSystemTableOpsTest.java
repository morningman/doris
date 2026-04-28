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

import org.apache.doris.connector.api.ConnectorTableId;
import org.apache.doris.connector.api.systable.SysTableExecutionMode;
import org.apache.doris.connector.api.systable.SysTableSpec;

import org.apache.paimon.table.Table;
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

class PaimonSystemTableOpsTest {

    static final List<String> EXPECTED_NAMES = Arrays.asList(
            "snapshots", "schemas", "options", "tags", "branches", "consumers",
            "aggregation_fields", "files", "manifests", "partitions",
            "statistics", "buckets", "table_indexes");

    private static final BiFunction<String, String, Table> NEVER_CALL =
            (db, t) -> {
                throw new AssertionError(
                        "base table loader must not be called during ops construction or lookup");
            };

    @Test
    void publishesThirteenSpecsInExpectedOrder() {
        PaimonSystemTableOps ops = new PaimonSystemTableOps(NEVER_CALL);
        List<SysTableSpec> specs = ops.listSysTables(ConnectorTableId.of("db", "tbl"));
        Assertions.assertEquals(EXPECTED_NAMES.size(), specs.size());
        for (int i = 0; i < EXPECTED_NAMES.size(); i++) {
            Assertions.assertEquals(EXPECTED_NAMES.get(i), specs.get(i).name(),
                    "spec at index " + i);
        }
    }

    @Test
    void listSysTableSuffixesReturnsTheSameNames() {
        PaimonSystemTableOps ops = new PaimonSystemTableOps(NEVER_CALL);
        Set<String> suffixes = ops.listSysTableSuffixes(ConnectorTableId.of("db", "tbl"));
        Assertions.assertEquals(new LinkedHashSet<>(EXPECTED_NAMES), suffixes);
    }

    @Test
    void listSysTableSuffixesIsImmutable() {
        PaimonSystemTableOps ops = new PaimonSystemTableOps(NEVER_CALL);
        Set<String> suffixes = ops.listSysTableSuffixes(ConnectorTableId.of("db", "tbl"));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> suffixes.add("foo"));
    }

    @Test
    void getSysTableReturnsSpecForEachKnownName() {
        PaimonSystemTableOps ops = new PaimonSystemTableOps(NEVER_CALL);
        for (String name : EXPECTED_NAMES) {
            Optional<SysTableSpec> spec = ops.getSysTable(ConnectorTableId.of("db", "tbl"), name);
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
        PaimonSystemTableOps ops = new PaimonSystemTableOps(NEVER_CALL);
        Assertions.assertTrue(ops.getSysTable(ConnectorTableId.of("db", "tbl"), "SNAPSHOTS").isPresent());
        Assertions.assertTrue(ops.getSysTable(ConnectorTableId.of("db", "tbl"), "Aggregation_Fields").isPresent());
        Assertions.assertTrue(ops.getSysTable(ConnectorTableId.of("db", "tbl"), "TABLE_INDEXES").isPresent());
    }

    @Test
    void unknownSysTableReturnsEmpty() {
        PaimonSystemTableOps ops = new PaimonSystemTableOps(NEVER_CALL);
        // audit_log / binlog / ro / row_tracking depend on the main table's
        // user schema and are intentionally not published — see
        // PaimonSystemTableSchemas javadoc.
        Assertions.assertEquals(Optional.empty(), ops.getSysTable(ConnectorTableId.of("db", "tbl"), "audit_log"));
        Assertions.assertEquals(Optional.empty(), ops.getSysTable(ConnectorTableId.of("db", "tbl"), "binlog"));
        Assertions.assertEquals(Optional.empty(), ops.getSysTable(ConnectorTableId.of("db", "tbl"), "ro"));
        Assertions.assertEquals(Optional.empty(), ops.getSysTable(ConnectorTableId.of("db", "tbl"), "row_tracking"));
        Assertions.assertEquals(Optional.empty(), ops.getSysTable(ConnectorTableId.of("db", "tbl"), "totally-not-a-thing"));
    }

    @Test
    void supportsSysTableDelegatesToGetSysTable() {
        PaimonSystemTableOps ops = new PaimonSystemTableOps(NEVER_CALL);
        Assertions.assertTrue(ops.supportsSysTable(ConnectorTableId.of("db", "tbl"), "snapshots"));
        Assertions.assertTrue(ops.supportsSysTable(ConnectorTableId.of("db", "tbl"), "files"));
        Assertions.assertFalse(ops.supportsSysTable(ConnectorTableId.of("db", "tbl"), "audit_log"));
    }

    @Test
    void allSchemasAreNonEmpty() {
        PaimonSystemTableOps ops = new PaimonSystemTableOps(NEVER_CALL);
        for (SysTableSpec spec : ops.listSysTables(ConnectorTableId.of("db", "tbl"))) {
            Assertions.assertFalse(spec.schema().getColumns().isEmpty(),
                    spec.name() + " schema is empty");
            Assertions.assertEquals("PAIMON_METADATA", spec.schema().getTableFormatType());
        }
    }

    @Test
    void specsAreSharedAcrossLookups() {
        PaimonSystemTableOps ops = new PaimonSystemTableOps(NEVER_CALL);
        Assertions.assertSame(
                ops.getSysTable(ConnectorTableId.of("db", "tbl"), "snapshots").get(),
                ops.getSysTable(ConnectorTableId.of("db", "tbl"), "snapshots").get());
    }

    @Test
    void nullArgumentsRejected() {
        PaimonSystemTableOps ops = new PaimonSystemTableOps(NEVER_CALL);
        Assertions.assertThrows(NullPointerException.class,
                () -> ops.getSysTable(null, "snapshots"));
        Assertions.assertThrows(NullPointerException.class,
                () -> ops.getSysTable(ConnectorTableId.of("db", "tbl"), null));
        Assertions.assertThrows(NullPointerException.class,
                () -> ops.listSysTables(null));
        Assertions.assertThrows(NullPointerException.class,
                () -> ops.listSysTableSuffixes(null));
        Assertions.assertThrows(NullPointerException.class,
                () -> new PaimonSystemTableOps(null));
    }

    @Test
    void baseTableLoaderInvokedOnlyWhenScanResolved() {
        AtomicInteger calls = new AtomicInteger();
        PaimonSystemTableOps ops = new PaimonSystemTableOps((db, t) -> {
            calls.incrementAndGet();
            throw new IllegalStateException("loader called");
        });
        ops.listSysTables(ConnectorTableId.of("db", "tbl"));
        ops.getSysTable(ConnectorTableId.of("db", "tbl"), "snapshots");
        ops.listSysTableSuffixes(ConnectorTableId.of("db", "tbl"));
        Assertions.assertEquals(0, calls.get(),
                "spec construction and lookup must not load the base paimon table");
    }

    @Test
    void nativeFactoryProducesProviderForKnownName() {
        PaimonSystemTableOps ops = new PaimonSystemTableOps((db, t) -> null);
        SysTableSpec spec = ops.getSysTable(ConnectorTableId.of("db", "tbl"), "snapshots").orElseThrow();
        Assertions.assertNotNull(spec.nativeFactory().get().create(
                "db", "tbl", "snapshots", Optional.empty()));
    }

    @Test
    void schemaColumnCountMatchesPaimonMetadataLayout() {
        // Counts mirror paimon 1.3.x SDK TABLE_TYPE definitions, see
        // PaimonSystemTableSchemas. Any divergence is a paimon SDK change
        // that must be re-validated.
        PaimonSystemTableOps ops = new PaimonSystemTableOps(NEVER_CALL);
        Assertions.assertEquals(13,
                ops.getSysTable(ConnectorTableId.of("db", "tbl"), "snapshots").get().schema().getColumns().size());
        Assertions.assertEquals(7,
                ops.getSysTable(ConnectorTableId.of("db", "tbl"), "schemas").get().schema().getColumns().size());
        Assertions.assertEquals(2,
                ops.getSysTable(ConnectorTableId.of("db", "tbl"), "options").get().schema().getColumns().size());
        Assertions.assertEquals(7,
                ops.getSysTable(ConnectorTableId.of("db", "tbl"), "tags").get().schema().getColumns().size());
        Assertions.assertEquals(2,
                ops.getSysTable(ConnectorTableId.of("db", "tbl"), "branches").get().schema().getColumns().size());
        Assertions.assertEquals(2,
                ops.getSysTable(ConnectorTableId.of("db", "tbl"), "consumers").get().schema().getColumns().size());
        Assertions.assertEquals(5,
                ops.getSysTable(ConnectorTableId.of("db", "tbl"), "aggregation_fields").get().schema().getColumns().size());
        Assertions.assertEquals(18,
                ops.getSysTable(ConnectorTableId.of("db", "tbl"), "files").get().schema().getColumns().size());
        Assertions.assertEquals(7,
                ops.getSysTable(ConnectorTableId.of("db", "tbl"), "manifests").get().schema().getColumns().size());
        Assertions.assertEquals(5,
                ops.getSysTable(ConnectorTableId.of("db", "tbl"), "partitions").get().schema().getColumns().size());
        Assertions.assertEquals(5,
                ops.getSysTable(ConnectorTableId.of("db", "tbl"), "statistics").get().schema().getColumns().size());
        Assertions.assertEquals(6,
                ops.getSysTable(ConnectorTableId.of("db", "tbl"), "buckets").get().schema().getColumns().size());
        Assertions.assertEquals(7,
                ops.getSysTable(ConnectorTableId.of("db", "tbl"), "table_indexes").get().schema().getColumns().size());
    }

    @Test
    void emptyDatabaseAndTableArgumentsAccepted() {
        // Sys-table spec lookup is independent of the (db, table) identity for
        // paimon — every paimon main table exposes the same set of sys tables.
        PaimonSystemTableOps ops = new PaimonSystemTableOps(NEVER_CALL);
        Assertions.assertEquals(EXPECTED_NAMES.size(), ops.listSysTables(ConnectorTableId.of("", "")).size());
        Assertions.assertTrue(ops.getSysTable(ConnectorTableId.of("", ""), "tags").isPresent());
    }

    @Test
    void listSysTablesReturnsImmutableView() {
        PaimonSystemTableOps ops = new PaimonSystemTableOps(NEVER_CALL);
        List<SysTableSpec> specs = ops.listSysTables(ConnectorTableId.of("db", "tbl"));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> specs.add(null));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> specs.removeAll(Collections.emptyList()));
    }

    @Test
    void atLeastTenSysTablesPublished() {
        // M1-14 task brief: "至少实现 10 张".
        PaimonSystemTableOps ops = new PaimonSystemTableOps(NEVER_CALL);
        Assertions.assertTrue(ops.listSysTables(ConnectorTableId.of("db", "tbl")).size() >= 10);
    }
}
