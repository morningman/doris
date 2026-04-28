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

package org.apache.doris.connector.hive.systable;

import org.apache.doris.connector.api.ConnectorTableId;
import org.apache.doris.connector.api.systable.SysTableExecutionMode;
import org.apache.doris.connector.api.systable.SysTableSpec;
import org.apache.doris.connector.api.systable.TvfInvocation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

class HiveSystemTableOpsTest {

    @Test
    void publishesExactlyOneSpecForPartitions() {
        HiveSystemTableOps ops = new HiveSystemTableOps();
        List<SysTableSpec> specs = ops.listSysTables(ConnectorTableId.of("db", "tbl"));
        Assertions.assertEquals(1, specs.size());
        Assertions.assertEquals("partitions", specs.get(0).name());
    }

    @Test
    void partitionsSpecDeclaresTvfMode() {
        HiveSystemTableOps ops = new HiveSystemTableOps();
        SysTableSpec spec = ops.getSysTable(ConnectorTableId.of("db", "tbl"), "partitions").orElseThrow();
        Assertions.assertEquals(SysTableExecutionMode.TVF, spec.mode(),
                "Hive $partitions must use TVF execution mode (M1-15)");
        Assertions.assertTrue(spec.tvfInvoker().isPresent());
        Assertions.assertFalse(spec.nativeFactory().isPresent());
        Assertions.assertFalse(spec.scanPlanProviderRef().isPresent());
    }

    @Test
    void partitionsSpecAcceptsNoTableVersion() {
        HiveSystemTableOps ops = new HiveSystemTableOps();
        SysTableSpec spec = ops.getSysTable(ConnectorTableId.of("db", "tbl"), "partitions").orElseThrow();
        Assertions.assertFalse(spec.acceptsTableVersion(),
                "Hive partition listings are always latest-state; version must be stripped.");
    }

    @Test
    void partitionsSpecHasPlaceholderSchema() {
        HiveSystemTableOps ops = new HiveSystemTableOps();
        SysTableSpec spec = ops.getSysTable(ConnectorTableId.of("db", "tbl"), "partitions").orElseThrow();
        Assertions.assertEquals(1, spec.schema().getColumns().size());
        Assertions.assertEquals("partition_name", spec.schema().getColumns().get(0).getName());
    }

    @Test
    void getSysTableIsCaseInsensitive() {
        HiveSystemTableOps ops = new HiveSystemTableOps();
        Assertions.assertTrue(ops.getSysTable(ConnectorTableId.of("db", "tbl"), "PARTITIONS").isPresent());
        Assertions.assertTrue(ops.getSysTable(ConnectorTableId.of("db", "tbl"), "Partitions").isPresent());
    }

    @Test
    void getSysTableUnknownReturnsEmpty() {
        HiveSystemTableOps ops = new HiveSystemTableOps();
        Assertions.assertEquals(Optional.empty(), ops.getSysTable(ConnectorTableId.of("db", "tbl"), "snapshots"));
        Assertions.assertEquals(Optional.empty(), ops.getSysTable(ConnectorTableId.of("db", "tbl"), "history"));
    }

    @Test
    void listSysTableSuffixesIsImmutableAndExposesPartitions() {
        HiveSystemTableOps ops = new HiveSystemTableOps();
        Set<String> suffixes = ops.listSysTableSuffixes(ConnectorTableId.of("db", "tbl"));
        Assertions.assertEquals(Set.of("partitions"), suffixes);
        Assertions.assertThrows(UnsupportedOperationException.class, () -> suffixes.add("foo"));
    }

    @Test
    void listSysTablesReturnsImmutable() {
        HiveSystemTableOps ops = new HiveSystemTableOps();
        List<SysTableSpec> specs = ops.listSysTables(ConnectorTableId.of("db", "tbl"));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> specs.add(specs.get(0)));
    }

    @Test
    void listSysTablesNpeOnNullArgs() {
        HiveSystemTableOps ops = new HiveSystemTableOps();
        Assertions.assertThrows(NullPointerException.class, () -> ops.listSysTables(null));
        Assertions.assertThrows(NullPointerException.class, () -> ops.getSysTable(null, "partitions"));
        Assertions.assertThrows(NullPointerException.class, () -> ops.getSysTable(ConnectorTableId.of("d", "t"), null));
        Assertions.assertThrows(NullPointerException.class, () -> ops.listSysTableSuffixes(null));
    }

    @Test
    void invokerProducesPartitionValuesInvocationWithDbAndTable() {
        HiveSystemTableOps ops = new HiveSystemTableOps();
        SysTableSpec spec = ops.getSysTable(ConnectorTableId.of("db", "tbl"), "partitions").orElseThrow();
        TvfInvocation inv = spec.tvfInvoker().orElseThrow()
                .resolve("default", "orders", "partitions", Optional.empty());
        Assertions.assertEquals("partition_values", inv.functionName());
        Assertions.assertEquals("default", inv.properties().get("database"));
        Assertions.assertEquals("orders", inv.properties().get("table"));
    }

    @Test
    void multipleListInvocationsReturnEqualSpecsAcrossDbTable() {
        HiveSystemTableOps ops = new HiveSystemTableOps();
        SysTableSpec a = ops.getSysTable(ConnectorTableId.of("db1", "t1"), "partitions").orElseThrow();
        SysTableSpec b = ops.getSysTable(ConnectorTableId.of("db2", "t2"), "partitions").orElseThrow();
        // Specs are static — same instance should be returned regardless of db/table.
        Assertions.assertSame(a, b);
    }

    @Test
    void schemaTypeNameIsHiveMetadata() {
        HiveSystemTableOps ops = new HiveSystemTableOps();
        SysTableSpec spec = ops.getSysTable(ConnectorTableId.of("db", "tbl"), "partitions").orElseThrow();
        Assertions.assertEquals("HIVE_METADATA", spec.schema().getTableFormatType());
    }
}
