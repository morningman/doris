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

package org.apache.doris.connector.api.systable;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SystemTableOpsTest {

    private static ConnectorTableSchema oneColSchema(String name) {
        ConnectorColumn c = new ConnectorColumn(name, ConnectorType.of("bigint"),
                null, true, null);
        return new ConnectorTableSchema(name, Collections.singletonList(c), "x",
                Collections.emptyMap());
    }

    /** Default methods return empty and supportsSysTable delegates to getSysTable. */
    @Test
    public void defaultsReturnEmpty() {
        SystemTableOps ops = new SystemTableOps() {
        };
        Assertions.assertTrue(ops.listSysTables("db", "t").isEmpty());
        Assertions.assertTrue(ops.getSysTable("db", "t", "snapshots").isEmpty());
        Assertions.assertFalse(ops.supportsSysTable("db", "t", "snapshots"));
    }

    @Test
    public void overrideListAndLookup() {
        Map<String, SysTableSpec> registry = new HashMap<>();
        registry.put("snapshots", SysTableSpec.builder()
                .name("snapshots")
                .schema(oneColSchema("snapshot_id"))
                .mode(SysTableExecutionMode.NATIVE)
                .nativeFactory((db, tbl, sys, v) -> (ConnectorScanPlanProvider) null)
                .build());

        SystemTableOps ops = new SystemTableOps() {
            @Override
            public List<SysTableSpec> listSysTables(String database, String table) {
                return List.copyOf(registry.values());
            }

            @Override
            public Optional<SysTableSpec> getSysTable(String database, String table, String sysTableName) {
                return Optional.ofNullable(registry.get(sysTableName));
            }
        };

        Assertions.assertEquals(1, ops.listSysTables("db", "t").size());
        Assertions.assertTrue(ops.getSysTable("db", "t", "snapshots").isPresent());
        Assertions.assertTrue(ops.supportsSysTable("db", "t", "snapshots"));
        Assertions.assertFalse(ops.supportsSysTable("db", "t", "no_such"));
    }
}
