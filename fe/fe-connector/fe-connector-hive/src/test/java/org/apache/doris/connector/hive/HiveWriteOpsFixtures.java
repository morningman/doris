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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared fixture builders for the M3-08 hive write-path tests.
 *
 * <p>Mocking the {@code HmsClient + HmsWriteOps} pair produces fairly
 * verbose setup; centralising it here keeps individual tests focused
 * on the specific assertion under test.</p>
 */
final class HiveWriteOpsFixtures {

    private HiveWriteOpsFixtures() {
    }

    /** Builds a non-ACID HMS table descriptor with the supplied partition keys. */
    static HmsTableInfo nonAcidTable(String db, String table, List<String> partKeys) {
        return baseTableBuilder(db, table, partKeys).parameters(new HashMap<>()).build();
    }

    /** Builds a full-ACID HMS table descriptor (transactional=true). */
    static HmsTableInfo acidTable(String db, String table, List<String> partKeys) {
        Map<String, String> params = new HashMap<>();
        params.put(HiveAcidUtil.TRANSACTIONAL, "true");
        return baseTableBuilder(db, table, partKeys).parameters(params).build();
    }

    /**
     * Builds an insert-only ACID HMS table — treated as non-ACID by
     * {@link HiveAcidUtil#isFullAcidTable} so the plugin runs the
     * non-ACID path.
     */
    static HmsTableInfo insertOnlyAcidTable(String db, String table, List<String> partKeys) {
        Map<String, String> params = new HashMap<>();
        params.put(HiveAcidUtil.TRANSACTIONAL, "true");
        params.put(HiveAcidUtil.TRANSACTIONAL_PROPERTIES, "insert_only");
        return baseTableBuilder(db, table, partKeys).parameters(params).build();
    }

    private static HmsTableInfo.Builder baseTableBuilder(String db, String table, List<String> partKeys) {
        List<ConnectorColumn> partCols = new ArrayList<>();
        for (String key : partKeys) {
            partCols.add(new ConnectorColumn(key, ConnectorType.of("STRING"), null, true, null));
        }
        return HmsTableInfo.builder()
                .dbName(db)
                .tableName(table)
                .tableType("MANAGED_TABLE")
                .location("file:///wh/" + db + "/" + table)
                .inputFormat("org.apache.hadoop.mapred.TextInputFormat")
                .outputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
                .serializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
                .columns(Arrays.asList(
                        new ConnectorColumn("id", ConnectorType.of("BIGINT"), null, true, null),
                        new ConnectorColumn("name", ConnectorType.of("STRING"), null, true, null)))
                .partitionKeys(partCols)
                .sdParameters(new HashMap<>());
    }

    static HiveTableHandle handleFor(HmsTableInfo info) {
        List<String> partKeys = info.getPartitionKeys() == null ? Collections.emptyList()
                : new ArrayList<>();
        if (info.getPartitionKeys() != null) {
            for (ConnectorColumn col : info.getPartitionKeys()) {
                partKeys.add(col.getName());
            }
        }
        return new HiveTableHandle.Builder(info.getDbName(), info.getTableName(), HiveTableType.HIVE)
                .inputFormat(info.getInputFormat())
                .serializationLib(info.getSerializationLib())
                .location(info.getLocation())
                .partitionKeyNames(partKeys)
                .sdParameters(info.getSdParameters())
                .tableParameters(info.getParameters())
                .build();
    }

    static HmsPartitionInfo partition(List<String> values, String location) {
        return new HmsPartitionInfo(values, location,
                "org.apache.hadoop.mapred.TextInputFormat",
                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                new HashMap<>());
    }
}
