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

import org.apache.doris.connector.api.systable.TvfInvocation;
import org.apache.doris.connector.api.systable.TvfSysTableInvoker;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * {@link TvfSysTableInvoker} for Hive {@code $partitions}.
 *
 * <p>Returns a {@link TvfInvocation} that names the fe-core
 * {@code partition_values} metadata TVF and carries the {@code database} /
 * {@code table} properties. The catalog name is injected by the engine when
 * binding the invocation to the concrete fe-core
 * {@link org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction
 * TableValuedFunction} (see {@code PluginTvfDispatcher} in fe-core), because
 * the plugin layer is intentionally agnostic of the Doris-side catalog name.</p>
 */
final class HivePartitionsTvfInvoker implements TvfSysTableInvoker {

    /** TVF name routed to fe-core's {@code partition_values}. */
    static final String FUNCTION_NAME = "partition_values";

    static final String PROP_DATABASE = "database";
    static final String PROP_TABLE = "table";

    @Override
    public TvfInvocation resolve(String database,
            String table,
            String sysTableName,
            Optional<ConnectorTableVersion> version) {
        Objects.requireNonNull(database, "database");
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(sysTableName, "sysTableName");
        Objects.requireNonNull(version, "version");
        Map<String, String> props = new LinkedHashMap<>();
        props.put(PROP_DATABASE, database);
        props.put(PROP_TABLE, table);
        return new TvfInvocation(FUNCTION_NAME, props);
    }
}
