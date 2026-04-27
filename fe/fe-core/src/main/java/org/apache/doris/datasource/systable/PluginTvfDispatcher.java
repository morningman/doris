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

package org.apache.doris.datasource.systable;

import org.apache.doris.connector.api.systable.TvfInvocation;
import org.apache.doris.nereids.trees.expressions.functions.table.PartitionValues;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * fe-core dispatcher mapping a plugin-emitted {@link TvfInvocation} (D6 §9.1)
 * onto a concrete fe-core
 * {@link org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction
 * TableValuedFunction}.
 *
 * <p>Plugins do not depend on fe-core, so they describe the TVF call as a
 * {@code (functionName, properties)} descriptor (see
 * {@link org.apache.doris.connector.api.systable.TvfSysTableInvoker
 * TvfSysTableInvoker}). When {@link
 * org.apache.doris.nereids.rules.analysis.BindRelation BindRelation} encounters
 * a plugin-managed sys table whose
 * {@link org.apache.doris.connector.api.systable.SysTableExecutionMode mode}
 * is {@link org.apache.doris.connector.api.systable.SysTableExecutionMode#TVF
 * TVF}, it delegates to {@link #toTableValuedFunction} to materialise the
 * fe-core TVF, which then becomes the body of a {@code LogicalTVFRelation}.</p>
 *
 * <p>This is the bridge that allows the M1-12 plugin route to reach the TVF
 * execution path. M1-15 publishes the only TVF mapping in use today
 * ({@code partition_values}), supporting the hive {@code $partitions} sys
 * table. New mappings should be registered here as plugins start emitting
 * additional metadata TVF names.</p>
 */
public final class PluginTvfDispatcher {

    private PluginTvfDispatcher() {
    }

    /**
     * Resolves {@code invocation} into a fe-core {@link TableValuedFunction}
     * positioned on {@code [catalogName, dbName, tableName]}. The plugin-side
     * properties are intentionally ignored when the function constructor is
     * fully derivable from {@code (catalog, db, table)} — keeping the catalog
     * name out of plugin code (the plugin has no notion of fe-core catalog
     * naming).
     *
     * @throws IllegalArgumentException if {@code invocation.functionName()}
     *         is not a known plugin TVF target.
     */
    public static TableValuedFunction toTableValuedFunction(
            String catalogName, String dbName, String tableName, TvfInvocation invocation) {
        Objects.requireNonNull(catalogName, "catalogName");
        Objects.requireNonNull(dbName, "dbName");
        Objects.requireNonNull(tableName, "tableName");
        Objects.requireNonNull(invocation, "invocation");
        String fn = invocation.functionName().toLowerCase(Locale.ROOT);
        switch (fn) {
            case "partition_values": {
                List<String> parts = ImmutableList.of(catalogName, dbName, tableName);
                return PartitionValues.create(parts);
            }
            default:
                throw new IllegalArgumentException(
                        "Unknown plugin TVF function name: " + invocation.functionName()
                                + " (only 'partition_values' is registered)");
        }
    }
}
