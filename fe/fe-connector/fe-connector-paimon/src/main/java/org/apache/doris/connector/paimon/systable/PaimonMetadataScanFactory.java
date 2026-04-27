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

import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.systable.NativeSysTableScanFactory;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;

import org.apache.paimon.table.Table;

import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * Single {@link NativeSysTableScanFactory} implementation parameterised by
 * a paimon system-table name (e.g. {@code "snapshots"}, {@code "files"}),
 * used by every spec registered in {@link PaimonSystemTableOps}.
 *
 * <p>Per the spec, {@link #create} must be lightweight: the heavy paimon
 * {@link org.apache.paimon.table.system.SystemTableLoader#load} call is
 * deferred to
 * {@link PaimonMetadataScanPlanProvider#resolveSystemTable()} on first
 * scan-plan request.</p>
 */
final class PaimonMetadataScanFactory implements NativeSysTableScanFactory {

    private final String systemTableName;
    private final BiFunction<String, String, Table> baseTableLoader;

    PaimonMetadataScanFactory(String systemTableName,
                              BiFunction<String, String, Table> baseTableLoader) {
        this.systemTableName = Objects.requireNonNull(systemTableName, "systemTableName")
                .toLowerCase(Locale.ROOT);
        this.baseTableLoader = Objects.requireNonNull(baseTableLoader, "baseTableLoader");
    }

    String systemTableName() {
        return systemTableName;
    }

    @Override
    public ConnectorScanPlanProvider create(String database,
                                            String table,
                                            String sysTableName,
                                            Optional<ConnectorTableVersion> version) {
        Objects.requireNonNull(database, "database");
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(sysTableName, "sysTableName");
        Objects.requireNonNull(version, "version");
        // Paimon snapshots / schemas / options / tags / branches / consumers /
        // aggregation_fields / files / manifests / partitions / statistics /
        // buckets / table_indexes are all full-history views; their specs set
        // acceptsTableVersion(false), so fe-core passes Optional.empty() here
        // (see M1-12 wrapper). The argument is captured for forward-compat only.
        return new PaimonMetadataScanPlanProvider(
                database, table, systemTableName,
                () -> baseTableLoader.apply(database, table));
    }
}
