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

import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.systable.NativeSysTableScanFactory;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;

import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Table;

import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * Single {@link NativeSysTableScanFactory} implementation parameterised by
 * {@link MetadataTableType}, used by all seven specs registered in
 * {@link IcebergSystemTableOps}.
 *
 * <p>Per the spec, {@code create(...)} must be lightweight: the heavy
 * iceberg metadata-table SDK call is deferred to
 * {@link IcebergMetadataScanPlanProvider#resolveMetadataTable()} on first
 * scan-plan request.</p>
 */
final class IcebergMetadataScanFactory implements NativeSysTableScanFactory {

    private final MetadataTableType metadataTableType;
    private final BiFunction<String, String, Table> baseTableLoader;

    IcebergMetadataScanFactory(MetadataTableType metadataTableType,
            BiFunction<String, String, Table> baseTableLoader) {
        this.metadataTableType = Objects.requireNonNull(metadataTableType, "metadataTableType");
        this.baseTableLoader = Objects.requireNonNull(baseTableLoader, "baseTableLoader");
    }

    MetadataTableType metadataTableType() {
        return metadataTableType;
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
        // Iceberg snapshots / history / refs / partitions / files / entries / manifests
        // are inherently full-history views — the spec for these tables sets
        // acceptsTableVersion(false), so fe-core passes Optional.empty() here
        // (see M1-12 wrapper). The argument is captured for completeness only.
        return new IcebergMetadataScanPlanProvider(
                database, table, metadataTableType,
                () -> baseTableLoader.apply(database, table));
    }
}
