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
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;
import org.apache.doris.connector.api.scan.ConnectorScanRequest;

import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Plugin-side {@link ConnectorScanPlanProvider} for an Iceberg metadata
 * table (one of {@link MetadataTableType#SNAPSHOTS},
 * {@link MetadataTableType#HISTORY}, etc.).
 *
 * <p>Each instance lazily resolves the underlying
 * {@code MetadataTableUtils.createMetadataTableInstance(baseTable, type)}
 * via the supplied base-table loader (typically backed by the iceberg
 * plugin's {@code iceberg.table} {@link
 * org.apache.doris.connector.api.cache.MetaCacheHandle MetaCacheHandle}),
 * so cache-driven invalidation on the main table propagates to the sys
 * table without any extra wiring.</p>
 *
 * <p><b>Scan dispatch (M1-13 status):</b> the plugin SPI surface
 * ({@link org.apache.doris.connector.api.systable.SystemTableOps SystemTableOps}
 * → {@code NativeSysTableScanFactory} → this provider) is now in place,
 * but the fe-core ScanNode that translates {@link ConnectorScanRange}s
 * coming out of a sys table provider is still M1-15 work (see M1-12
 * handoff hook #2). Until that lands, {@link #planScan} returns an empty
 * range list and logs a one-time INFO. The metadata {@link Table} itself
 * is fully resolvable via {@link #resolveMetadataTable} for tests and
 * for downstream work (M1-15 ScanNode, M1-16 e2e validation).</p>
 */
public final class IcebergMetadataScanPlanProvider implements ConnectorScanPlanProvider {

    private static final Logger LOG = LogManager.getLogger(IcebergMetadataScanPlanProvider.class);

    private final String database;
    private final String table;
    private final MetadataTableType metadataTableType;
    private final Supplier<Table> baseTableSupplier;

    public IcebergMetadataScanPlanProvider(String database,
            String table,
            MetadataTableType metadataTableType,
            Supplier<Table> baseTableSupplier) {
        this.database = Objects.requireNonNull(database, "database");
        this.table = Objects.requireNonNull(table, "table");
        this.metadataTableType = Objects.requireNonNull(metadataTableType, "metadataTableType");
        this.baseTableSupplier = Objects.requireNonNull(baseTableSupplier, "baseTableSupplier");
    }

    public String database() {
        return database;
    }

    public String table() {
        return table;
    }

    public MetadataTableType metadataTableType() {
        return metadataTableType;
    }

    /**
     * Resolves the iceberg metadata {@link Table} via the SDK on top of the
     * base table loaded through {@link #baseTableSupplier}. Heavy operation —
     * exposed primarily for the fe-core ScanNode (M1-15) and tests.
     */
    public Table resolveMetadataTable() {
        Table base = baseTableSupplier.get();
        Objects.requireNonNull(base, "base iceberg table");
        return MetadataTableUtils.createMetadataTableInstance(base, metadataTableType);
    }

    @Override
    public ConnectorScanRangeType getScanRangeType() {
        return ConnectorScanRangeType.FILE_SCAN;
    }

    @Override
    public List<ConnectorScanRange> planScan(ConnectorScanRequest req) {
        Objects.requireNonNull(req, "req");
        // M1-13: SPI surface is published; the fe-core ScanNode that
        // consumes plugin sys-table ranges is M1-15 (see M1-12 handoff hook #2).
        // resolveMetadataTable() above is the integration point; until M1-15
        // wires range generation, return an empty plan to keep the planner safe.
        LOG.info("Iceberg sys table {}.{}${} resolved via plugin; range generation pending M1-15.",
                database, table, metadataTableType.name().toLowerCase(java.util.Locale.ROOT));
        return Collections.emptyList();
    }
}
