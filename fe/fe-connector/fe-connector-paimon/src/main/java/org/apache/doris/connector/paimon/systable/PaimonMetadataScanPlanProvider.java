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
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;
import org.apache.doris.connector.api.scan.ConnectorScanRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.system.SystemTableLoader;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Plugin-side {@link ConnectorScanPlanProvider} for a paimon system table
 * (one of {@code snapshots / schemas / options / tags / branches /
 * consumers / aggregation_fields / files / manifests / partitions /
 * statistics / buckets / table_indexes}).
 *
 * <p>Each instance lazily resolves the underlying paimon sys-table
 * {@link Table} via {@link SystemTableLoader#load(String, FileStoreTable)}
 * on the {@link FileStoreTable} returned by the supplied base-table loader
 * (typically backed by the paimon plugin's {@code paimon.table}
 * {@link org.apache.doris.connector.api.cache.MetaCacheHandle MetaCacheHandle},
 * see M1-11), so cache-driven invalidation on the main table propagates to
 * the sys table without any extra wiring.</p>
 *
 * <p><b>Scan dispatch (M1-14 status):</b> the plugin SPI surface
 * ({@link org.apache.doris.connector.api.systable.SystemTableOps} →
 * {@link PaimonMetadataScanFactory} → this provider) is now in place, but
 * the fe-core ScanNode that translates {@link ConnectorScanRange}s coming
 * out of a sys-table provider is still M1-15 work (mirrors the M1-13
 * iceberg deviation; see the M1-12 handoff hook #2). Until that lands,
 * {@link #planScan} returns an empty range list and logs a one-time INFO.
 * The metadata {@link Table} itself is fully resolvable via
 * {@link #resolveSystemTable} for tests and downstream work.</p>
 */
public final class PaimonMetadataScanPlanProvider implements ConnectorScanPlanProvider {

    private static final Logger LOG = LogManager.getLogger(PaimonMetadataScanPlanProvider.class);

    private final String database;
    private final String table;
    private final String systemTableName;
    private final Supplier<Table> baseTableSupplier;

    public PaimonMetadataScanPlanProvider(String database,
                                          String table,
                                          String systemTableName,
                                          Supplier<Table> baseTableSupplier) {
        this.database = Objects.requireNonNull(database, "database");
        this.table = Objects.requireNonNull(table, "table");
        this.systemTableName = Objects.requireNonNull(systemTableName, "systemTableName")
                .toLowerCase(Locale.ROOT);
        this.baseTableSupplier = Objects.requireNonNull(baseTableSupplier, "baseTableSupplier");
    }

    public String database() {
        return database;
    }

    public String table() {
        return table;
    }

    public String systemTableName() {
        return systemTableName;
    }

    /**
     * Resolves the paimon system {@link Table} via
     * {@link SystemTableLoader#load(String, FileStoreTable)} on top of the
     * base table loaded through the supplier. Heavy operation — exposed
     * primarily for the fe-core ScanNode (M1-15) and tests.
     *
     * <p>Throws if the supplier returns null or returns a non-{@link
     * FileStoreTable} (paimon's sys-table loader requires a concrete
     * file-store base).</p>
     */
    public Table resolveSystemTable() {
        Table base = baseTableSupplier.get();
        Objects.requireNonNull(base, "base paimon table");
        if (!(base instanceof FileStoreTable)) {
            throw new IllegalStateException(
                    "paimon sys-table base must be a FileStoreTable, got "
                            + base.getClass().getName());
        }
        Table loaded = SystemTableLoader.load(systemTableName, (FileStoreTable) base);
        if (loaded == null) {
            throw new IllegalStateException(
                    "paimon SystemTableLoader returned null for sys table: " + systemTableName);
        }
        return loaded;
    }

    @Override
    public ConnectorScanRangeType getScanRangeType() {
        return ConnectorScanRangeType.FILE_SCAN;
    }

    @Override
    public List<ConnectorScanRange> planScan(ConnectorScanRequest req) {
        Objects.requireNonNull(req, "req");
        // M1-14: SPI surface is published; the fe-core ScanNode that
        // consumes plugin sys-table ranges is M1-15 (see M1-12 hook #2,
        // mirrors M1-13 iceberg behaviour). resolveSystemTable() above is
        // the integration point; until M1-15 wires range generation,
        // return an empty plan to keep the planner safe.
        LOG.info("Paimon sys table {}.{}${} resolved via plugin; range generation pending M1-15.",
                database, table, systemTableName);
        return Collections.emptyList();
    }
}
