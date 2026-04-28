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

package org.apache.doris.connector.paimon.mtmv;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorTableId;
import org.apache.doris.connector.api.mtmv.ConnectorMtmvSnapshot;
import org.apache.doris.connector.api.mtmv.ConnectorPartitionItem;
import org.apache.doris.connector.api.mtmv.ConnectorPartitionType;
import org.apache.doris.connector.api.mtmv.MtmvOps;
import org.apache.doris.connector.api.mtmv.MtmvRefreshHint;
import org.apache.doris.connector.api.timetravel.ConnectorMvccSnapshot;
import org.apache.doris.connector.paimon.PaimonTypeMapping;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * D8 — Paimon {@link MtmvOps} implementation built on top of the Paimon
 * SDK {@link Catalog} / {@link Table} APIs.
 *
 * <p>Wraps the connector's catalog handle so that materialized views can use
 * paimon tables as base tables without depending on any fe-core type. The
 * {@code PluginDrivenMtmvBridge} in fe-core adapts the values returned here
 * into the legacy {@code MTMVRelatedTableIf} surface.</p>
 *
 * <p>Snapshot semantics are intentionally simpler than the legacy
 * {@code PaimonExternalTable}: both the partition-level and table-level
 * snapshots are exposed as
 * {@link ConnectorMtmvSnapshot.SnapshotIdMtmvSnapshot} carrying
 * {@code Table.latestSnapshot().id()} (or {@code -1} when the table has no
 * snapshot). Per-partition {@code lastFileCreationTime} would require lifting
 * fe-core's snapshot cache into the plugin and is deferred — see the M2-09
 * handoff for the trade-off discussion.</p>
 *
 * <p>Partition handling: paimon partitions are physical {@code key=value}
 * tuples. {@link #listPartitions} emits one
 * {@link ConnectorPartitionItem.ListPartitionItem} per
 * {@link Catalog#listPartitions(Identifier)} entry, keyed by the
 * {@code k1=v1/k2=v2/...} string built from {@link Table#partitionKeys()}
 * order so that bridge change-detection is deterministic across snapshots.
 * No {@link ConnectorPartitionItem.RangePartitionItem} is emitted (the bridge
 * SPI gap from M2-06 still rejects them).</p>
 *
 * <p>{@link #isValidRelatedTable} returns {@code true} as long as the table
 * loads successfully — paimon has no equivalent of iceberg's transform
 * restriction. Any loader exception collapses to {@code false} so the
 * planner falls back to a full-table refresh.</p>
 */
public final class PaimonMtmvOps implements MtmvOps {

    private static final Logger LOG = LogManager.getLogger(PaimonMtmvOps.class);

    private final Catalog catalog;
    private final PaimonTypeMapping.Options typeMappingOptions;

    public PaimonMtmvOps(Catalog catalog) {
        this(catalog, new PaimonTypeMapping.Options(false, false));
    }

    public PaimonMtmvOps(Catalog catalog, PaimonTypeMapping.Options typeMappingOptions) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.typeMappingOptions = Objects.requireNonNull(typeMappingOptions, "typeMappingOptions");
    }

    @Override
    public Map<String, ConnectorPartitionItem> listPartitions(
            ConnectorTableId id, Optional<ConnectorMvccSnapshot> snapshot) {
        Objects.requireNonNull(id, "id");
        String database = id.database();
        String table = id.table();
        Table paimonTable = loadTable(database, table);
        List<String> partitionKeys = paimonTable.partitionKeys();
        if (partitionKeys == null || partitionKeys.isEmpty()) {
            return Collections.singletonMap("", new ConnectorPartitionItem.UnpartitionedItem());
        }
        List<Partition> partitions = listPartitionEntries(database, table);
        Map<String, ConnectorPartitionItem> out = new LinkedHashMap<>();
        for (Partition partition : partitions) {
            Map<String, String> spec = partition.spec();
            if (spec == null) {
                continue;
            }
            List<String> values = new ArrayList<>(partitionKeys.size());
            StringBuilder name = new StringBuilder();
            for (int i = 0; i < partitionKeys.size(); i++) {
                String key = partitionKeys.get(i);
                String value = spec.get(key);
                String rendered = value == null ? "null" : value;
                values.add(rendered);
                if (i > 0) {
                    name.append('/');
                }
                name.append(key).append('=').append(rendered);
            }
            String partitionName = name.toString();
            if (out.containsKey(partitionName)) {
                continue;
            }
            out.put(partitionName, new ConnectorPartitionItem.ListPartitionItem(
                    Collections.singletonList(values)));
        }
        return out;
    }

    @Override
    public ConnectorPartitionType getPartitionType(
            ConnectorTableId id, Optional<ConnectorMvccSnapshot> snapshot) {
        Objects.requireNonNull(id, "id");
        String database = id.database();
        String table = id.table();
        Table paimonTable = loadTable(database, table);
        List<String> partitionKeys = paimonTable.partitionKeys();
        if (partitionKeys == null || partitionKeys.isEmpty()) {
            return ConnectorPartitionType.UNPARTITIONED;
        }
        return ConnectorPartitionType.LIST;
    }

    @Override
    public Set<String> getPartitionColumnNames(
            ConnectorTableId id, Optional<ConnectorMvccSnapshot> snapshot) {
        Objects.requireNonNull(id, "id");
        String database = id.database();
        String table = id.table();
        Table paimonTable = loadTable(database, table);
        List<String> partitionKeys = paimonTable.partitionKeys();
        if (partitionKeys == null || partitionKeys.isEmpty()) {
            return Collections.emptySet();
        }
        Set<String> names = new LinkedHashSet<>(partitionKeys.size());
        for (String key : partitionKeys) {
            names.add(key.toLowerCase());
        }
        return names;
    }

    @Override
    public List<ConnectorColumn> getPartitionColumns(
            ConnectorTableId id, Optional<ConnectorMvccSnapshot> snapshot) {
        Objects.requireNonNull(id, "id");
        String database = id.database();
        String table = id.table();
        Table paimonTable = loadTable(database, table);
        List<String> partitionKeys = paimonTable.partitionKeys();
        if (partitionKeys == null || partitionKeys.isEmpty()) {
            return Collections.emptyList();
        }
        RowType rowType = paimonTable.rowType();
        Map<String, DataField> byName = new LinkedHashMap<>();
        for (DataField field : rowType.getFields()) {
            byName.put(field.name(), field);
        }
        List<ConnectorColumn> columns = new ArrayList<>(partitionKeys.size());
        for (String key : partitionKeys) {
            DataField field = byName.get(key);
            if (field == null) {
                throw new IllegalStateException(
                        "Paimon partition key '" + key + "' not present in row type for "
                                + database + "." + table);
            }
            columns.add(new ConnectorColumn(
                    field.name().toLowerCase(),
                    PaimonTypeMapping.toConnectorType(field.type(), typeMappingOptions),
                    field.description() == null ? "" : field.description(),
                    field.type().isNullable(),
                    null));
        }
        return columns;
    }

    @Override
    public ConnectorMtmvSnapshot getPartitionSnapshot(
            ConnectorTableId id, String partitionName,
            MtmvRefreshHint hint, Optional<ConnectorMvccSnapshot> snapshot) {
        Objects.requireNonNull(id, "id");
        String database = id.database();
        String table = id.table();
        Objects.requireNonNull(partitionName, "partitionName");
        Table paimonTable = loadTable(database, table);
        return new ConnectorMtmvSnapshot.SnapshotIdMtmvSnapshot(latestSnapshotId(paimonTable));
    }

    @Override
    public ConnectorMtmvSnapshot getTableSnapshot(
            ConnectorTableId id,
            MtmvRefreshHint hint, Optional<ConnectorMvccSnapshot> snapshot) {
        Objects.requireNonNull(id, "id");
        String database = id.database();
        String table = id.table();
        Table paimonTable = loadTable(database, table);
        return new ConnectorMtmvSnapshot.SnapshotIdMtmvSnapshot(latestSnapshotId(paimonTable));
    }

    @Override
    public long getNewestUpdateVersionOrTime(ConnectorTableId id) {
        Objects.requireNonNull(id, "id");
        String database = id.database();
        String table = id.table();
        try {
            Table paimonTable = loadTable(database, table);
            return latestSnapshotId(paimonTable);
        } catch (RuntimeException e) {
            LOG.debug("getNewestUpdateVersionOrTime({}.{}) returning -1 due to error: {}",
                    database, table, e.getMessage());
            return -1L;
        }
    }

    @Override
    public boolean isPartitionColumnAllowNull(ConnectorTableId id) {
        Objects.requireNonNull(id, "id");
        // Paimon writes a sentinel "null" partition rather than rejecting NULLs,
        // matching the legacy PaimonExternalTable contract.
        return true;
    }

    @Override
    public boolean isValidRelatedTable(ConnectorTableId id) {
        Objects.requireNonNull(id, "id");
        String database = id.database();
        String table = id.table();
        try {
            loadTable(database, table);
            return true;
        } catch (RuntimeException e) {
            LOG.debug("isValidRelatedTable({}.{}) returning false due to error: {}",
                    database, table, e.getMessage());
            return false;
        }
    }

    @Override
    public boolean needAutoRefresh(ConnectorTableId id) {
        Objects.requireNonNull(id, "id");
        return true;
    }

    // ---------------------------------------------------------------- helpers

    private Table loadTable(String database, String table) {
        try {
            Table loaded = catalog.getTable(Identifier.create(database, table));
            if (loaded == null) {
                throw new IllegalStateException(
                        "Paimon catalog returned null for " + database + "." + table);
            }
            return loaded;
        } catch (Catalog.TableNotExistException e) {
            throw new IllegalStateException(
                    "Paimon table not found: " + database + "." + table, e);
        }
    }

    private List<Partition> listPartitionEntries(String database, String table) {
        try {
            List<Partition> partitions = catalog.listPartitions(Identifier.create(database, table));
            return partitions == null ? Collections.emptyList() : partitions;
        } catch (Catalog.TableNotExistException e) {
            throw new IllegalStateException(
                    "Paimon table not found while listing partitions: "
                            + database + "." + table, e);
        }
    }

    private static long latestSnapshotId(Table table) {
        Optional<Snapshot> snap = table.latestSnapshot();
        return snap.map(Snapshot::id).orElse(-1L);
    }
}
