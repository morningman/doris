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

package org.apache.doris.connector.iceberg.mtmv;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.mtmv.ConnectorMtmvSnapshot;
import org.apache.doris.connector.api.mtmv.ConnectorPartitionItem;
import org.apache.doris.connector.api.mtmv.ConnectorPartitionType;
import org.apache.doris.connector.api.mtmv.MtmvOps;
import org.apache.doris.connector.api.mtmv.MtmvRefreshHint;
import org.apache.doris.connector.api.timetravel.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;
import org.apache.doris.connector.iceberg.IcebergTypeMapping;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * D8 — Iceberg {@link MtmvOps} implementation built on top of the Iceberg
 * SDK {@link Table} API.
 *
 * <p>Wraps the connector's table loader so that materialized views can use
 * Iceberg tables as base tables without depending on any fe-core type. The
 * {@code PluginDrivenMtmvBridge} in fe-core adapts the values returned here
 * into the legacy {@code MTMVRelatedTableIf} surface.</p>
 *
 * <p>Snapshot semantics mirror the legacy {@code IcebergDlaTable}: both the
 * partition-level and table-level snapshots are exposed as
 * {@link ConnectorMtmvSnapshot.SnapshotIdMtmvSnapshot} carrying the iceberg
 * snapshot id (the bridge maps it to {@code MTMVSnapshotIdSnapshot}).</p>
 *
 * <p>{@link #isValidRelatedTable} replicates the legacy gating exactly: the
 * iceberg table is a valid MV base table only when every historical partition
 * spec has zero or one field, all transforms are {@code identity / year /
 * month / day / hour}, and (for partitioned variants) every spec uses the
 * same source column. Multi-field specs and {@code bucket / truncate / void}
 * transforms all collapse to {@code false} so the planner falls back to a
 * full-table refresh.</p>
 *
 * <p>SPI gap (tracked in M2-06 handoff): the bridge currently throws
 * {@code UnsupportedOperationException} for
 * {@link ConnectorPartitionItem.ListPartitionItem} and
 * {@link ConnectorPartitionItem.RangePartitionItem} because it cannot build
 * typed {@code PartitionKey}s from the SPI's plain string values. This class
 * still emits {@link ConnectorPartitionItem.ListPartitionItem} so that a
 * future bridge enhancement does not need plugin-side changes; range emission
 * for {@code month/day/hour} transforms is deferred until typed literal
 * support lands on {@link ConnectorPartitionItem}.</p>
 */
public final class IcebergMtmvOps implements MtmvOps {

    private static final Logger LOG = LogManager.getLogger(IcebergMtmvOps.class);

    static final String TRANSFORM_IDENTITY = "identity";
    static final String TRANSFORM_YEAR = "year";
    static final String TRANSFORM_MONTH = "month";
    static final String TRANSFORM_DAY = "day";
    static final String TRANSFORM_HOUR = "hour";

    private static final Set<String> ALLOWED_TRANSFORMS = Set.of(
            TRANSFORM_IDENTITY, TRANSFORM_YEAR, TRANSFORM_MONTH, TRANSFORM_DAY, TRANSFORM_HOUR);

    private final BiFunction<String, String, Table> tableLoader;

    public IcebergMtmvOps(BiFunction<String, String, Table> tableLoader) {
        this.tableLoader = Objects.requireNonNull(tableLoader, "tableLoader");
    }

    @Override
    public Map<String, ConnectorPartitionItem> listPartitions(
            String database, String table, Optional<ConnectorMvccSnapshot> snapshot) {
        Table icebergTable = loadTable(database, table);
        PartitionSpec spec = icebergTable.spec();
        if (spec.isUnpartitioned()) {
            return Collections.singletonMap("", new ConnectorPartitionItem.UnpartitionedItem());
        }
        TableScan scan = newScan(icebergTable, snapshot);
        Map<String, ConnectorPartitionItem> out = new LinkedHashMap<>();
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            for (FileScanTask task : tasks) {
                StructLike struct = task.partition();
                if (struct == null) {
                    continue;
                }
                String name = spec.partitionToPath(struct);
                if (out.containsKey(name)) {
                    continue;
                }
                List<String> values = extractPartitionValues(spec, struct);
                out.put(name, new ConnectorPartitionItem.ListPartitionItem(
                        Collections.singletonList(values)));
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to plan iceberg files for " + database + "." + table, e);
        }
        return out;
    }

    @Override
    public ConnectorPartitionType getPartitionType(
            String database, String table, Optional<ConnectorMvccSnapshot> snapshot) {
        Table icebergTable = loadTable(database, table);
        if (icebergTable.spec().isUnpartitioned()) {
            return ConnectorPartitionType.UNPARTITIONED;
        }
        return ConnectorPartitionType.LIST;
    }

    @Override
    public Set<String> getPartitionColumnNames(
            String database, String table, Optional<ConnectorMvccSnapshot> snapshot) {
        Table icebergTable = loadTable(database, table);
        PartitionSpec spec = icebergTable.spec();
        if (spec.isUnpartitioned()) {
            return Collections.emptySet();
        }
        Schema schema = icebergTable.schema();
        Set<String> names = new LinkedHashSet<>(spec.fields().size());
        for (PartitionField pf : spec.fields()) {
            Types.NestedField src = schema.findField(pf.sourceId());
            String name = src != null ? src.name() : pf.name();
            names.add(name.toLowerCase());
        }
        return names;
    }

    @Override
    public List<ConnectorColumn> getPartitionColumns(
            String database, String table, Optional<ConnectorMvccSnapshot> snapshot) {
        Table icebergTable = loadTable(database, table);
        PartitionSpec spec = icebergTable.spec();
        if (spec.isUnpartitioned()) {
            return Collections.emptyList();
        }
        Schema schema = icebergTable.schema();
        List<ConnectorColumn> out = new ArrayList<>(spec.fields().size());
        for (PartitionField pf : spec.fields()) {
            Types.NestedField src = schema.findField(pf.sourceId());
            // Partition column type follows the transform's result type so the
            // engine sees the materialised column shape (e.g. month -> INT).
            Type resultType = pf.transform().getResultType(src.type());
            String name = src.name();
            out.add(new ConnectorColumn(
                    name,
                    IcebergTypeMapping.fromIcebergType(resultType, false, false),
                    "",
                    src.isOptional(),
                    null));
        }
        return out;
    }

    @Override
    public ConnectorMtmvSnapshot getPartitionSnapshot(
            String database, String table, String partitionName,
            MtmvRefreshHint hint, Optional<ConnectorMvccSnapshot> snapshot) {
        Objects.requireNonNull(partitionName, "partitionName");
        Table icebergTable = loadTable(database, table);
        long snapshotId = currentSnapshotId(icebergTable, snapshot);
        return new ConnectorMtmvSnapshot.SnapshotIdMtmvSnapshot(snapshotId);
    }

    @Override
    public ConnectorMtmvSnapshot getTableSnapshot(
            String database, String table,
            MtmvRefreshHint hint, Optional<ConnectorMvccSnapshot> snapshot) {
        Table icebergTable = loadTable(database, table);
        long snapshotId = currentSnapshotId(icebergTable, snapshot);
        return new ConnectorMtmvSnapshot.SnapshotIdMtmvSnapshot(snapshotId);
    }

    @Override
    public long getNewestUpdateVersionOrTime(String database, String table) {
        try {
            Table icebergTable = loadTable(database, table);
            Snapshot snap = icebergTable.currentSnapshot();
            return snap == null ? -1L : snap.snapshotId();
        } catch (RuntimeException e) {
            LOG.debug("getNewestUpdateVersionOrTime({}.{}) returning -1 due to error: {}",
                    database, table, e.getMessage());
            return -1L;
        }
    }

    @Override
    public boolean isPartitionColumnAllowNull(String database, String table) {
        // Iceberg partition specs admit nulls; null values are routed to a
        // dedicated bucket rather than being rejected.
        return true;
    }

    @Override
    public boolean isValidRelatedTable(String database, String table) {
        try {
            Table icebergTable = loadTable(database, table);
            Set<String> sourceColumns = new LinkedHashSet<>();
            for (PartitionSpec spec : icebergTable.specs().values()) {
                if (spec == null) {
                    return false;
                }
                List<PartitionField> fields = spec.fields();
                if (fields.isEmpty()) {
                    // Historical unpartitioned spec contributes no source column.
                    continue;
                }
                if (fields.size() != 1) {
                    return false;
                }
                PartitionField pf = fields.get(0);
                String transformName = pf.transform().toString();
                if (!ALLOWED_TRANSFORMS.contains(transformName)) {
                    return false;
                }
                String sourceName = icebergTable.schema().findColumnName(pf.sourceId());
                if (sourceName == null) {
                    return false;
                }
                sourceColumns.add(sourceName);
            }
            // If every historical spec was empty, treat as unpartitioned-valid only when the
            // current spec is also unpartitioned. Otherwise require a single shared source.
            if (sourceColumns.isEmpty()) {
                return icebergTable.spec().isUnpartitioned();
            }
            return sourceColumns.size() == 1;
        } catch (RuntimeException e) {
            LOG.debug("isValidRelatedTable({}.{}) returning false due to error: {}",
                    database, table, e.getMessage());
            return false;
        }
    }

    @Override
    public boolean needAutoRefresh(String database, String table) {
        return true;
    }

    // ---------------------------------------------------------------- helpers

    private Table loadTable(String database, String table) {
        Table loaded = tableLoader.apply(database, table);
        if (loaded == null) {
            throw new IllegalStateException(
                    "Iceberg table loader returned null for " + database + "." + table);
        }
        return loaded;
    }

    private TableScan newScan(Table table, Optional<ConnectorMvccSnapshot> snapshot) {
        Optional<Long> pinned = extractSnapshotId(snapshot);
        TableScan scan = table.newScan();
        if (pinned.isPresent()) {
            return scan.useSnapshot(pinned.get());
        }
        return scan;
    }

    private long currentSnapshotId(Table table, Optional<ConnectorMvccSnapshot> snapshot) {
        Optional<Long> pinned = extractSnapshotId(snapshot);
        if (pinned.isPresent()) {
            return pinned.get();
        }
        Snapshot snap = table.currentSnapshot();
        return snap == null ? -1L : snap.snapshotId();
    }

    private static Optional<Long> extractSnapshotId(Optional<ConnectorMvccSnapshot> snapshot) {
        if (snapshot.isEmpty()) {
            return Optional.empty();
        }
        Optional<ConnectorTableVersion> version = snapshot.get().asVersion();
        if (version.isEmpty()) {
            return Optional.empty();
        }
        ConnectorTableVersion v = version.get();
        if (v instanceof ConnectorTableVersion.BySnapshotId by) {
            return Optional.of(by.snapshotId());
        }
        return Optional.empty();
    }

    @SuppressWarnings("unchecked")
    private static List<String> extractPartitionValues(PartitionSpec spec, StructLike struct) {
        List<PartitionField> fields = spec.fields();
        Schema schema = spec.schema();
        List<String> values = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            PartitionField pf = fields.get(i);
            Types.NestedField src = schema.findField(pf.sourceId());
            Type resultType = pf.transform().getResultType(src.type());
            Class<?> javaClass = resultType.typeId().javaClass();
            Object raw = struct.get(i, javaClass);
            if (raw == null) {
                values.add("null");
                continue;
            }
            Transform<Object, Object> transform = (Transform<Object, Object>) pf.transform();
            values.add(transform.toHumanString(resultType, raw));
        }
        return values;
    }
}
