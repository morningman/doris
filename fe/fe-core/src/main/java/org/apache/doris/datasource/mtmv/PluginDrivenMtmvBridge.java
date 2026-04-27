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

package org.apache.doris.datasource.mtmv;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.mtmv.ConnectorMtmvSnapshot;
import org.apache.doris.connector.api.mtmv.ConnectorPartitionItem;
import org.apache.doris.connector.api.mtmv.ConnectorPartitionType;
import org.apache.doris.connector.api.mtmv.MtmvOps;
import org.apache.doris.connector.api.mtmv.MtmvRefreshHint;
import org.apache.doris.connector.api.timetravel.ConnectorMvccSnapshot;
import org.apache.doris.datasource.ConnectorColumnConverter;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.mtmv.MTMVMaxTimestampSnapshot;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVSnapshotIdSnapshot;
import org.apache.doris.mtmv.MTMVSnapshotIf;
import org.apache.doris.mtmv.MTMVTimestampSnapshot;
import org.apache.doris.mtmv.MTMVVersionSnapshot;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Adapts the plugin-side {@link MtmvOps} surface to the fe-core
 * {@code MTMVRelatedTableIf} contract used by the materialized-view planner
 * and refresh scheduler.
 *
 * <p>All methods are guarded by
 * {@link ConnectorCapability#SUPPORTS_MTMV} plus a non-empty
 * {@link ConnectorMetadata#mtmvOps()}. When either is missing, the active
 * methods raise {@link UnsupportedOperationException} and the
 * {@code isValidRelatedTable} / {@code needAutoRefresh} predicates return
 * {@code false} so the planner naturally skips MV rewrites instead of
 * crashing.</p>
 *
 * <p>Type conversions used here:</p>
 * <ul>
 *   <li>{@link ConnectorPartitionType} &rarr; {@link PartitionType} is a
 *       1:1 enum mapping.</li>
 *   <li>{@link ConnectorMtmvSnapshot} &rarr; {@link MTMVSnapshotIf} maps the
 *       four sealed subtypes to their fe-core counterparts.</li>
 *   <li>{@link MvccSnapshot} &harr; {@link ConnectorMvccSnapshot} is bridged
 *       through {@link PluginDrivenMvccSnapshot}; non-plugin {@code MvccSnapshot}
 *       inputs are rejected.</li>
 *   <li>{@link ConnectorPartitionItem} &rarr; {@link PartitionItem} is currently
 *       unsupported because building fe-core {@code RangePartitionItem} /
 *       {@code ListPartitionItem} requires {@code PartitionKey} construction
 *       wired against partition column types &mdash; deferred to a follow-up
 *       milestone. Bridge calls {@code ops.listPartitions} and converts each
 *       entry; non-empty {@code Range} / {@code List} entries trigger
 *       {@link UnsupportedOperationException}, while
 *       {@link ConnectorPartitionItem.UnpartitionedItem} entries are skipped
 *       (the planner uses {@link #getPartitionType} first to detect
 *       unpartitioned tables).</li>
 * </ul>
 */
public final class PluginDrivenMtmvBridge {

    private final PluginDrivenExternalCatalog catalog;
    private final String dbName;
    private final String tableName;

    /**
     * Creates a bridge for the given plugin-driven table. Resolves the
     * catalog, remote database name, and remote table name from the table
     * eagerly so the bridge can be reused without re-reading the table on
     * every call.
     */
    public PluginDrivenMtmvBridge(PluginDrivenExternalTable table) {
        Objects.requireNonNull(table, "table");
        if (!(table.getCatalog() instanceof PluginDrivenExternalCatalog)) {
            throw new IllegalArgumentException(
                    "PluginDrivenMtmvBridge requires a PluginDrivenExternalCatalog parent, got "
                            + table.getCatalog());
        }
        this.catalog = (PluginDrivenExternalCatalog) table.getCatalog();
        this.dbName = table.getDb() != null ? table.getDb().getRemoteName() : "";
        this.tableName = table.getRemoteName();
    }

    /** Constructor for unit tests; package-private. */
    PluginDrivenMtmvBridge(PluginDrivenExternalCatalog catalog, String dbName, String tableName) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.dbName = Objects.requireNonNull(dbName, "dbName");
        this.tableName = Objects.requireNonNull(tableName, "tableName");
    }

    // ---------------------------------------------------------------- API

    public Map<String, PartitionItem> getAndCopyPartitionItems(Optional<MvccSnapshot> snapshot) {
        MtmvOps ops = requireOps();
        Map<String, ConnectorPartitionItem> raw =
                ops.listPartitions(dbName, tableName, toConnectorSnapshot(snapshot));
        Map<String, PartitionItem> out = new LinkedHashMap<>();
        for (Map.Entry<String, ConnectorPartitionItem> e : raw.entrySet()) {
            PartitionItem converted = convertPartitionItem(e.getValue());
            if (converted != null) {
                out.put(e.getKey(), converted);
            }
        }
        return out;
    }

    public PartitionType getPartitionType(Optional<MvccSnapshot> snapshot) {
        ConnectorPartitionType t = requireOps().getPartitionType(
                dbName, tableName, toConnectorSnapshot(snapshot));
        return convertPartitionType(t);
    }

    public Set<String> getPartitionColumnNames(Optional<MvccSnapshot> snapshot) {
        return requireOps().getPartitionColumnNames(
                dbName, tableName, toConnectorSnapshot(snapshot));
    }

    public List<Column> getPartitionColumns(Optional<MvccSnapshot> snapshot) {
        return ConnectorColumnConverter.convertColumns(
                requireOps().getPartitionColumns(
                        dbName, tableName, toConnectorSnapshot(snapshot)));
    }

    public MTMVSnapshotIf getPartitionSnapshot(String partitionName, MTMVRefreshContext context,
            Optional<MvccSnapshot> snapshot) {
        Objects.requireNonNull(partitionName, "partitionName");
        ConnectorMtmvSnapshot raw = requireOps().getPartitionSnapshot(
                dbName, tableName, partitionName,
                hintFor(context, Optional.of(partitionName)),
                toConnectorSnapshot(snapshot));
        return convertSnapshot(raw, partitionName);
    }

    public MTMVSnapshotIf getTableSnapshot(MTMVRefreshContext context, Optional<MvccSnapshot> snapshot) {
        return getTableSnapshot(snapshot);
    }

    public MTMVSnapshotIf getTableSnapshot(Optional<MvccSnapshot> snapshot) {
        ConnectorMtmvSnapshot raw = requireOps().getTableSnapshot(
                dbName, tableName,
                hintFor(null, Optional.empty()),
                toConnectorSnapshot(snapshot));
        return convertSnapshot(raw, "");
    }

    public long getNewestUpdateVersionOrTime() {
        return requireOps().getNewestUpdateVersionOrTime(dbName, tableName);
    }

    public boolean isPartitionColumnAllowNull() {
        return requireOps().isPartitionColumnAllowNull(dbName, tableName);
    }

    public boolean isValidRelatedTable() {
        Optional<MtmvOps> ops = tryOps();
        return ops.isPresent() && ops.get().isValidRelatedTable(dbName, tableName);
    }

    public boolean needAutoRefresh() {
        Optional<MtmvOps> ops = tryOps();
        return ops.isPresent() && ops.get().needAutoRefresh(dbName, tableName);
    }

    // ------------------------------------------------------------- Internals

    private MtmvOps requireOps() {
        Optional<MtmvOps> opt = tryOps();
        if (opt.isEmpty()) {
            throw new UnsupportedOperationException(
                    "Connector " + catalog.getName() + " does not support MTMV "
                            + "(missing SUPPORTS_MTMV capability or empty MtmvOps)");
        }
        return opt.get();
    }

    private Optional<MtmvOps> tryOps() {
        Connector connector = catalog.getConnector();
        if (connector == null || !connector.getCapabilities().contains(ConnectorCapability.SUPPORTS_MTMV)) {
            return Optional.empty();
        }
        ConnectorSession session = catalog.buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);
        return metadata.mtmvOps();
    }

    static Optional<ConnectorMvccSnapshot> toConnectorSnapshot(Optional<MvccSnapshot> snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        if (snapshot.isEmpty()) {
            return Optional.empty();
        }
        MvccSnapshot inner = snapshot.get();
        if (inner instanceof PluginDrivenMvccSnapshot) {
            return Optional.of(((PluginDrivenMvccSnapshot) inner).delegate());
        }
        throw new UnsupportedOperationException(
                "Cannot bridge MvccSnapshot of type " + inner.getClass().getName()
                        + " to ConnectorMvccSnapshot; expected PluginDrivenMvccSnapshot");
    }

    static PartitionType convertPartitionType(ConnectorPartitionType t) {
        Objects.requireNonNull(t, "partitionType");
        switch (t) {
            case UNPARTITIONED:
                return PartitionType.UNPARTITIONED;
            case RANGE:
                return PartitionType.RANGE;
            case LIST:
                return PartitionType.LIST;
            default:
                throw new UnsupportedOperationException("Unknown ConnectorPartitionType: " + t);
        }
    }

    static MTMVSnapshotIf convertSnapshot(ConnectorMtmvSnapshot raw, String partitionName) {
        Objects.requireNonNull(raw, "snapshot");
        if (raw instanceof ConnectorMtmvSnapshot.VersionMtmvSnapshot) {
            return new MTMVVersionSnapshot(((ConnectorMtmvSnapshot.VersionMtmvSnapshot) raw).version(), 0L);
        }
        if (raw instanceof ConnectorMtmvSnapshot.TimestampMtmvSnapshot) {
            return new MTMVTimestampSnapshot(((ConnectorMtmvSnapshot.TimestampMtmvSnapshot) raw).epochMillis());
        }
        if (raw instanceof ConnectorMtmvSnapshot.MaxTimestampMtmvSnapshot) {
            return new MTMVMaxTimestampSnapshot(partitionName,
                    ((ConnectorMtmvSnapshot.MaxTimestampMtmvSnapshot) raw).epochMillis());
        }
        if (raw instanceof ConnectorMtmvSnapshot.SnapshotIdMtmvSnapshot) {
            return new MTMVSnapshotIdSnapshot(((ConnectorMtmvSnapshot.SnapshotIdMtmvSnapshot) raw).snapshotId());
        }
        throw new UnsupportedOperationException(
                "Unknown ConnectorMtmvSnapshot subtype: " + raw.getClass().getName());
    }

    /**
     * Converts a {@link ConnectorPartitionItem} to a fe-core {@link PartitionItem}.
     *
     * <p>Returns {@code null} for {@link ConnectorPartitionItem.UnpartitionedItem}
     * (callers filter these out). Throws {@link UnsupportedOperationException}
     * for {@link ConnectorPartitionItem.RangePartitionItem} and
     * {@link ConnectorPartitionItem.ListPartitionItem} until partition-key
     * construction is wired through the SPI &mdash; tracked as a known gap.</p>
     */
    static PartitionItem convertPartitionItem(ConnectorPartitionItem item) {
        Objects.requireNonNull(item, "item");
        if (item instanceof ConnectorPartitionItem.UnpartitionedItem) {
            return null;
        }
        throw new UnsupportedOperationException(
                "Plugin-driven MTMV partition-item conversion not yet wired for "
                        + item.getClass().getSimpleName()
                        + "; needs PartitionKey construction against partition column types "
                        + "(deferred to a future milestone)");
    }

    private static MtmvRefreshHint hintFor(MTMVRefreshContext context, Optional<String> partitionScope) {
        // The fe-core MTMVRefreshContext does not currently carry a refresh-mode hint
        // distinguishing FORCE_FULL / INCREMENTAL_AUTO / ON_DEMAND; default to
        // INCREMENTAL_AUTO so connectors may return cached snapshots when valid.
        return new MtmvRefreshHint(MtmvRefreshHint.RefreshMode.INCREMENTAL_AUTO, partitionScope);
    }
}
