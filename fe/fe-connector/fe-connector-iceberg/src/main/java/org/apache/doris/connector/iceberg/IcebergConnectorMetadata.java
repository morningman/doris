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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.cache.MetaCacheHandle;
import org.apache.doris.connector.api.event.EventSourceOps;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.mtmv.MtmvOps;
import org.apache.doris.connector.api.systable.SysTableSpec;
import org.apache.doris.connector.api.systable.SystemTableOps;
import org.apache.doris.connector.api.timetravel.ConnectorRefSpec;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;
import org.apache.doris.connector.api.timetravel.RefKind;
import org.apache.doris.connector.api.timetravel.RefOps;
import org.apache.doris.connector.iceberg.api.IcebergBackend;
import org.apache.doris.connector.iceberg.api.IcebergBackendContext;
import org.apache.doris.connector.iceberg.cache.IcebergTableCacheKey;
import org.apache.doris.connector.iceberg.event.IcebergEventSourceOps;
import org.apache.doris.connector.iceberg.mtmv.IcebergMtmvOps;
import org.apache.doris.connector.iceberg.systable.IcebergSystemTableOps;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * {@link ConnectorMetadata} implementation for Iceberg catalogs.
 *
 * <p>Phase 1 provides read-only metadata operations:
 * <ul>
 *   <li>List databases (namespaces) and tables</li>
 *   <li>Get table schema from Iceberg's native Schema</li>
 *   <li>Partition spec info in table properties</li>
 * </ul>
 *
 * <p>Uses the Iceberg SDK Catalog API directly. All catalog backends (REST, HMS,
 * Glue, etc.) are transparent — the Iceberg Catalog interface abstracts them.</p>
 */
public class IcebergConnectorMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(IcebergConnectorMetadata.class);

    private final Catalog catalog;
    private final Map<String, String> properties;
    private final IcebergBackend backend;
    private final IcebergBackendContext backendContext;
    private final MetaCacheHandle<IcebergTableCacheKey, Table> tableHandle;
    private final String catalogName;
    private volatile SystemTableOps sysTableOps;
    private volatile EventSourceOps eventSourceOps;
    private volatile MtmvOps mtmvOps;

    public IcebergConnectorMetadata(Catalog catalog, Map<String, String> properties) {
        this(catalog, properties, null, null, null, "");
    }

    public IcebergConnectorMetadata(Catalog catalog,
                                    Map<String, String> properties,
                                    IcebergBackend backend,
                                    IcebergBackendContext backendContext,
                                    MetaCacheHandle<IcebergTableCacheKey, Table> tableHandle) {
        this(catalog, properties, backend, backendContext, tableHandle, "");
    }

    public IcebergConnectorMetadata(Catalog catalog,
                                    Map<String, String> properties,
                                    IcebergBackend backend,
                                    IcebergBackendContext backendContext,
                                    MetaCacheHandle<IcebergTableCacheKey, Table> tableHandle,
                                    String catalogName) {
        this.catalog = catalog;
        this.properties = properties;
        this.backend = backend;
        this.backendContext = backendContext;
        this.tableHandle = tableHandle;
        this.catalogName = catalogName == null ? "" : catalogName;
    }

    // ========== EventSourceOps (D7 / M2-03) ==========

    @Override
    public EventSourceOps getEventSourceOps() {
        EventSourceOps ops = eventSourceOps;
        if (ops == null) {
            synchronized (this) {
                ops = eventSourceOps;
                if (ops == null) {
                    if (catalog == null || catalogName.isEmpty()) {
                        ops = EventSourceOps.NONE;
                    } else {
                        ops = new IcebergEventSourceOps(catalog, catalogName);
                    }
                    eventSourceOps = ops;
                }
            }
        }
        return ops;
    }

    // ========== MtmvOps (D8 / M2-08) ==========

    @Override
    public Optional<MtmvOps> mtmvOps() {
        MtmvOps ops = mtmvOps;
        if (ops == null) {
            synchronized (this) {
                ops = mtmvOps;
                if (ops == null) {
                    ops = new IcebergMtmvOps(this::loadIcebergTable);
                    mtmvOps = ops;
                }
            }
        }
        return Optional.of(ops);
    }

    @Override
    public Optional<RefOps> refOps() {
        if (backend == null || backendContext == null) {
            return Optional.empty();
        }
        return Optional.of(new IcebergRefOps(backend, backendContext));
    }

    // ========== SystemTableOps (M1-13) ==========

    /**
     * Returns the seven Iceberg metadata-table specs published by the plugin
     * (snapshots / history / files / entries / manifests / refs / partitions).
     * Backed by an {@link IcebergSystemTableOps} that wires each spec's
     * {@code NativeSysTableScanFactory} to the {@code iceberg.table}
     * {@link MetaCacheHandle} so D3 invalidation propagates automatically.
     */
    @Override
    public List<SysTableSpec> listSysTables(String database, String table) {
        return systemTableOps().listSysTables(database, table);
    }

    @Override
    public Optional<SysTableSpec> getSysTable(String database, String table, String sysTableName) {
        return systemTableOps().getSysTable(database, table, sysTableName);
    }

    private SystemTableOps systemTableOps() {
        // Lazily construct to keep ctor cheap and to avoid allocating when sys
        // tables are never queried. The instance is stateless beyond the loader.
        SystemTableOps cached = sysTableOps;
        if (cached == null) {
            synchronized (this) {
                cached = sysTableOps;
                if (cached == null) {
                    cached = new IcebergSystemTableOps(this::loadIcebergTable);
                    sysTableOps = cached;
                }
            }
        }
        return cached;
    }

    private Table loadIcebergTable(String dbName, String tableName) {
        return tableHandle != null
                ? tableHandle.get(new IcebergTableCacheKey(dbName, tableName))
                : catalog.loadTable(TableIdentifier.of(dbName, tableName));
    }

    // ========== ConnectorSchemaOps ==========

    @Override
    public List<String> listDatabaseNames(ConnectorSession session) {
        if (!(catalog instanceof SupportsNamespaces)) {
            LOG.warn("Iceberg catalog does not support namespaces");
            return Collections.emptyList();
        }
        SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
        return nsCatalog.listNamespaces(Namespace.empty()).stream()
                .map(ns -> ns.level(ns.length() - 1))
                .collect(Collectors.toList());
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String dbName) {
        if (!(catalog instanceof SupportsNamespaces)) {
            return false;
        }
        return ((SupportsNamespaces) catalog).namespaceExists(Namespace.of(dbName));
    }

    // ========== ConnectorTableOps ==========

    @Override
    public List<String> listTableNames(ConnectorSession session, String dbName) {
        Namespace ns = Namespace.of(dbName);
        return catalog.listTables(ns).stream()
                .map(TableIdentifier::name)
                .collect(Collectors.toList());
    }

    @Override
    public Optional<ConnectorTableHandle> getTableHandle(
            ConnectorSession session, String dbName, String tableName) {
        TableIdentifier tableId = TableIdentifier.of(dbName, tableName);
        if (!catalog.tableExists(tableId)) {
            return Optional.empty();
        }
        Table table = loadIcebergTable(dbName, tableName);
        return Optional.of(buildTableHandle(dbName, tableName, table, null, null));
    }

    /**
     * Time-travel-aware {@code getTableHandle}: resolves the supplied
     * {@link ConnectorTableVersion} / {@link ConnectorRefSpec} to an
     * iceberg snapshot id and pins it on the returned handle. Resolution
     * mirrors legacy {@code IcebergUtils.getQuerySpecSnapshot}: ref + ref-kind
     * mismatches and unknown refs raise {@link DorisConnectorException};
     * {@link ConnectorTableVersion.ByOpaque} accepts the
     * {@code "iceberg:&lt;snapshotId&gt;:&lt;commitMs&gt;"} token shape produced by
     * {@link IcebergConnectorMvccSnapshot#toOpaqueToken()}.
     */
    @Override
    public Optional<ConnectorTableHandle> getTableHandle(
            ConnectorSession session, String dbName, String tableName,
            Optional<ConnectorTableVersion> version,
            Optional<ConnectorRefSpec> refSpec) {
        Objects.requireNonNull(version, "version");
        Objects.requireNonNull(refSpec, "refSpec");
        TableIdentifier tableId = TableIdentifier.of(dbName, tableName);
        if (!catalog.tableExists(tableId)) {
            return Optional.empty();
        }
        Table table = loadIcebergTable(dbName, tableName);
        Long snapshotId = null;
        String renderedRef = null;
        if (version.isPresent()) {
            ResolvedRef rr = resolveVersion(table, version.get());
            snapshotId = rr.snapshotId;
            renderedRef = rr.renderedRef;
        } else if (refSpec.isPresent()) {
            ResolvedRef rr = resolveRef(table, refSpec.get().name(), refSpec.get().kind());
            snapshotId = rr.snapshotId;
            renderedRef = rr.renderedRef;
        }
        return Optional.of(buildTableHandle(dbName, tableName, table, snapshotId, renderedRef));
    }

    private static final class ResolvedRef {
        final long snapshotId;
        final String renderedRef;

        ResolvedRef(long snapshotId, String renderedRef) {
            this.snapshotId = snapshotId;
            this.renderedRef = renderedRef;
        }
    }

    private ResolvedRef resolveVersion(Table table, ConnectorTableVersion version) {
        if (version instanceof ConnectorTableVersion.BySnapshotId) {
            long sid = ((ConnectorTableVersion.BySnapshotId) version).snapshotId();
            return new ResolvedRef(sid, null);
        }
        if (version instanceof ConnectorTableVersion.ByTimestamp) {
            long ts = ((ConnectorTableVersion.ByTimestamp) version).ts().toEpochMilli();
            long sid = SnapshotUtil.snapshotIdAsOfTime(table, ts);
            return new ResolvedRef(sid, null);
        }
        if (version instanceof ConnectorTableVersion.ByRef) {
            ConnectorTableVersion.ByRef br = (ConnectorTableVersion.ByRef) version;
            return resolveRef(table, br.name(), br.kind());
        }
        if (version instanceof ConnectorTableVersion.ByRefAtTimestamp) {
            ConnectorTableVersion.ByRefAtTimestamp brt =
                    (ConnectorTableVersion.ByRefAtTimestamp) version;
            ResolvedRef refResolved = resolveRef(table, brt.name(), brt.kind());
            long sid = walkAncestorsForTimestamp(
                    table, refResolved.snapshotId, brt.ts().toEpochMilli());
            return new ResolvedRef(sid, refResolved.renderedRef);
        }
        if (version instanceof ConnectorTableVersion.ByOpaque) {
            String token = ((ConnectorTableVersion.ByOpaque) version).token();
            return new ResolvedRef(parseOpaqueSnapshotId(token), null);
        }
        throw new DorisConnectorException(
                "Unsupported ConnectorTableVersion: " + version.getClass().getName());
    }

    private ResolvedRef resolveRef(Table table, String refName, RefKind kind) {
        SnapshotRef ref = table.refs().get(refName);
        if (ref == null) {
            throw new DorisConnectorException(
                    "Iceberg table " + table.name() + " has no ref named '" + refName + "'");
        }
        if (kind == RefKind.BRANCH && !ref.isBranch()) {
            throw new DorisConnectorException(
                    "Iceberg ref '" + refName + "' on " + table.name()
                            + " is not a branch");
        }
        if (kind == RefKind.TAG && !ref.isTag()) {
            throw new DorisConnectorException(
                    "Iceberg ref '" + refName + "' on " + table.name()
                            + " is not a tag");
        }
        String rendered = (kind == RefKind.TAG ? "tag:" : "branch:") + refName;
        return new ResolvedRef(ref.snapshotId(), rendered);
    }

    /**
     * Find the latest snapshot in the {@code refHead}'s ancestry whose
     * commit-time is &le; {@code timestampMs}. Mirrors the legacy
     * "walk ancestors and stop when committed-after-ts" pattern from
     * iceberg's {@code SnapshotUtil#snapshotIdAsOfTime}, but scoped to a
     * named ref's history rather than the table's main lineage.
     */
    private long walkAncestorsForTimestamp(Table table, long refHead, long timestampMs) {
        Long matched = null;
        for (Long ancestor : SnapshotUtil.ancestorIdsBetween(refHead, null, table::snapshot)) {
            Snapshot s = table.snapshot(ancestor);
            if (s == null) {
                continue;
            }
            if (s.timestampMillis() <= timestampMs) {
                matched = s.snapshotId();
                break;
            }
        }
        if (matched == null) {
            throw new DorisConnectorException(
                    "Iceberg ref " + refHead + " has no snapshot at or before timestamp "
                            + timestampMs + "ms");
        }
        return matched;
    }

    private static long parseOpaqueSnapshotId(String token) {
        if (!token.startsWith("iceberg:")) {
            throw new DorisConnectorException(
                    "Iceberg cannot decode opaque token (must start with 'iceberg:'): " + token);
        }
        String rest = token.substring("iceberg:".length());
        int colon = rest.indexOf(':');
        String snapshotIdStr = colon < 0 ? rest : rest.substring(0, colon);
        try {
            return Long.parseLong(snapshotIdStr);
        } catch (NumberFormatException e) {
            throw new DorisConnectorException(
                    "Iceberg opaque token has non-numeric snapshotId: " + token, e);
        }
    }

    /**
     * Build an {@link IcebergTableHandle} from a resolved iceberg {@link Table},
     * populating snapshot / schema / spec / format-version / metadata-location
     * fields per D11 §4. {@code pinnedSnapshotId} overrides the table's
     * current snapshot when a time-travel coordinate was resolved;
     * {@code renderedRef} is a diagnostic-only "branch:foo" / "tag:bar"
     * label. Engine-side scan resolution always reads {@code snapshotId}.
     */
    private IcebergTableHandle buildTableHandle(String dbName, String tableName, Table table,
                                                Long pinnedSnapshotId, String renderedRef) {
        Long snapshotId = pinnedSnapshotId;
        if (snapshotId == null) {
            Snapshot current = table.currentSnapshot();
            snapshotId = current != null ? current.snapshotId() : null;
        }

        Integer schemaId;
        if (pinnedSnapshotId != null) {
            // Use the schema bound to the pinned snapshot so projections resolve
            // against the historical layout, mirroring legacy
            // IcebergUtils.getQuerySpecSnapshot's schema resolution.
            Snapshot snap = table.snapshot(pinnedSnapshotId);
            schemaId = snap != null && snap.schemaId() != null
                    ? snap.schemaId()
                    : table.schema().schemaId();
        } else {
            schemaId = table.schema().schemaId();
        }

        Integer formatVersion = null;
        String metadataLocation = null;
        if (table instanceof BaseTable) {
            TableOperations ops = ((BaseTable) table).operations();
            TableMetadata meta = ops.current();
            if (meta != null) {
                formatVersion = meta.formatVersion();
                metadataLocation = meta.metadataFileLocation();
            }
        }

        return IcebergTableHandle.builder()
                .dbName(dbName)
                .tableName(tableName)
                .snapshotId(snapshotId)
                .refSpec(renderedRef)
                .formatVersion(formatVersion)
                .schemaId(schemaId)
                .partitionSpecId(table.spec().specId())
                .metadataLocation(metadataLocation)
                .build();
    }

    @Override
    public ConnectorTableSchema getTableSchema(
            ConnectorSession session, ConnectorTableHandle handle) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        String dbName = iceHandle.getDbName();
        String tableName = iceHandle.getTableName();

        Table table = tableHandle != null
                ? tableHandle.get(new IcebergTableCacheKey(dbName, tableName))
                : catalog.loadTable(TableIdentifier.of(dbName, tableName));
        Schema icebergSchema = table.schema();
        List<ConnectorColumn> columns = parseSchema(icebergSchema);

        Map<String, String> tableProps = new HashMap<>();
        tableProps.putAll(table.properties());
        tableProps.put("iceberg.format-version",
                String.valueOf(table.spec().specId() >= 0 ? 2 : 1));
        if (table.location() != null) {
            tableProps.put("location", table.location());
        }
        if (!table.spec().isUnpartitioned()) {
            tableProps.put("iceberg.partition-spec", table.spec().toString());
        }

        return new ConnectorTableSchema(tableName, columns, "ICEBERG", tableProps);
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle handle) {
        Objects.requireNonNull(handle, "handle");
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        Table table = loadIcebergTable(iceHandle.getDbName(), iceHandle.getTableName());

        Schema schema;
        Integer schemaId = iceHandle.getSchemaId();
        if (schemaId != null) {
            schema = table.schemas().get(schemaId);
            if (schema == null) {
                throw new IllegalStateException("Iceberg schema id " + schemaId
                        + " not found on table "
                        + iceHandle.getDbName() + "." + iceHandle.getTableName());
            }
        } else {
            schema = table.schema();
        }

        List<Types.NestedField> fields = schema.columns();
        if (fields.isEmpty()) {
            throw new IllegalStateException("Iceberg table "
                    + iceHandle.getDbName() + "." + iceHandle.getTableName()
                    + " has empty schema (schemaId=" + schema.schemaId() + ")");
        }

        Map<String, ConnectorColumnHandle> result = new LinkedHashMap<>(fields.size());
        int position = 0;
        for (Types.NestedField field : fields) {
            result.put(field.name(), new IcebergColumnHandle(
                    field.fieldId(),
                    field.name(),
                    field.type(),
                    field.isOptional(),
                    field.doc(),
                    position++));
        }
        return result;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    // ========== Internal helpers ==========

    /**
     * Convert an Iceberg Schema to a list of ConnectorColumn.
     */
    private List<ConnectorColumn> parseSchema(Schema schema) {
        List<Types.NestedField> fields = schema.columns();
        List<ConnectorColumn> columns = new ArrayList<>(fields.size());
        boolean enableVarbinary = Boolean.parseBoolean(
                properties.getOrDefault(
                        IcebergConnectorProperties.ENABLE_MAPPING_VARBINARY, "false"));
        boolean enableTimestampTz = Boolean.parseBoolean(
                properties.getOrDefault(
                        IcebergConnectorProperties.ENABLE_MAPPING_TIMESTAMP_TZ, "false"));

        for (Types.NestedField field : fields) {
            columns.add(new ConnectorColumn(
                    field.name(),
                    IcebergTypeMapping.fromIcebergType(
                            field.type(), enableVarbinary, enableTimestampTz),
                    field.doc() != null ? field.doc() : "",
                    field.isOptional(),
                    null));
        }
        return columns;
    }
}
