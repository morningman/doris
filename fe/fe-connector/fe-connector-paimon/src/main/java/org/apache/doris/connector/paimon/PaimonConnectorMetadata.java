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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableId;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.action.ConnectorActionOps;
import org.apache.doris.connector.api.cache.MetaCacheHandle;
import org.apache.doris.connector.api.event.EventSourceOps;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorInsertHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.mtmv.MtmvOps;
import org.apache.doris.connector.api.systable.SysTableSpec;
import org.apache.doris.connector.api.systable.SystemTableOps;
import org.apache.doris.connector.api.timetravel.RefOps;
import org.apache.doris.connector.api.write.ConnectorTransactionContext;
import org.apache.doris.connector.api.write.ConnectorTxnCapability;
import org.apache.doris.connector.api.write.ConnectorWriteConfig;
import org.apache.doris.connector.api.write.ConnectorWriteType;
import org.apache.doris.connector.api.write.WriteIntent;
import org.apache.doris.connector.paimon.api.PaimonBackend;
import org.apache.doris.connector.paimon.api.PaimonBackendContext;
import org.apache.doris.connector.paimon.cache.PaimonTableCacheKey;
import org.apache.doris.connector.paimon.event.PaimonEventSourceOps;
import org.apache.doris.connector.paimon.mtmv.PaimonMtmvOps;
import org.apache.doris.connector.paimon.systable.PaimonSystemTableOps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * {@link ConnectorMetadata} implementation for Paimon.
 *
 * <p>Phase 1 (metadata-only): supports listing databases and tables,
 * getting table handles, and reading table schema. Scan planning,
 * predicate pushdown, and DML operations remain in fe-core.
 *
 * <p>Per-table loads ({@link Catalog#getTable(Identifier)}) are routed
 * through the {@code paimon.table} cache binding when present, so repeated
 * schema reads on the same {@code (db, t)} pair hit the binding-level
 * cache instead of re-issuing a metadata RPC.
 */
public class PaimonConnectorMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(PaimonConnectorMetadata.class);

    private final Catalog catalog;
    private final PaimonTypeMapping.Options typeMappingOptions;
    private final PaimonBackend backend;
    private final PaimonBackendContext backendContext;
    private final MetaCacheHandle<PaimonTableCacheKey, Table> tableHandle;
    private final String catalogName;
    private volatile SystemTableOps sysTableOps;
    private volatile EventSourceOps eventSourceOps;
    private volatile MtmvOps mtmvOps;
    private volatile ConnectorActionOps actionOps;

    public PaimonConnectorMetadata(Catalog catalog, Map<String, String> properties) {
        this(catalog, properties, null, null, null, "");
    }

    public PaimonConnectorMetadata(Catalog catalog,
                                   Map<String, String> properties,
                                   PaimonBackend backend,
                                   PaimonBackendContext backendContext) {
        this(catalog, properties, backend, backendContext, null, "");
    }

    public PaimonConnectorMetadata(Catalog catalog,
                                   Map<String, String> properties,
                                   PaimonBackend backend,
                                   PaimonBackendContext backendContext,
                                   MetaCacheHandle<PaimonTableCacheKey, Table> tableHandle) {
        this(catalog, properties, backend, backendContext, tableHandle, "");
    }

    public PaimonConnectorMetadata(Catalog catalog,
                                   Map<String, String> properties,
                                   PaimonBackend backend,
                                   PaimonBackendContext backendContext,
                                   MetaCacheHandle<PaimonTableCacheKey, Table> tableHandle,
                                   String catalogName) {
        this.catalog = catalog;
        this.typeMappingOptions = buildTypeMappingOptions(properties);
        this.backend = backend;
        this.backendContext = backendContext;
        this.tableHandle = tableHandle;
        this.catalogName = catalogName == null ? "" : catalogName;
    }

    // ========== EventSourceOps (D7 / M2-04) ==========

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
                        ops = new PaimonEventSourceOps(catalog, catalogName);
                    }
                    eventSourceOps = ops;
                }
            }
        }
        return ops;
    }

    @Override
    public Optional<RefOps> refOps() {
        if (backend != null && backendContext != null) {
            return Optional.of(new PaimonRefOps(backend, backendContext));
        }
        return Optional.empty();
    }

    // ========== ConnectorActionOps (D1-write-path / M3-12) ==========

    /**
     * Lazily wires {@link PaimonActionOps} so the {@code CALL} dispatcher
     * (M3-10) can invoke paimon's native {@code create_tag},
     * {@code delete_tag}, {@code create_branch}, {@code delete_branch}, and
     * the placeholder {@code compact} procedure. Returns
     * {@link Optional#empty()} only when the catalog handle is missing
     * (test fixtures), in which case the engine reports the action as
     * unknown.
     */
    @Override
    public Optional<ConnectorActionOps> actionOps() {
        if (catalog == null) {
            return Optional.empty();
        }
        ConnectorActionOps ops = actionOps;
        if (ops == null) {
            synchronized (this) {
                ops = actionOps;
                if (ops == null) {
                    ops = new PaimonActionOps(this::loadPaimonTableForAction);
                    actionOps = ops;
                }
            }
        }
        return Optional.of(ops);
    }

    /**
     * Loader bridge for {@link PaimonActionOps} — unwraps the checked
     * {@link Catalog.TableNotExistException} into a runtime so it can flow
     * through the {@code BiFunction} surface, and routes through the
     * {@code paimon.table} cache binding when present.
     */
    private Table loadPaimonTableForAction(String dbName, String tableName) {
        try {
            return loadTable(dbName, tableName);
        } catch (Catalog.TableNotExistException e) {
            throw new DorisConnectorException(
                    "Paimon table not found for action: " + dbName + "." + tableName, e);
        }
    }

    // ========== MtmvOps (D8 / M2-09) ==========

    /**
     * Lazily wires {@link PaimonMtmvOps} to this metadata's catalog so paimon
     * tables can serve as MV base tables. Returns {@link Optional#empty()}
     * only when the catalog handle is missing (test fixtures), in which case
     * {@code PluginDrivenExternalTable} falls back to the legacy fe-core path.
     */
    @Override
    public Optional<MtmvOps> mtmvOps() {
        if (catalog == null) {
            return Optional.empty();
        }
        MtmvOps ops = mtmvOps;
        if (ops == null) {
            synchronized (this) {
                ops = mtmvOps;
                if (ops == null) {
                    ops = new PaimonMtmvOps(catalog, typeMappingOptions);
                    mtmvOps = ops;
                }
            }
        }
        return Optional.of(ops);
    }

    // ========== SystemTableOps (M1-14) ==========

    /**
     * Returns the paimon metadata-table specs published by the plugin
     * ({@code snapshots / schemas / options / tags / branches / consumers /
     * aggregation_fields / files / manifests / partitions / statistics /
     * buckets / table_indexes}). Backed by a {@link PaimonSystemTableOps}
     * that wires each spec's {@code NativeSysTableScanFactory} to the
     * {@code paimon.table} {@link MetaCacheHandle} so D3 invalidation
     * propagates automatically.
     */
    @Override
    public List<SysTableSpec> listSysTables(ConnectorTableId id) {
        return systemTableOps().listSysTables(id);
    }

    @Override
    public Optional<SysTableSpec> getSysTable(ConnectorTableId id, String sysTableName) {
        return systemTableOps().getSysTable(id, sysTableName);
    }

    private SystemTableOps systemTableOps() {
        // Lazily construct to keep ctor cheap and to avoid allocating when sys
        // tables are never queried. The instance is stateless beyond the loader.
        SystemTableOps cached = sysTableOps;
        if (cached == null) {
            synchronized (this) {
                cached = sysTableOps;
                if (cached == null) {
                    cached = new PaimonSystemTableOps(this::loadPaimonTableForSysTable);
                    sysTableOps = cached;
                }
            }
        }
        return cached;
    }

    /**
     * Loader bridge for {@link PaimonSystemTableOps} — unwraps the
     * checked {@link Catalog.TableNotExistException} into a runtime so it
     * can flow through the {@code BiFunction} surface, and routes through
     * the {@code paimon.table} cache binding when present.
     */
    private Table loadPaimonTableForSysTable(String dbName, String tableName) {
        try {
            return loadTable(dbName, tableName);
        } catch (Catalog.TableNotExistException e) {
            throw new RuntimeException(
                    "Paimon table not found for sys-table base: " + dbName + "." + tableName, e);
        }
    }

    @Override
    public List<String> listDatabaseNames(ConnectorSession session) {
        try {
            return catalog.listDatabases();
        } catch (Exception e) {
            LOG.warn("Failed to list Paimon databases", e);
            return Collections.emptyList();
        }
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String dbName) {
        try {
            catalog.getDatabase(dbName);
            return true;
        } catch (Catalog.DatabaseNotExistException e) {
            return false;
        }
    }

    @Override
    public List<String> listTableNames(ConnectorSession session, String dbName) {
        try {
            return catalog.listTables(dbName);
        } catch (Catalog.DatabaseNotExistException e) {
            LOG.warn("Database does not exist: {}", dbName);
            return Collections.emptyList();
        } catch (Exception e) {
            LOG.warn("Failed to list tables in database: {}", dbName, e);
            return Collections.emptyList();
        }
    }

    @Override
    public Optional<ConnectorTableHandle> getTableHandle(
            ConnectorSession session, String dbName, String tableName) {
        try {
            Table table = loadTable(dbName, tableName);
            List<String> partitionKeys = table.partitionKeys();
            List<String> primaryKeys = table.primaryKeys();
            PaimonTableHandle handle = new PaimonTableHandle(
                    dbName, tableName,
                    partitionKeys != null ? partitionKeys : Collections.emptyList(),
                    primaryKeys != null ? primaryKeys : Collections.emptyList());
            handle.setPaimonTable(table);
            return Optional.of(handle);
        } catch (Catalog.TableNotExistException e) {
            return Optional.empty();
        } catch (Exception e) {
            LOG.warn("Failed to get Paimon table handle: {}.{}", dbName, tableName, e);
            return Optional.empty();
        }
    }

    @Override
    public ConnectorTableSchema getTableSchema(
            ConnectorSession session, ConnectorTableHandle handle) {
        PaimonTableHandle paimonHandle = (PaimonTableHandle) handle;
        String dbName = paimonHandle.getDatabaseName();
        String tableName = paimonHandle.getTableName();
        Table table;
        try {
            table = loadTable(dbName, tableName);
        } catch (Catalog.TableNotExistException e) {
            throw new RuntimeException("Paimon table not found: " + dbName + "." + tableName, e);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to get Paimon table schema: " + dbName + "." + tableName, e);
        }
        RowType rowType = table.rowType();
        List<String> primaryKeys = table.primaryKeys();
        List<ConnectorColumn> columns = mapFields(rowType, primaryKeys);

        Map<String, String> schemaProps = new HashMap<>();
        if (paimonHandle.getPartitionKeys() != null
                && !paimonHandle.getPartitionKeys().isEmpty()) {
            schemaProps.put("partition_keys",
                    String.join(",", paimonHandle.getPartitionKeys()));
        }
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            schemaProps.put("primary_keys", String.join(",", primaryKeys));
        }

        return new ConnectorTableSchema(
                paimonHandle.getTableName(),
                columns,
                "PAIMON",
                schemaProps);
    }

    @Override
    public Map<String, String> getProperties() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle handle) {
        PaimonTableHandle paimonHandle = (PaimonTableHandle) handle;
        Table table = paimonHandle.getPaimonTable();
        if (table == null) {
            try {
                table = loadTable(paimonHandle.getDatabaseName(), paimonHandle.getTableName());
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to load Paimon table: "
                                + paimonHandle.getDatabaseName() + "."
                                + paimonHandle.getTableName(), e);
            }
        }
        RowType rowType = table.rowType();
        List<DataField> fields = rowType.getFields();
        Map<String, ConnectorColumnHandle> handles = new LinkedHashMap<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            String name = fields.get(i).name().toLowerCase();
            handles.put(name, new PaimonColumnHandle(name, i));
        }
        return handles;
    }

    // ========== ConnectorWriteOps (M3-06) ==========

    /**
     * Paimon writes are file-based. Append-only tables accept APPEND
     * (and overwrite shapes that paimon's {@link BatchWriteBuilder}
     * supports natively); primary-key tables additionally accept UPSERT
     * because paimon merges row-ops on commit using the configured
     * merge engine. UPSERT against an append-only table is rejected at
     * {@link #beginInsert} time. The branch-routed write path (M3-04
     * mirror) uses paimon's {@link FileStoreTable#switchToBranch} so
     * commits land on the branch's snapshot lineage and never touch
     * {@code main}.
     */
    @Override
    public boolean supportsInsert() {
        return true;
    }

    @Override
    public ConnectorWriteConfig getWriteConfig(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumn> columns) {
        Objects.requireNonNull(handle, "handle");
        PaimonTableHandle paimonHandle = (PaimonTableHandle) handle;
        Table table = loadTableForWrite(paimonHandle.getDatabaseName(), paimonHandle.getTableName());
        Map<String, String> tableOptions = table.options() != null
                ? table.options() : Collections.emptyMap();

        String fileFormat = tableOptions.getOrDefault(
                CoreOptions.FILE_FORMAT.key(),
                CoreOptions.FILE_FORMAT.defaultValue());
        String compression = tableOptions.getOrDefault(
                CoreOptions.FILE_COMPRESSION.key(),
                CoreOptions.FILE_COMPRESSION.defaultValue());

        List<String> partitionColumns = table.partitionKeys() != null
                ? new ArrayList<>(table.partitionKeys()) : Collections.emptyList();

        Map<String, String> writeProps = new HashMap<>();
        writeProps.put("file-format", fileFormat);
        if (table instanceof FileStoreTable) {
            String location = ((FileStoreTable) table).location() != null
                    ? ((FileStoreTable) table).location().toString() : null;
            if (location != null) {
                writeProps.put("location", location);
            }
        }
        List<String> primaryKeys = table.primaryKeys();
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            writeProps.put("primary-keys", String.join(",", primaryKeys));
            String mergeEngine = tableOptions.get(CoreOptions.MERGE_ENGINE.key());
            if (mergeEngine != null) {
                writeProps.put("merge-engine", mergeEngine);
            }
        }
        String bucket = tableOptions.get(CoreOptions.BUCKET.key());
        if (bucket != null) {
            writeProps.put("bucket", bucket);
        }
        String writeBufferSize = tableOptions.get(CoreOptions.WRITE_BUFFER_SIZE.key());
        if (writeBufferSize != null) {
            writeProps.put("write-buffer-size", writeBufferSize);
        }

        ConnectorWriteConfig.Builder builder = ConnectorWriteConfig.builder(ConnectorWriteType.FILE_WRITE)
                .fileFormat(fileFormat)
                .compression(compression)
                .partitionColumns(partitionColumns)
                .properties(writeProps);
        if (writeProps.containsKey("location")) {
            builder.writeLocation(writeProps.get("location"));
        }
        return builder.build();
    }

    @Override
    public EnumSet<ConnectorTxnCapability> txnCapabilities() {
        return EnumSet.copyOf(PaimonTransactionContext.CAPABILITIES);
    }

    @Override
    public ConnectorTransactionContext beginTransaction(
            ConnectorSession session,
            ConnectorTableHandle handle,
            WriteIntent intent) {
        Objects.requireNonNull(handle, "handle");
        Objects.requireNonNull(intent, "intent");
        PaimonTableHandle paimonHandle = (PaimonTableHandle) handle;
        Table baseTable = loadTableForWrite(paimonHandle.getDatabaseName(), paimonHandle.getTableName());
        validateIntentAgainstTable(intent, baseTable);
        Table targetTable = switchBranchIfRequested(baseTable, intent);
        BatchWriteBuilder writeBuilder = targetTable.newBatchWriteBuilder();
        applyOverwriteMode(writeBuilder, intent);
        return new PaimonTransactionContext(
                paimonHandle.getDatabaseName(), paimonHandle.getTableName(),
                targetTable, writeBuilder, intent);
    }

    @Override
    public ConnectorInsertHandle beginInsert(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumn> columns,
            WriteIntent intent) {
        Objects.requireNonNull(handle, "handle");
        Objects.requireNonNull(intent, "intent");
        PaimonTableHandle paimonHandle = (PaimonTableHandle) handle;
        Table baseTable = loadTableForWrite(paimonHandle.getDatabaseName(), paimonHandle.getTableName());
        validateIntentAgainstTable(intent, baseTable);
        Table targetTable = switchBranchIfRequested(baseTable, intent);
        BatchWriteBuilder writeBuilder = targetTable.newBatchWriteBuilder();
        applyOverwriteMode(writeBuilder, intent);
        return new PaimonInsertHandle(
                paimonHandle.getDatabaseName(), paimonHandle.getTableName(),
                targetTable, writeBuilder, intent);
    }

    private static void validateIntentAgainstTable(WriteIntent intent, Table table) {
        boolean isPkTable = table.primaryKeys() != null && !table.primaryKeys().isEmpty();
        if (intent.isUpsert() && !isPkTable) {
            throw new DorisConnectorException(
                    "Paimon UPSERT requires a primary-key table; "
                            + table.fullName() + " is append-only");
        }
        WriteIntent.OverwriteMode mode = intent.overwriteMode();
        boolean partitioned = table.partitionKeys() != null && !table.partitionKeys().isEmpty();
        if (mode == WriteIntent.OverwriteMode.STATIC_PARTITION && !partitioned) {
            throw new DorisConnectorException(
                    "Static-partition overwrite requested on unpartitioned paimon table "
                            + table.fullName());
        }
        if (mode == WriteIntent.OverwriteMode.STATIC_PARTITION) {
            List<String> partitionKeys = table.partitionKeys();
            for (String key : intent.staticPartitions().keySet()) {
                if (!partitionKeys.contains(key)) {
                    throw new DorisConnectorException(
                            "Unknown partition column '" + key + "' for paimon table "
                                    + table.fullName());
                }
            }
        }
        if (mode == WriteIntent.OverwriteMode.DYNAMIC_PARTITION) {
            // Paimon's BatchWriteBuilder.withOverwrite(map) only accepts a
            // *static* partition predicate (or an empty map = full-table
            // overwrite). There is no native equivalent of iceberg's
            // ReplacePartitions today; reject loudly so callers know to
            // either fall back to FULL_TABLE or list partitions explicitly.
            throw new DorisConnectorException(
                    "Paimon does not support DYNAMIC_PARTITION overwrite via BatchWriteBuilder; "
                            + "use STATIC_PARTITION with explicit keys or FULL_TABLE");
        }
    }

    private static Table switchBranchIfRequested(Table table, WriteIntent intent) {
        if (!intent.branch().isPresent()) {
            return table;
        }
        String branchName = intent.branch().get();
        if (!(table instanceof FileStoreTable)) {
            throw new DorisConnectorException(
                    "Paimon branch writes require a FileStoreTable, got "
                            + table.getClass().getName() + " for " + table.fullName());
        }
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        List<String> branches = fileStoreTable.branchManager().branches();
        if (branches == null || !branches.contains(branchName)) {
            throw new DorisConnectorException(
                    "Paimon branch '" + branchName + "' does not exist on table "
                            + table.fullName());
        }
        return fileStoreTable.switchToBranch(branchName);
    }

    private static void applyOverwriteMode(BatchWriteBuilder writeBuilder, WriteIntent intent) {
        switch (intent.overwriteMode()) {
            case NONE:
                return;
            case FULL_TABLE:
                writeBuilder.withOverwrite(Collections.emptyMap());
                return;
            case STATIC_PARTITION:
                writeBuilder.withOverwrite(new LinkedHashMap<>(intent.staticPartitions()));
                return;
            default:
                // DYNAMIC_PARTITION is rejected upstream in
                // validateIntentAgainstTable; reaching here is a bug.
                throw new DorisConnectorException(
                        "Unhandled paimon overwrite mode: " + intent.overwriteMode());
        }
    }

    @Override
    public void finishInsert(ConnectorSession session,
            ConnectorInsertHandle handle,
            Collection<byte[]> fragments) {
        Objects.requireNonNull(handle, "handle");
        PaimonInsertHandle insertHandle = (PaimonInsertHandle) handle;
        List<CommitMessage> messages = PaimonCommitDataConverter.decodeFragments(fragments);
        try (BatchTableCommit commit = insertHandle.getWriteBuilder().newCommit()) {
            commit.commit(messages);
        } catch (Exception e) {
            if (e instanceof DorisConnectorException) {
                throw (DorisConnectorException) e;
            }
            throw new DorisConnectorException(
                    "Failed to commit paimon write for "
                            + insertHandle.getDbName() + "." + insertHandle.getTableName(), e);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Paimon insert committed for {}.{} mode={} branch={} messages={}",
                    insertHandle.getDbName(), insertHandle.getTableName(),
                    insertHandle.getIntent().overwriteMode(),
                    insertHandle.getIntent().branch().orElse(null),
                    messages.size());
        }
    }

    @Override
    public void abortInsert(ConnectorSession session, ConnectorInsertHandle handle) {
        Objects.requireNonNull(handle, "handle");
        PaimonInsertHandle insertHandle = (PaimonInsertHandle) handle;
        // Paimon BatchWriteBuilder hold no external resources before
        // BE-side writers stage files; with no fragments to pass to
        // BatchTableCommit#abort the safe behaviour is to drop the
        // builder reference and let GC reclaim the in-memory state.
        // Mirrors the iceberg M3-03 abort path.
        if (LOG.isDebugEnabled()) {
            LOG.debug("Paimon insert aborted for {}.{} mode={} branch={}",
                    insertHandle.getDbName(), insertHandle.getTableName(),
                    insertHandle.getIntent().overwriteMode(),
                    insertHandle.getIntent().branch().orElse(null));
        }
    }

    private Table loadTableForWrite(String dbName, String tableName) {
        try {
            return loadTable(dbName, tableName);
        } catch (Catalog.TableNotExistException e) {
            throw new DorisConnectorException(
                    "Paimon table not found: " + dbName + "." + tableName, e);
        }
    }

    /** Load the paimon {@link Table}, routing through the table cache binding when present. */
    private Table loadTable(String dbName, String tableName) throws Catalog.TableNotExistException {
        if (tableHandle != null) {
            try {
                return tableHandle.get(new PaimonTableCacheKey(dbName, tableName));
            } catch (RuntimeException e) {
                Throwable cause = e.getCause();
                if (cause instanceof Catalog.TableNotExistException) {
                    throw (Catalog.TableNotExistException) cause;
                }
                throw e;
            }
        }
        return catalog.getTable(Identifier.create(dbName, tableName));
    }

    private List<ConnectorColumn> mapFields(RowType rowType, List<String> primaryKeys) {
        List<DataField> fields = rowType.getFields();
        List<ConnectorColumn> columns = new ArrayList<>(fields.size());
        for (DataField field : fields) {
            ConnectorType connectorType = PaimonTypeMapping.toConnectorType(
                    field.type(), typeMappingOptions);
            String comment = field.description();
            boolean nullable = field.type().isNullable();
            columns.add(new ConnectorColumn(
                    field.name().toLowerCase(),
                    connectorType,
                    comment,
                    nullable,
                    null));
        }
        return columns;
    }

    private static PaimonTypeMapping.Options buildTypeMappingOptions(Map<String, String> props) {
        boolean binaryAsVarbinary = Boolean.parseBoolean(
                props.getOrDefault(
                        PaimonConnectorProperties.ENABLE_MAPPING_BINARY_AS_VARBINARY,
                        "false"));
        boolean timestampTz = Boolean.parseBoolean(
                props.getOrDefault(
                        PaimonConnectorProperties.ENABLE_MAPPING_TIMESTAMP_TZ,
                        "false"));
        return new PaimonTypeMapping.Options(binaryAsVarbinary, timestampTz);
    }
}
