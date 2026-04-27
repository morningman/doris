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
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.cache.MetaCacheHandle;
import org.apache.doris.connector.api.event.EventSourceOps;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.systable.SysTableSpec;
import org.apache.doris.connector.api.systable.SystemTableOps;
import org.apache.doris.connector.api.timetravel.RefOps;
import org.apache.doris.connector.paimon.api.PaimonBackend;
import org.apache.doris.connector.paimon.api.PaimonBackendContext;
import org.apache.doris.connector.paimon.cache.PaimonTableCacheKey;
import org.apache.doris.connector.paimon.event.PaimonEventSourceOps;
import org.apache.doris.connector.paimon.systable.PaimonSystemTableOps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
