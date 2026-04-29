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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableId;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.audit.ConnectorAuditOps;
import org.apache.doris.connector.api.event.EventSourceOps;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorDeleteHandle;
import org.apache.doris.connector.api.handle.ConnectorInsertHandle;
import org.apache.doris.connector.api.handle.ConnectorMergeHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.mtmv.MtmvOps;
import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorFilterConstraint;
import org.apache.doris.connector.api.pushdown.ConnectorIn;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.pushdown.FilterApplicationResult;
import org.apache.doris.connector.api.systable.SysTableSpec;
import org.apache.doris.connector.api.systable.SystemTableOps;
import org.apache.doris.connector.api.write.ConnectorWriteConfig;
import org.apache.doris.connector.api.write.ConnectorWriteType;
import org.apache.doris.connector.api.write.WriteIntent;
import org.apache.doris.connector.hive.audit.HiveAuditOps;
import org.apache.doris.connector.hive.event.HiveEventSourceOps;
import org.apache.doris.connector.hive.mtmv.HiveMtmvOps;
import org.apache.doris.connector.hive.systable.HiveSystemTableOps;
import org.apache.doris.connector.hms.HmsAcidOperation;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsClientException;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;
import org.apache.doris.connector.hms.HmsTypeMapping;
import org.apache.doris.connector.hms.HmsWriteOps;
import org.apache.doris.thrift.THivePartitionUpdate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * {@link ConnectorMetadata} implementation for Hive (HMS-based) catalogs.
 *
 * <p>Provides read-only metadata operations:
 * <ul>
 *   <li>List databases and tables</li>
 *   <li>Get table schema (columns + partition keys)</li>
 *   <li>Table format detection (HIVE/HUDI/ICEBERG)</li>
 *   <li>Partition name listing</li>
 *   <li>Column handle resolution for scan planning</li>
 *   <li>Partition pruning via {@code applyFilter}</li>
 * </ul>
 */
public class HiveConnectorMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(HiveConnectorMetadata.class);

    /** Default heartbeat period for in-flight ACID transactions. */
    static final long DEFAULT_HEARTBEAT_PERIOD_MS = 30_000L;

    private final HmsClient hmsClient;
    private final HmsWriteOps writeOps;
    private final ScheduledExecutorService heartbeatExecutor;
    private final long heartbeatPeriodMs;
    private final Map<String, String> properties;
    private final HmsTypeMapping.Options typeMappingOptions;
    private final String catalogName;
    private final AtomicLong stmtIdSequence = new AtomicLong(0);
    private volatile SystemTableOps sysTableOps;
    private volatile EventSourceOps eventSourceOps;
    private volatile MtmvOps mtmvOps;

    public HiveConnectorMetadata(HmsClient hmsClient, Map<String, String> properties) {
        this(hmsClient, properties, "");
    }

    public HiveConnectorMetadata(HmsClient hmsClient, Map<String, String> properties, String catalogName) {
        this(hmsClient, hmsClient instanceof HmsWriteOps ? (HmsWriteOps) hmsClient : null,
                null, DEFAULT_HEARTBEAT_PERIOD_MS, properties, catalogName);
    }

    /** Test / advanced constructor: inject {@link HmsWriteOps} and heartbeat executor. */
    public HiveConnectorMetadata(HmsClient hmsClient, HmsWriteOps writeOps,
            ScheduledExecutorService heartbeatExecutor, long heartbeatPeriodMs,
            Map<String, String> properties, String catalogName) {
        this.hmsClient = hmsClient;
        this.writeOps = writeOps;
        this.heartbeatExecutor = heartbeatExecutor;
        this.heartbeatPeriodMs = heartbeatPeriodMs;
        this.properties = properties;
        this.typeMappingOptions = buildTypeMappingOptions(properties);
        this.catalogName = catalogName == null ? "" : catalogName;
    }

    // ========== EventSourceOps (D7 / M2-02) ==========

    @Override
    public EventSourceOps getEventSourceOps() {
        EventSourceOps ops = eventSourceOps;
        if (ops == null) {
            synchronized (this) {
                ops = eventSourceOps;
                if (ops == null) {
                    if (catalogName.isEmpty()) {
                        ops = EventSourceOps.NONE;
                    } else {
                        ops = new HiveEventSourceOps(hmsClient, catalogName);
                    }
                    eventSourceOps = ops;
                }
            }
        }
        return ops;
    }

    // ========== MtmvOps (D8 / M2-07) ==========

    @Override
    public Optional<MtmvOps> mtmvOps() {
        MtmvOps ops = mtmvOps;
        if (ops == null) {
            synchronized (this) {
                ops = mtmvOps;
                if (ops == null) {
                    ops = new HiveMtmvOps(hmsClient);
                    mtmvOps = ops;
                }
            }
        }
        return Optional.of(ops);
    }

    // ========== ConnectorAuditOps (D8 / M2-13a) ==========

    @Override
    public Optional<ConnectorAuditOps> auditOps() {
        return Optional.of(HiveAuditOps.INSTANCE);
    }

    // ========== SystemTableOps (D6 / M1-15) ==========

    @Override
    public List<SysTableSpec> listSysTables(ConnectorTableId id) {
        return getOrInitSysTableOps().listSysTables(id);
    }

    @Override
    public Optional<SysTableSpec> getSysTable(ConnectorTableId id, String sysTableName) {
        return getOrInitSysTableOps().getSysTable(id, sysTableName);
    }

    private SystemTableOps getOrInitSysTableOps() {
        SystemTableOps ops = sysTableOps;
        if (ops == null) {
            synchronized (this) {
                ops = sysTableOps;
                if (ops == null) {
                    ops = new HiveSystemTableOps();
                    sysTableOps = ops;
                }
            }
        }
        return ops;
    }

    // ========== ConnectorSchemaOps ==========

    @Override
    public List<String> listDatabaseNames(ConnectorSession session) {
        return hmsClient.listDatabases();
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String dbName) {
        try {
            hmsClient.getDatabase(dbName);
            return true;
        } catch (HmsClientException e) {
            LOG.debug("Database '{}' not found: {}", dbName, e.getMessage());
            return false;
        }
    }

    // ========== ConnectorTableOps ==========

    @Override
    public List<String> listTableNames(ConnectorSession session, String dbName) {
        return hmsClient.listTables(dbName);
    }

    @Override
    public Optional<ConnectorTableHandle> getTableHandle(
            ConnectorSession session, String dbName, String tableName) {
        if (!hmsClient.tableExists(dbName, tableName)) {
            return Optional.empty();
        }
        HmsTableInfo tableInfo = hmsClient.getTable(dbName, tableName);
        HiveTableType tableType = HiveTableFormatDetector.detect(tableInfo);

        // Build partition key column names
        List<String> partKeyNames = Collections.emptyList();
        List<ConnectorColumn> partKeys = tableInfo.getPartitionKeys();
        if (partKeys != null && !partKeys.isEmpty()) {
            partKeyNames = partKeys.stream()
                    .map(ConnectorColumn::getName)
                    .collect(Collectors.toList());
        }

        HiveTableHandle handle = new HiveTableHandle.Builder(dbName, tableName, tableType)
                .inputFormat(tableInfo.getInputFormat())
                .serializationLib(tableInfo.getSerializationLib())
                .location(tableInfo.getLocation())
                .partitionKeyNames(partKeyNames)
                .sdParameters(tableInfo.getSdParameters())
                .tableParameters(tableInfo.getParameters())
                .build();
        return Optional.of(handle);
    }

    @Override
    public ConnectorTableSchema getTableSchema(
            ConnectorSession session, ConnectorTableHandle handle) {
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        String dbName = hiveHandle.getDbName();
        String tableName = hiveHandle.getTableName();

        HmsTableInfo tableInfo = hmsClient.getTable(dbName, tableName);
        List<ConnectorColumn> columns = buildColumns(tableInfo);
        List<ConnectorColumn> partitionKeys = buildPartitionKeys(tableInfo);

        // Merge: regular columns + partition columns
        List<ConnectorColumn> allColumns = new ArrayList<>(columns.size() + partitionKeys.size());
        allColumns.addAll(columns);
        allColumns.addAll(partitionKeys);

        String formatType = detectFormatType(tableInfo);
        Map<String, String> tableProperties = tableInfo.getParameters() != null
                ? tableInfo.getParameters() : Collections.emptyMap();

        return new ConnectorTableSchema(tableName, allColumns, formatType, tableProperties);
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    // ========== ConnectorTableOps: Column Handles ==========

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle handle) {
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        HmsTableInfo tableInfo = hmsClient.getTable(
                hiveHandle.getDbName(), hiveHandle.getTableName());

        Set<String> partKeyNames = hiveHandle.getPartitionKeyNames() != null
                ? hiveHandle.getPartitionKeyNames().stream().collect(Collectors.toSet())
                : Collections.emptySet();

        Map<String, ConnectorColumnHandle> result = new LinkedHashMap<>();
        List<ConnectorColumn> allCols = new ArrayList<>();
        if (tableInfo.getColumns() != null) {
            allCols.addAll(tableInfo.getColumns());
        }
        if (tableInfo.getPartitionKeys() != null) {
            allCols.addAll(tableInfo.getPartitionKeys());
        }
        for (ConnectorColumn col : allCols) {
            boolean isPartKey = partKeyNames.contains(col.getName());
            result.put(col.getName(), new HiveColumnHandle(
                    col.getName(), col.getType().getTypeName(), isPartKey));
        }
        return result;
    }

    // ========== ConnectorPushdownOps: Filter Pushdown ==========

    @Override
    public Optional<FilterApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session, ConnectorTableHandle handle,
            ConnectorFilterConstraint constraint) {
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        List<String> partKeyNames = hiveHandle.getPartitionKeyNames();
        if (partKeyNames == null || partKeyNames.isEmpty()) {
            return Optional.empty();
        }

        // Extract equality predicates on partition columns from the expression
        Map<String, List<String>> partitionPredicates = extractPartitionPredicates(
                constraint.getExpression(), partKeyNames);
        if (partitionPredicates.isEmpty()) {
            return Optional.empty();
        }

        // Build partition name filter patterns for HMS
        List<String> allPartNames = hmsClient.listPartitionNames(
                hiveHandle.getDbName(), hiveHandle.getTableName(), 100000);
        List<String> matchedPartNames = prunePartitionNames(
                allPartNames, partKeyNames, partitionPredicates);

        if (matchedPartNames.size() == allPartNames.size()) {
            // No pruning effect
            return Optional.empty();
        }

        List<HmsPartitionInfo> prunedPartitions = matchedPartNames.isEmpty()
                ? Collections.emptyList()
                : hmsClient.getPartitions(hiveHandle.getDbName(),
                        hiveHandle.getTableName(), matchedPartNames);

        LOG.info("Partition pruning: {}.{} all={} pruned={}",
                hiveHandle.getDbName(), hiveHandle.getTableName(),
                allPartNames.size(), prunedPartitions.size());

        HiveTableHandle newHandle = hiveHandle.toBuilder()
                .prunedPartitions(prunedPartitions)
                .build();
        return Optional.of(new FilterApplicationResult<>(
                newHandle, constraint.getExpression(), false));
    }

    // ========== Internal helpers ==========

    private List<ConnectorColumn> buildColumns(HmsTableInfo tableInfo) {
        List<ConnectorColumn> spiColumns = tableInfo.getColumns();
        if (spiColumns == null || spiColumns.isEmpty()) {
            return Collections.emptyList();
        }
        // HmsTableInfo already returns ConnectorColumn with types mapped by HmsTypeMapping
        // during ThriftHmsClient.getTable(). Enrich with default values if available.
        Map<String, String> defaults = getDefaultValues(tableInfo);
        if (defaults.isEmpty()) {
            return spiColumns;
        }
        List<ConnectorColumn> enriched = new ArrayList<>(spiColumns.size());
        for (ConnectorColumn col : spiColumns) {
            String defaultVal = defaults.get(col.getName());
            if (defaultVal != null && col.getDefaultValue() == null) {
                enriched.add(new ConnectorColumn(
                        col.getName(), col.getType(), col.getComment(),
                        col.isNullable(), defaultVal));
            } else {
                enriched.add(col);
            }
        }
        return enriched;
    }

    private List<ConnectorColumn> buildPartitionKeys(HmsTableInfo tableInfo) {
        List<ConnectorColumn> partKeys = tableInfo.getPartitionKeys();
        if (partKeys == null) {
            return Collections.emptyList();
        }
        return partKeys;
    }

    private Map<String, String> getDefaultValues(HmsTableInfo tableInfo) {
        try {
            return hmsClient.getDefaultColumnValues(
                    tableInfo.getDbName(), tableInfo.getTableName());
        } catch (HmsClientException e) {
            LOG.debug("Could not get default column values for {}.{}: {}",
                    tableInfo.getDbName(), tableInfo.getTableName(), e.getMessage());
            return Collections.emptyMap();
        }
    }

    private String detectFormatType(HmsTableInfo tableInfo) {
        HiveTableType type = HiveTableFormatDetector.detect(tableInfo);
        switch (type) {
            case HIVE:
                return resolveHiveFileFormat(tableInfo.getInputFormat());
            case HUDI:
                return "HUDI";
            case ICEBERG:
                return "ICEBERG";
            default:
                return "UNKNOWN";
        }
    }

    /**
     * Resolve the Hive file format name from the input format class.
     */
    private static String resolveHiveFileFormat(String inputFormat) {
        if (inputFormat == null) {
            return "HIVE";
        }
        if (inputFormat.contains("Parquet") || inputFormat.contains("parquet")) {
            return "HIVE_PARQUET";
        }
        if (inputFormat.contains("Orc") || inputFormat.contains("orc")) {
            return "HIVE_ORC";
        }
        if (inputFormat.contains("Text") || inputFormat.contains("text")) {
            return "HIVE_TEXT";
        }
        return "HIVE";
    }

    private static HmsTypeMapping.Options buildTypeMappingOptions(Map<String, String> props) {
        boolean binaryAsString = Boolean.parseBoolean(
                props.getOrDefault(HiveConnectorProperties.ENABLE_MAPPING_BINARY_AS_STRING, "false"));
        boolean timestampTz = Boolean.parseBoolean(
                props.getOrDefault(HiveConnectorProperties.ENABLE_MAPPING_TIMESTAMP_TZ, "false"));
        return new HmsTypeMapping.Options(
                HmsTypeMapping.DEFAULT_TIME_SCALE, binaryAsString, timestampTz);
    }

    /**
     * Extracts equality predicates on partition columns from the expression tree.
     * Supports: col = 'value', col IN ('v1', 'v2', ...), AND combinations.
     */
    private Map<String, List<String>> extractPartitionPredicates(
            ConnectorExpression expr, List<String> partKeyNames) {
        Set<String> partKeySet = partKeyNames.stream().collect(Collectors.toSet());
        Map<String, List<String>> result = new HashMap<>();
        extractPredicatesRecursive(expr, partKeySet, result);
        return result;
    }

    private void extractPredicatesRecursive(ConnectorExpression expr,
            Set<String> partKeySet, Map<String, List<String>> result) {
        if (expr instanceof ConnectorAnd) {
            for (ConnectorExpression child : ((ConnectorAnd) expr).getConjuncts()) {
                extractPredicatesRecursive(child, partKeySet, result);
            }
        } else if (expr instanceof ConnectorComparison) {
            ConnectorComparison cmp = (ConnectorComparison) expr;
            if (cmp.getOperator() == ConnectorComparison.Operator.EQ) {
                String colName = extractColumnName(cmp.getLeft());
                String value = extractLiteralValue(cmp.getRight());
                if (colName != null && value != null && partKeySet.contains(colName)) {
                    result.computeIfAbsent(colName, k -> new ArrayList<>()).add(value);
                }
            }
        } else if (expr instanceof ConnectorIn) {
            ConnectorIn inExpr = (ConnectorIn) expr;
            if (!inExpr.isNegated()) {
                String colName = extractColumnName(inExpr.getValue());
                if (colName != null && partKeySet.contains(colName)) {
                    List<String> values = new ArrayList<>();
                    for (ConnectorExpression item : inExpr.getInList()) {
                        String val = extractLiteralValue(item);
                        if (val != null) {
                            values.add(val);
                        }
                    }
                    if (!values.isEmpty()) {
                        result.computeIfAbsent(colName, k -> new ArrayList<>()).addAll(values);
                    }
                }
            }
        }
    }

    private String extractColumnName(ConnectorExpression expr) {
        if (expr instanceof org.apache.doris.connector.api.pushdown.ConnectorColumnRef) {
            return ((org.apache.doris.connector.api.pushdown.ConnectorColumnRef) expr).getColumnName();
        }
        return null;
    }

    private String extractLiteralValue(ConnectorExpression expr) {
        if (expr instanceof ConnectorLiteral) {
            Object val = ((ConnectorLiteral) expr).getValue();
            return val != null ? String.valueOf(val) : null;
        }
        return null;
    }

    /**
     * Prunes partition names based on extracted equality predicates.
     * Partition names follow the Hive convention: key1=val1/key2=val2
     */
    private List<String> prunePartitionNames(List<String> allPartNames,
            List<String> partKeyNames, Map<String, List<String>> predicates) {
        List<String> matched = new ArrayList<>();
        for (String partName : allPartNames) {
            Map<String, String> partValues = parsePartitionName(partName, partKeyNames);
            if (matchesPredicates(partValues, predicates)) {
                matched.add(partName);
            }
        }
        return matched;
    }

    private Map<String, String> parsePartitionName(String partName,
            List<String> partKeyNames) {
        Map<String, String> values = new HashMap<>();
        String[] parts = partName.split("/");
        for (String part : parts) {
            int eq = part.indexOf('=');
            if (eq > 0) {
                values.put(part.substring(0, eq), part.substring(eq + 1));
            }
        }
        return values;
    }

    private boolean matchesPredicates(Map<String, String> partValues,
            Map<String, List<String>> predicates) {
        for (Map.Entry<String, List<String>> entry : predicates.entrySet()) {
            String colName = entry.getKey();
            List<String> allowedValues = entry.getValue();
            String actualValue = partValues.get(colName);
            if (actualValue == null || !allowedValues.contains(actualValue)) {
                return false;
            }
        }
        return true;
    }

    // ========== ConnectorWriteOps (D1 / M3-08) ==========

    @Override
    public boolean supportsInsert() {
        return true;
    }

    @Override
    public boolean supportsDelete() {
        // Row-level DELETE is gated per-table by beginDelete (ACID only);
        // this flag is the connector-wide capability — declaring true lets
        // the planner attempt to dispatch row-level deletes, and the
        // per-table check below short-circuits non-ACID tables.
        return writeOps != null;
    }

    @Override
    public boolean supportsMerge() {
        return writeOps != null;
    }

    @Override
    public ConnectorWriteConfig getWriteConfig(ConnectorSession session, ConnectorTableHandle handle,
            List<ConnectorColumn> columns) {
        Objects.requireNonNull(handle, "handle");
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        HmsTableInfo tableInfo = hmsClient.getTable(hiveHandle.getDbName(), hiveHandle.getTableName());

        Map<String, String> writeProps = new HashMap<>();
        writeProps.put("input-format", String.valueOf(tableInfo.getInputFormat()));
        writeProps.put("serialization-lib", String.valueOf(tableInfo.getSerializationLib()));
        if (HiveAcidUtil.isFullAcidTable(tableInfo)) {
            writeProps.put("acid", "true");
        }

        return ConnectorWriteConfig.builder(ConnectorWriteType.FILE_WRITE)
                .fileFormat(detectFileFormat(tableInfo.getInputFormat()))
                .writeLocation(tableInfo.getLocation())
                .partitionColumns(new ArrayList<>(hiveHandle.getPartitionKeyNames()))
                .properties(writeProps)
                .build();
    }

    private static String detectFileFormat(String inputFormat) {
        if (inputFormat == null) {
            return "parquet";
        }
        String lower = inputFormat.toLowerCase(java.util.Locale.ROOT);
        if (lower.contains("orc")) {
            return "orc";
        }
        if (lower.contains("parquet")) {
            return "parquet";
        }
        if (lower.contains("text")) {
            return "text";
        }
        return "parquet";
    }

    @Override
    public ConnectorInsertHandle beginInsert(ConnectorSession session,
            ConnectorTableHandle handle, List<ConnectorColumn> columns, WriteIntent intent) {
        Objects.requireNonNull(handle, "handle");
        Objects.requireNonNull(intent, "intent");
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        HmsTableInfo tableInfo = hmsClient.getTable(hiveHandle.getDbName(), hiveHandle.getTableName());
        boolean isAcid = HiveAcidUtil.isFullAcidTable(tableInfo);
        validateOverwriteAgainstTable(intent, hiveHandle, tableInfo, isAcid);

        HiveAcidContext.Builder ctx = HiveAcidContext.builder(
                        hiveHandle.getDbName(), hiveHandle.getTableName(), intent)
                .stagingLocation(tableInfo.getLocation())
                .acid(isAcid);
        if (isAcid) {
            startAcidTransaction(ctx, hiveHandle, HmsAcidOperation.INSERT, intent);
        }
        return new HiveInsertHandle(ctx.build());
    }

    private void validateOverwriteAgainstTable(WriteIntent intent, HiveTableHandle hiveHandle,
            HmsTableInfo tableInfo, boolean isAcid) {
        WriteIntent.OverwriteMode mode = intent.overwriteMode();
        boolean partitioned = !hiveHandle.getPartitionKeyNames().isEmpty();
        if (mode == WriteIntent.OverwriteMode.STATIC_PARTITION && !partitioned) {
            throw new DorisConnectorException(
                    "Static-partition overwrite requested on unpartitioned hive table "
                            + hiveHandle.getDbName() + "." + hiveHandle.getTableName());
        }
        if (mode == WriteIntent.OverwriteMode.STATIC_PARTITION) {
            for (String key : intent.staticPartitions().keySet()) {
                if (!hiveHandle.getPartitionKeyNames().contains(key)) {
                    throw new DorisConnectorException(
                            "Unknown partition column '" + key + "' for hive table "
                                    + hiveHandle.getDbName() + "." + hiveHandle.getTableName());
                }
            }
        }
        if (isAcid && mode != WriteIntent.OverwriteMode.NONE) {
            // Legacy fe-core HMSTransaction never wired INSERT OVERWRITE on
            // ACID tables — Hive itself maps it to a compactor major /
            // truncate-then-insert sequence which the plugin write path
            // does not yet implement. Fail fast with the explicit reason
            // (handoff: M3-hive-acid-driver follow-up).
            throw new DorisConnectorException(
                    "INSERT OVERWRITE is not supported on hive ACID table "
                            + hiveHandle.getDbName() + "." + hiveHandle.getTableName()
                            + "; deferred to the M3-hive-acid-driver follow-up");
        }
    }

    @Override
    public void finishInsert(ConnectorSession session, ConnectorInsertHandle handle,
            Collection<byte[]> fragments) {
        Objects.requireNonNull(handle, "handle");
        HiveAcidContext context = ((HiveInsertHandle) handle).getContext();
        List<THivePartitionUpdate> updates = HiveCommitDataConverter.decodeFragments(fragments);
        try {
            if (context.isAcid()) {
                finishAcid(context, updates, HmsAcidOperation.INSERT);
            } else {
                HiveTableHandle tableHandle = rebuildHandle(context);
                new HiveCommitDriver(hmsClient, requireWriteOpsForNonAcid())
                        .commitNonAcid(tableHandle, context.getIntent(), updates);
            }
        } finally {
            context.cancelHeartbeat();
        }
    }

    @Override
    public void abortInsert(ConnectorSession session, ConnectorInsertHandle handle) {
        if (handle == null) {
            return;
        }
        abortContext(((HiveInsertHandle) handle).getContext());
    }

    @Override
    public ConnectorDeleteHandle beginDelete(ConnectorSession session, ConnectorTableHandle handle) {
        Objects.requireNonNull(handle, "handle");
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        HmsTableInfo tableInfo = hmsClient.getTable(hiveHandle.getDbName(), hiveHandle.getTableName());
        if (!HiveAcidUtil.isFullAcidTable(tableInfo)) {
            throw new DorisConnectorException(
                    "Row-level DELETE requires a hive ACID (transactional) table; "
                            + hiveHandle.getDbName() + "." + hiveHandle.getTableName()
                            + " is not transactional");
        }
        HiveAcidContext.Builder ctx = HiveAcidContext.builder(
                        hiveHandle.getDbName(), hiveHandle.getTableName(), WriteIntent.simple())
                .stagingLocation(tableInfo.getLocation())
                .acid(true);
        startAcidTransaction(ctx, hiveHandle, HmsAcidOperation.DELETE, WriteIntent.simple());
        return new HiveDeleteHandle(ctx.build());
    }

    @Override
    public void finishDelete(ConnectorSession session, ConnectorDeleteHandle handle,
            Collection<byte[]> fragments) {
        Objects.requireNonNull(handle, "handle");
        HiveAcidContext context = ((HiveDeleteHandle) handle).getContext();
        try {
            finishAcid(context, HiveCommitDataConverter.decodeFragments(fragments), HmsAcidOperation.DELETE);
        } finally {
            context.cancelHeartbeat();
        }
    }

    @Override
    public void abortDelete(ConnectorSession session, ConnectorDeleteHandle handle) {
        if (handle == null) {
            return;
        }
        abortContext(((HiveDeleteHandle) handle).getContext());
    }

    @Override
    public ConnectorMergeHandle beginMerge(ConnectorSession session, ConnectorTableHandle handle) {
        Objects.requireNonNull(handle, "handle");
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        HmsTableInfo tableInfo = hmsClient.getTable(hiveHandle.getDbName(), hiveHandle.getTableName());
        if (!HiveAcidUtil.isFullAcidTable(tableInfo)) {
            throw new DorisConnectorException(
                    "MERGE / UPDATE requires a hive ACID (transactional) table; "
                            + hiveHandle.getDbName() + "." + hiveHandle.getTableName()
                            + " is not transactional");
        }
        HiveAcidContext.Builder ctx = HiveAcidContext.builder(
                        hiveHandle.getDbName(), hiveHandle.getTableName(), WriteIntent.simple())
                .stagingLocation(tableInfo.getLocation())
                .acid(true);
        startAcidTransaction(ctx, hiveHandle, HmsAcidOperation.UPDATE, WriteIntent.simple());
        return new HiveMergeHandle(ctx.build());
    }

    @Override
    public void finishMerge(ConnectorSession session, ConnectorMergeHandle handle,
            Collection<byte[]> fragments) {
        Objects.requireNonNull(handle, "handle");
        HiveAcidContext context = ((HiveMergeHandle) handle).getContext();
        try {
            finishAcid(context, HiveCommitDataConverter.decodeFragments(fragments), HmsAcidOperation.UPDATE);
        } finally {
            context.cancelHeartbeat();
        }
    }

    @Override
    public void abortMerge(ConnectorSession session, ConnectorMergeHandle handle) {
        if (handle == null) {
            return;
        }
        abortContext(((HiveMergeHandle) handle).getContext());
    }

    // ========== ACID transaction helpers ==========

    private void startAcidTransaction(HiveAcidContext.Builder ctx, HiveTableHandle hiveHandle,
            HmsAcidOperation op, WriteIntent intent) {
        HmsWriteOps ops = requireWriteOps();
        boolean exclusive = intent.overwriteMode() != WriteIntent.OverwriteMode.NONE;
        long txnId = ops.openTxn(currentUser());
        long writeId;
        long lockId;
        try {
            writeId = ops.allocateWriteId(txnId, hiveHandle.getDbName(), hiveHandle.getTableName());
            lockId = ops.acquireLock(txnId, currentUser(), currentQueryId(),
                    hiveHandle.getDbName(), hiveHandle.getTableName(), !exclusive);
        } catch (RuntimeException e) {
            // best-effort abort: do not mask the original cause
            try {
                ops.abortTxn(txnId);
            } catch (RuntimeException abortFailure) {
                e.addSuppressed(abortFailure);
            }
            throw e;
        }
        ScheduledFuture<?> heartbeat = scheduleHeartbeat(ops, txnId, lockId);
        ctx.acidOperation(op).txnId(txnId).writeId(writeId).lockId(lockId).heartbeatFuture(heartbeat);
    }

    private ScheduledFuture<?> scheduleHeartbeat(HmsWriteOps ops, long txnId, long lockId) {
        if (heartbeatExecutor == null) {
            return null;
        }
        return heartbeatExecutor.scheduleAtFixedRate(() -> {
            try {
                ops.heartbeat(txnId, lockId);
            } catch (RuntimeException e) {
                LOG.warn("Heartbeat for hive ACID txn {} (lock {}) failed: {}", txnId, lockId, e.toString());
            }
        }, heartbeatPeriodMs, heartbeatPeriodMs, TimeUnit.MILLISECONDS);
    }

    private void finishAcid(HiveAcidContext context, List<THivePartitionUpdate> updates, HmsAcidOperation op) {
        HmsWriteOps ops = requireWriteOps();
        List<String> partitionNames = updates.stream()
                .map(THivePartitionUpdate::getName)
                .filter(n -> n != null && !n.isEmpty())
                .distinct()
                .collect(Collectors.toList());
        if (!partitionNames.isEmpty()) {
            ops.addDynamicPartitions(context.getTxnId(), context.getWriteId(),
                    context.getDbName(), context.getTableName(), partitionNames, op);
        }
        ops.commitTxn(context.getTxnId());
    }

    private void abortContext(HiveAcidContext context) {
        try {
            if (context.isAcid() && writeOps != null) {
                try {
                    writeOps.abortTxn(context.getTxnId());
                } catch (RuntimeException e) {
                    LOG.warn("abortTxn({}) failed during abort cleanup: {}",
                            context.getTxnId(), e.toString());
                }
            }
        } finally {
            context.cancelHeartbeat();
        }
    }

    private HmsWriteOps requireWriteOps() {
        if (writeOps == null) {
            throw new DorisConnectorException("Hive write path requires HmsWriteOps; none configured");
        }
        return writeOps;
    }

    private HmsWriteOps requireWriteOpsForNonAcid() {
        return requireWriteOps();
    }

    private HiveTableHandle rebuildHandle(HiveAcidContext context) {
        HmsTableInfo tableInfo = hmsClient.getTable(context.getDbName(), context.getTableName());
        List<String> partKeyNames = tableInfo.getPartitionKeys() != null
                ? tableInfo.getPartitionKeys().stream().map(ConnectorColumn::getName).collect(Collectors.toList())
                : Collections.emptyList();
        return new HiveTableHandle.Builder(context.getDbName(), context.getTableName(),
                HiveTableType.HIVE)
                .inputFormat(tableInfo.getInputFormat())
                .serializationLib(tableInfo.getSerializationLib())
                .location(tableInfo.getLocation())
                .partitionKeyNames(partKeyNames)
                .sdParameters(tableInfo.getSdParameters())
                .tableParameters(tableInfo.getParameters())
                .build();
    }

    private static String currentUser() {
        String user = System.getProperty("user.name");
        return user == null ? "doris" : user;
    }

    private static String currentQueryId() {
        // queryId is plumbed through ConnectContext in fe-core; the plugin
        // does not yet have a session-scoped query id, so derive a
        // monotonic identifier per JVM. Lock requests only need it for
        // metastore-side grouping / debugging.
        return "doris-" + System.nanoTime();
    }

    /** Visible for tests: stmtId sequence used when building delta directories. */
    int nextStmtId() {
        return (int) stmtIdSequence.getAndIncrement();
    }

    /** Visible for tests: a single-thread heartbeat executor with daemon threads. */
    public static ScheduledExecutorService newHeartbeatExecutor() {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "hive-acid-heartbeat");
            t.setDaemon(true);
            return t;
        });
    }
}
