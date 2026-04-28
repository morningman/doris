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

package org.apache.doris.datasource;

import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.PassthroughQueryTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorFilterConstraint;
import org.apache.doris.connector.api.pushdown.FilterApplicationResult;
import org.apache.doris.connector.api.pushdown.LimitApplicationResult;
import org.apache.doris.connector.api.pushdown.ProjectionApplicationResult;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRequest;
import org.apache.doris.connector.api.scan.ScanNodePropertiesResult;
import org.apache.doris.connector.api.timetravel.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.timetravel.ConnectorRefSpec;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileTextScanRangeParams;
import org.apache.doris.thrift.TPushAggOp;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Generic scan node that delegates scan planning to the connector SPI.
 *
 * <p>Replaces connector-specific ScanNode subclasses for plugin-driven catalogs.
 * Extends {@link FileQueryScanNode} to reuse the existing split-to-Thrift
 * conversion pipeline. Uses {@code FORMAT_JNI} by default, which routes
 * to BE's JNI scanner framework.</p>
 *
 * <p>Scan flow:</p>
 * <ol>
 *   <li>{@link #getSplits} calls {@link ConnectorScanPlanProvider#planScan}
 *       to obtain {@link ConnectorScanRange}s from the connector plugin</li>
 *   <li>Each range is wrapped in a {@link PluginDrivenSplit}</li>
 *   <li>{@link FileQueryScanNode#createScanRangeLocations} distributes splits
 *       to backends</li>
 *   <li>{@link #setScanParams} populates {@link TTableFormatFileDesc} with
 *       connector-specific properties for each split</li>
 * </ol>
 */
public class PluginDrivenScanNode extends FileQueryScanNode {

    private static final Logger LOG = LogManager.getLogger(PluginDrivenScanNode.class);

    private static final String TABLE_FORMAT_TYPE = "plugin_driven";

    /** Scan node property keys (shared with connector plugins). */
    private static final String PROP_FILE_FORMAT_TYPE = "file_format_type";
    private static final String PROP_PATH_PARTITION_KEYS = "path_partition_keys";
    private static final String PROP_LOCATION_PREFIX = "location.";
    private static final String PROP_HIVE_TEXT_PREFIX = "hive.text.";

    private final Connector connector;
    private final ConnectorSession connectorSession;

    // Set during filter pushdown; may be updated from the original table handle.
    private ConnectorTableHandle currentHandle;

    // Populated from ConnectorScanPlanProvider.getScanNodePropertiesResult()
    private ScanNodePropertiesResult cachedPropertiesResult;
    private Map<String, String> scanNodeProperties;
    // Maps filtered conjunct indices (after CAST removal) back to original conjunct indices
    private List<Integer> filteredToOriginalIndex;

    // Time-travel coordinates resolved from the Nereids translator before
    // getSplits() runs. All three are optional and surface to the connector
    // via ConnectorScanRequest. The first time getSplits() runs and either
    // {@code connectorRefSpec} or {@link FileQueryScanNode#getConnectorTableVersion()}
    // is set, the table handle is refreshed via the time-travel-aware
    // 5-arg getTableHandle(); see resolveTableHandleWithTimeTravel().
    private ConnectorRefSpec connectorRefSpec;
    private ConnectorMvccSnapshot connectorMvccSnapshot;
    private boolean handleResolvedWithTimeTravel;

    public PluginDrivenScanNode(PlanNodeId id, TupleDescriptor desc,
            boolean needCheckColumnPriv, SessionVariable sv,
            ScanContext scanContext, Connector connector,
            ConnectorSession connectorSession, ConnectorTableHandle tableHandle) {
        super(id, desc, "PluginDrivenScanNode", scanContext, needCheckColumnPriv, sv);
        this.connector = connector;
        this.connectorSession = connectorSession;
        this.currentHandle = tableHandle;
    }

    /**
     * Creates a PluginDrivenScanNode by resolving the connector, session, and table handle
     * from the plugin-driven catalog and table.
     */
    public static PluginDrivenScanNode create(PlanNodeId id, TupleDescriptor desc,
            boolean needCheckColumnPriv, SessionVariable sv,
            ScanContext scanContext, PluginDrivenExternalCatalog catalog,
            PluginDrivenExternalTable table) {
        Connector connector = catalog.getConnector();
        ConnectorSession session = catalog.buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);
        String dbName = table.getDb() != null ? table.getDb().getRemoteName() : "";
        String tableName = table.getRemoteName();
        ConnectorTableHandle handle = metadata.getTableHandle(session, dbName, tableName)
                .orElseThrow(() -> new RuntimeException(
                        "Table handle not found for plugin-driven table: " + dbName + "." + tableName));
        return new PluginDrivenScanNode(id, desc, needCheckColumnPriv, sv,
                scanContext, connector, session, handle);
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        if (currentHandle instanceof PassthroughQueryTableHandle) {
            output.append(prefix).append("TABLE VALUE FUNCTION\n");
            String query = ((PassthroughQueryTableHandle) currentHandle).getQuery();
            output.append(prefix).append("QUERY: ").append(query).append("\n");
        } else {
            Map<String, String> props = getOrLoadScanNodeProperties();
            String query = props.get("query");
            output.append(prefix).append("TABLE: ")
                    .append(desc.getTable().getNameWithFullQualifiers()).append("\n");
            if (query != null) {
                output.append(prefix).append("QUERY: ").append(query).append("\n");
            }
            if (!conjuncts.isEmpty()) {
                Expr expr = convertConjunctsToAndCompoundPredicate(conjuncts);
                output.append(prefix).append("PREDICATES: ")
                        .append(expr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE))
                        .append("\n");
            }
            // Delegate connector-specific EXPLAIN info to the SPI
            ConnectorScanPlanProvider scanProvider = connector.getScanPlanProvider();
            if (scanProvider != null) {
                scanProvider.appendExplainInfo(output, prefix, props);
            }
            // Show ES terminate_after optimization when limit is pushed to ES
            if (limit > 0 && conjuncts.isEmpty()
                    && "es_http".equals(props.get(PROP_FILE_FORMAT_TYPE))) {
                output.append(prefix).append("ES terminate_after: ")
                        .append(limit).append("\n");
            }
        }
        if (useTopnFilter()) {
            String topnFilterSources = String.join(",",
                    topnFilterSortNodes.stream()
                            .map(node -> node.getId().asInt() + "").collect(Collectors.toList()));
            output.append(prefix).append("TOPN OPT:").append(topnFilterSources).append("\n");
        }
        return output.toString();
    }

    @Override
    protected TFileFormatType getFileFormatType() throws UserException {
        Map<String, String> props = getOrLoadScanNodeProperties();
        String format = props.get(PROP_FILE_FORMAT_TYPE);
        if (format != null) {
            return mapFileFormatType(format);
        }
        return TFileFormatType.FORMAT_JNI;
    }

    @Override
    protected List<String> getPathPartitionKeys() throws UserException {
        Map<String, String> props = getOrLoadScanNodeProperties();
        String keys = props.get(PROP_PATH_PARTITION_KEYS);
        if (keys != null && !keys.isEmpty()) {
            return Arrays.asList(keys.split(","));
        }
        return Collections.emptyList();
    }

    @Override
    protected TableIf getTargetTable() throws UserException {
        return desc.getTable();
    }

    @Override
    protected Map<String, String> getLocationProperties() throws UserException {
        Map<String, String> props = getOrLoadScanNodeProperties();
        Map<String, String> locationProps = new HashMap<>();
        for (Map.Entry<String, String> entry : props.entrySet()) {
            if (entry.getKey().startsWith(PROP_LOCATION_PREFIX)) {
                String realKey = entry.getKey().substring(PROP_LOCATION_PREFIX.length());
                locationProps.put(realKey, entry.getValue());
            }
        }
        return locationProps;
    }

    @Override
    protected TFileAttributes getFileAttributes() throws UserException {
        Map<String, String> props = getOrLoadScanNodeProperties();
        String serDeLib = props.get(PROP_HIVE_TEXT_PREFIX + "serde_lib");
        if (serDeLib == null || serDeLib.isEmpty()) {
            return new TFileAttributes();
        }

        TFileAttributes attrs = new TFileAttributes();
        String skipLinesStr = props.get(PROP_HIVE_TEXT_PREFIX + "skip_lines");
        if (skipLinesStr != null) {
            try {
                attrs.setSkipLines(Integer.parseInt(skipLinesStr));
            } catch (NumberFormatException e) {
                // ignore
            }
        }

        TFileTextScanRangeParams textParams = new TFileTextScanRangeParams();
        String colSep = props.get(PROP_HIVE_TEXT_PREFIX + "column_separator");
        if (colSep != null) {
            textParams.setColumnSeparator(colSep);
        }
        String lineSep = props.get(PROP_HIVE_TEXT_PREFIX + "line_delimiter");
        if (lineSep != null) {
            textParams.setLineDelimiter(lineSep);
        }
        String mapkvDelim = props.get(PROP_HIVE_TEXT_PREFIX + "mapkv_delimiter");
        if (mapkvDelim != null) {
            textParams.setMapkvDelimiter(mapkvDelim);
        }
        String collDelim = props.get(PROP_HIVE_TEXT_PREFIX + "collection_delimiter");
        if (collDelim != null) {
            textParams.setCollectionDelimiter(collDelim);
        }
        String escape = props.get(PROP_HIVE_TEXT_PREFIX + "escape");
        if (escape != null && !escape.isEmpty()) {
            textParams.setEscape(escape.getBytes()[0]);
        }
        String nullFmt = props.get(PROP_HIVE_TEXT_PREFIX + "null_format");
        if (nullFmt != null) {
            textParams.setNullFormat(nullFmt);
        }
        String enclose = props.get(PROP_HIVE_TEXT_PREFIX + "enclose");
        if (enclose != null && !enclose.isEmpty()) {
            textParams.setEnclose(enclose.getBytes()[0]);
            attrs.setTrimDoubleQuotes(true);
        }

        attrs.setTextParams(textParams);
        attrs.setHeaderType("");
        attrs.setEnableTextValidateUtf8(sessionVariable.enableTextValidateUtf8);

        String isJson = props.get(PROP_HIVE_TEXT_PREFIX + "is_json");
        if ("true".equals(isJson)) {
            attrs.setReadJsonByLine(true);
            attrs.setReadByColumnDef(true);
        }

        return attrs;
    }

    @Override
    protected void convertPredicate() {
        // Attempt filter pushdown via the connector SPI
        if (conjuncts == null || conjuncts.isEmpty()) {
            return;
        }
        ConnectorMetadata metadata = connector.getMetadata(connectorSession);
        ConnectorFilterConstraint constraint = buildFilterConstraint(conjuncts);
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(connectorSession, currentHandle, constraint);
        if (result.isPresent()) {
            FilterApplicationResult<ConnectorTableHandle> filterResult = result.get();
            currentHandle = filterResult.getHandle();

            // Consume remainingFilter to avoid duplicate predicate evaluation on BE:
            // - null means all predicates were fully pushed down → clear conjuncts
            // - non-null means some/all predicates remain → keep conjuncts (conservative)
            ConnectorExpression remaining = filterResult.getRemainingFilter();
            if (remaining == null) {
                conjuncts.clear();
                LOG.debug("Filter fully pushed down for plugin-driven scan, cleared conjuncts");
            } else {
                // Partial or full remaining: keep all conjuncts for BE-side evaluation.
                // Fine-grained conjunct removal (matching individual remaining sub-expressions
                // back to original Expr conjuncts) is deferred to a future enhancement.
                LOG.debug("Filter pushdown accepted with remaining filter, keeping conjuncts");
            }
        }
        // Invalidate cached properties so they are rebuilt with the updated conjuncts/handle.
        scanNodeProperties = null;
        cachedPropertiesResult = null;
        filteredToOriginalIndex = null;
    }

    /**
     * Attempts to push the limit down via the SPI applyLimit() protocol.
     * Called before getSplits(), after filter pushdown.
     *
     * <p>If the connector accepts the limit, the handle is updated.
     * The limit is also propagated through the {@link ConnectorScanRequest}
     * passed to {@code planScan()} so connectors that handle the limit
     * directly during scan planning (e.g., JDBC, MaxCompute) can read it
     * from {@link ConnectorScanRequest#getLimit()}.</p>
     */
    private void tryPushDownLimit() {
        if (limit <= 0) {
            return;
        }
        ConnectorMetadata metadata = connector.getMetadata(connectorSession);
        Optional<LimitApplicationResult<ConnectorTableHandle>> result =
                metadata.applyLimit(connectorSession, currentHandle, limit);
        if (result.isPresent()) {
            currentHandle = result.get().getHandle();
            LOG.debug("Limit {} pushed down via applyLimit for plugin-driven scan", limit);
        }
    }

    /**
     * Attempts to push the projection down via the SPI applyProjection() protocol.
     * Called before getSplits(), after filter and limit pushdown.
     *
     * <p>If the connector accepts the projection, the handle is updated.</p>
     */
    private void tryPushDownProjection(List<ConnectorColumnHandle> columns) {
        if (columns.isEmpty()) {
            return;
        }
        ConnectorMetadata metadata = connector.getMetadata(connectorSession);
        Optional<ProjectionApplicationResult<ConnectorTableHandle>> result =
                metadata.applyProjection(connectorSession, currentHandle, columns);
        if (result.isPresent()) {
            currentHandle = result.get().getHandle();
            LOG.debug("Projection pushed down via applyProjection for plugin-driven scan");
        }
    }

    /**
     * Sets the SPI-typed branch/tag pin resolved from the Nereids
     * {@code @branch(...)/@tag(...)} TVF syntax. Mirrors
     * {@link FileQueryScanNode#setConnectorTableVersion}: both fields are
     * read once at the top of {@link #getSplits(int)} and propagated
     * through the {@link ConnectorScanRequest}.
     */
    public void setConnectorRefSpec(ConnectorRefSpec connectorRefSpec) {
        this.connectorRefSpec = connectorRefSpec;
    }

    public ConnectorRefSpec getConnectorRefSpec() {
        return connectorRefSpec;
    }

    /**
     * Sets the engine-resolved MVCC snapshot for this scan. Currently
     * unused at FE planning time; the field exists so MTMV refresh and
     * other engine paths that thread an opaque snapshot through the scan
     * can populate it without re-plumbing the constructor. See
     * {@link org.apache.doris.connector.timetravel.MvccSnapshotPersistence}
     * for the FE→state-store round-trip helper.
     */
    public void setConnectorMvccSnapshot(ConnectorMvccSnapshot connectorMvccSnapshot) {
        this.connectorMvccSnapshot = connectorMvccSnapshot;
    }

    public ConnectorMvccSnapshot getConnectorMvccSnapshot() {
        return connectorMvccSnapshot;
    }

    /**
     * Refreshes {@link #currentHandle} via the time-travel-aware 5-arg
     * {@link ConnectorMetadata#getTableHandle(ConnectorSession, String,
     * String, java.util.Optional, java.util.Optional)} when either a
     * {@link ConnectorTableVersion} or {@link ConnectorRefSpec} has been
     * threaded in by the Nereids translator.
     *
     * <p>The constructor / {@link #create} resolves the handle eagerly via
     * the legacy 3-arg overload because at create-time the translator has
     * not yet attached time-travel coordinates. We re-resolve lazily here,
     * exactly once per scan, just before {@code planScan} runs. Connectors
     * whose default 5-arg {@code getTableHandle} delegates back to the
     * 3-arg form keep working unchanged.</p>
     */
    private void resolveTableHandleWithTimeTravel() {
        if (handleResolvedWithTimeTravel) {
            return;
        }
        ConnectorTableVersion version = getConnectorTableVersion();
        if (version == null && connectorRefSpec == null) {
            return;
        }
        TableIf tbl = desc.getTable();
        if (!(tbl instanceof PluginDrivenExternalTable)) {
            return;
        }
        PluginDrivenExternalTable pdt = (PluginDrivenExternalTable) tbl;
        String dbName = pdt.getDb() != null ? pdt.getDb().getRemoteName() : "";
        String tableName = pdt.getRemoteName();
        ConnectorMetadata metadata = connector.getMetadata(connectorSession);
        Optional<ConnectorTableHandle> refreshed = metadata.getTableHandle(
                connectorSession, dbName, tableName,
                Optional.ofNullable(version),
                Optional.ofNullable(connectorRefSpec));
        if (refreshed.isPresent()) {
            currentHandle = refreshed.get();
            // Cached scan properties were derived from the un-pinned handle;
            // invalidate so EXPLAIN / split planning re-fetches against the
            // pinned handle.
            scanNodeProperties = null;
            cachedPropertiesResult = null;
        }
        handleResolvedWithTimeTravel = true;
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {
        // Refresh handle with time-travel coordinates (FOR VERSION/TIME AS OF
        // and @branch/@tag) before any pushdown reads from currentHandle.
        resolveTableHandleWithTimeTravel();
        // Attempt limit and projection pushdown via SPI protocol
        tryPushDownLimit();

        ConnectorScanPlanProvider scanProvider = connector.getScanPlanProvider();
        if (scanProvider == null) {
            LOG.warn("Connector does not provide a scan plan provider, returning empty splits");
            return Collections.emptyList();
        }

        List<ConnectorColumnHandle> columns = buildColumnHandles();
        tryPushDownProjection(columns);
        Optional<ConnectorExpression> remainingFilter = buildRemainingFilter();

        Optional<Long> countPushdown = Optional.empty();
        if (getPushDownAggNoGroupingOp() == TPushAggOp.COUNT) {
            countPushdown = scanProvider.getCountPushdownResult(
                    connectorSession, currentHandle, remainingFilter);
        }

        List<ConnectorScanRange> ranges = scanProvider.planScan(
                buildScanRequest(columns, remainingFilter));

        if (countPushdown.isPresent()) {
            long total = countPushdown.get();
            setPushDownCount(total);
            ranges = applyCountToRanges(ranges, total);
        }

        List<Split> splits = new ArrayList<>(ranges.size());
        for (ConnectorScanRange range : ranges) {
            splits.add(new PluginDrivenSplit(range));
        }
        return splits;
    }

    /**
     * Distributes a metadata-only {@code COUNT(*)} total across the
     * produced ranges. Mirrors the legacy
     * {@code IcebergScanNode#assignCountToSplits} algorithm: each of the
     * first {@code N-1} ranges receives {@code total/N} and the LAST
     * range receives {@code total/N + total%N}, so the integer remainder
     * is captured exactly without inflating the per-range count beyond
     * what BE can attribute to that scan task.
     *
     * <p>Returns a new list; original ranges are not mutated. For ranges
     * that opt out of count pushdown (default
     * {@link ConnectorScanRange#withTableLevelRowCount} returns
     * {@code this}), the override is a no-op so the call is safe across
     * connector types.</p>
     */
    static List<ConnectorScanRange> applyCountToRanges(
            List<ConnectorScanRange> ranges, long total) {
        if (ranges.isEmpty()) {
            return ranges;
        }
        int n = ranges.size();
        long perRange = total / n;
        long remainder = total % n;
        List<ConnectorScanRange> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            long count = (i == n - 1) ? perRange + remainder : perRange;
            out.add(ranges.get(i).withTableLevelRowCount(count));
        }
        return out;
    }

    /**
     * Builds the {@link ConnectorScanRequest} fed to
     * {@link ConnectorScanPlanProvider#planScan}, threading the engine-side
     * limit and the optional time-travel coordinates
     * ({@link ConnectorTableVersion}, {@link ConnectorRefSpec},
     * {@link ConnectorMvccSnapshot}) through to the connector. Package-
     * private so tests can exercise the request shape without driving the
     * full {@link #getSplits} pipeline.
     */
    ConnectorScanRequest buildScanRequest(List<ConnectorColumnHandle> columns,
                                          Optional<ConnectorExpression> remainingFilter) {
        ConnectorScanRequest.Builder builder = ConnectorScanRequest.builder()
                .session(connectorSession)
                .table(currentHandle)
                .columns(columns)
                .filter(remainingFilter)
                .version(Optional.ofNullable(getConnectorTableVersion()))
                .refSpec(Optional.ofNullable(connectorRefSpec))
                .mvccSnapshot(Optional.ofNullable(connectorMvccSnapshot));
        if (limit > 0) {
            builder.limit(OptionalLong.of(limit));
        }
        return builder.build();
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        if (!(split instanceof PluginDrivenSplit)) {
            return;
        }
        PluginDrivenSplit pluginSplit = (PluginDrivenSplit) split;
        ConnectorScanRange scanRange = pluginSplit.getConnectorScanRange();

        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(scanRange.getTableFormatType());

        // Delegate format-specific Thrift construction to the connector SPI
        scanRange.populateRangeParams(tableFormatFileDesc, rangeDesc);

        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }


    @Override
    protected Optional<String> getSerializedTable() {
        ConnectorScanPlanProvider scanProvider = connector.getScanPlanProvider();
        if (scanProvider != null) {
            Map<String, String> props = getOrLoadScanNodeProperties();
            String serialized = scanProvider.getSerializedTable(props);
            if (serialized != null) {
                return Optional.of(serialized);
            }
        }
        return Optional.empty();
    }

    @Override
    public void createScanRangeLocations() throws UserException {
        super.createScanRangeLocations();
        // Delegate scan-level Thrift params to the connector SPI
        ConnectorScanPlanProvider scanProvider = connector.getScanPlanProvider();
        if (scanProvider != null) {
            Map<String, String> props = getOrLoadScanNodeProperties();
            scanProvider.populateScanLevelParams(params, props);
        }
        pruneConjunctsFromNodeProperties();

        // Push down limit to ES via terminate_after optimization.
        // When all predicates are pushed to ES (conjuncts empty) and limit fits in one batch,
        // ES can use terminate_after to stop scanning early instead of scrolling all results.
        if (limit > 0 && limit <= sessionVariable.batchSize && conjuncts.isEmpty()
                && params.isSetEsProperties()) {
            params.getEsProperties().put("limit", String.valueOf(limit));
        }
    }


    /**
     * Prunes pushed-down conjuncts using the structured result from
     * {@link ConnectorScanPlanProvider#getScanNodePropertiesResult()}.
     *
     * <p>Only conjuncts whose indices are in the not-pushed set are retained.
     * If the connector has no not-pushed tracking (empty set), all conjuncts
     * are considered pushed and cleared.</p>
     */
    private void pruneConjunctsFromNodeProperties() {
        if (conjuncts == null || conjuncts.isEmpty()) {
            return;
        }
        ScanNodePropertiesResult result = getOrLoadPropertiesResult();

        if (!result.hasConjunctTracking()) {
            // No conjunct tracking — do not prune (keep all conjuncts for safety)
            return;
        }

        // notPushedSet indices are relative to the filtered conjunct list
        // (after CAST expr removal). Map them back to original conjunct indices.
        Set<Integer> notPushedSet = result.getNotPushedConjunctIndices();
        Set<Integer> originalNotPushed = new HashSet<>();
        if (filteredToOriginalIndex != null) {
            for (int filteredIdx : notPushedSet) {
                if (filteredIdx < filteredToOriginalIndex.size()) {
                    originalNotPushed.add(filteredToOriginalIndex.get(filteredIdx));
                }
            }
        } else {
            // No CAST filtering was applied — indices map 1:1
            originalNotPushed.addAll(notPushedSet);
        }

        // Also keep any conjuncts that were filtered out (CAST expressions)
        // since those were never sent to the connector for pushdown
        if (filteredToOriginalIndex != null) {
            Set<Integer> sentToConnector = new HashSet<>(filteredToOriginalIndex);
            for (int i = 0; i < conjuncts.size(); i++) {
                if (!sentToConnector.contains(i)) {
                    originalNotPushed.add(i);
                }
            }
        }

        List<Expr> remaining = new ArrayList<>();
        for (int i = 0; i < conjuncts.size(); i++) {
            if (originalNotPushed.contains(i)) {
                remaining.add(conjuncts.get(i));
            }
        }
        conjuncts.clear();
        conjuncts.addAll(remaining);
    }

    /**
     * Lazily loads and caches the ScanNodePropertiesResult from the connector.
     * Both getOrLoadScanNodeProperties() and pruneConjunctsFromNodeProperties()
     * use this to avoid redundant computation.
     */
    private ScanNodePropertiesResult getOrLoadPropertiesResult() {
        if (cachedPropertiesResult == null) {
            ConnectorScanPlanProvider scanProvider = connector.getScanPlanProvider();
            if (scanProvider != null) {
                List<ConnectorColumnHandle> columns = buildColumnHandles();
                Optional<ConnectorExpression> filter = buildRemainingFilter();
                cachedPropertiesResult = scanProvider.getScanNodePropertiesResult(
                        connectorSession, currentHandle, columns, filter);
            }
            if (cachedPropertiesResult == null) {
                cachedPropertiesResult = new ScanNodePropertiesResult(Collections.emptyMap());
            }
        }
        return cachedPropertiesResult;
    }

    /**
     * Lazily loads scan node properties from the connector's scan plan provider.
     */
    private Map<String, String> getOrLoadScanNodeProperties() {
        if (scanNodeProperties == null) {
            scanNodeProperties = getOrLoadPropertiesResult().getProperties();
            if (scanNodeProperties == null) {
                scanNodeProperties = Collections.emptyMap();
            }
        }
        return scanNodeProperties;
    }

    /**
     * Maps a file format name string to the corresponding TFileFormatType.
     */
    private static TFileFormatType mapFileFormatType(String format) {
        switch (format.toLowerCase()) {
            case "parquet":
                return TFileFormatType.FORMAT_PARQUET;
            case "orc":
                return TFileFormatType.FORMAT_ORC;
            case "text":
            case "csv":
                return TFileFormatType.FORMAT_CSV_PLAIN;
            case "json":
                return TFileFormatType.FORMAT_JSON;
            case "avro":
                return TFileFormatType.FORMAT_AVRO;
            case "es_http":
                return TFileFormatType.FORMAT_ES_HTTP;
            default:
                return TFileFormatType.FORMAT_JNI;
        }
    }

    /**
     * Builds column handles from the tuple descriptor's slot descriptors.
     * These tell the connector which columns are needed for the query,
     * enabling optimized column selection (e.g., SELECT col1, col2 instead of SELECT *).
     */
    private List<ConnectorColumnHandle> buildColumnHandles() {
        ConnectorMetadata metadata = connector.getMetadata(connectorSession);
        Map<String, ConnectorColumnHandle> allHandles =
                metadata.getColumnHandles(connectorSession, currentHandle);
        if (allHandles.isEmpty()) {
            return Collections.emptyList();
        }
        List<ConnectorColumnHandle> selected = new ArrayList<>();
        for (org.apache.doris.analysis.SlotDescriptor slot : desc.getSlots()) {
            if (slot.getColumn() != null) {
                ConnectorColumnHandle ch = allHandles.get(slot.getColumn().getName());
                if (ch != null) {
                    selected.add(ch);
                }
            }
        }
        return selected;
    }

    /**
     * Builds a {@link ConnectorFilterConstraint} from the current conjuncts.
     */
    private ConnectorFilterConstraint buildFilterConstraint(List<Expr> exprs) {
        ConnectorExpression combined = ExprToConnectorExpressionConverter.convertConjuncts(exprs);
        return new ConnectorFilterConstraint(combined, Collections.emptyMap());
    }

    /**
     * Builds the remaining filter expression from unconsumed conjuncts.
     * If no conjuncts remain, returns {@link Optional#empty()}.
     * Filters out CAST-containing predicates when the connector does not support CAST pushdown.
     */
    private Optional<ConnectorExpression> buildRemainingFilter() {
        if (conjuncts == null || conjuncts.isEmpty()) {
            filteredToOriginalIndex = null;
            return Optional.empty();
        }
        List<Expr> pushableConjuncts = conjuncts;
        ConnectorMetadata metadata = connector.getMetadata(connectorSession);
        if (!metadata.supportsCastPredicatePushdown(connectorSession)) {
            filteredToOriginalIndex = new ArrayList<>();
            pushableConjuncts = new ArrayList<>();
            for (int i = 0; i < conjuncts.size(); i++) {
                if (!containsCastExpr(conjuncts.get(i))) {
                    pushableConjuncts.add(conjuncts.get(i));
                    filteredToOriginalIndex.add(i);
                }
            }
            // If no filtering occurred, clear the mapping (1:1)
            if (filteredToOriginalIndex.size() == conjuncts.size()) {
                filteredToOriginalIndex = null;
            }
        } else {
            filteredToOriginalIndex = null;
        }
        if (pushableConjuncts.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(ExprToConnectorExpressionConverter.convertConjuncts(pushableConjuncts));
    }

    private static boolean containsCastExpr(Expr expr) {
        List<CastExpr> castExprs = new ArrayList<>();
        expr.collect(CastExpr.class, castExprs);
        return !castExprs.isEmpty();
    }
}
