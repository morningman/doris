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

package org.apache.doris.connector.iceberg.source;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;
import org.apache.doris.connector.api.scan.ConnectorScanRequest;
import org.apache.doris.connector.api.timetravel.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;
import org.apache.doris.connector.iceberg.IcebergColumnHandle;
import org.apache.doris.connector.iceberg.IcebergConnectorMvccSnapshot;
import org.apache.doris.connector.iceberg.IcebergPredicateConverter;
import org.apache.doris.connector.iceberg.IcebergTableHandle;
import org.apache.doris.connector.iceberg.cache.IcebergPluginManifestCache;
import org.apache.doris.connector.iceberg.cache.ManifestCacheKey;
import org.apache.doris.connector.iceberg.cache.ManifestCacheValue;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.thrift.TFileScanRangeParams;

import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IcebergPluginInternals;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.expressions.True;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;

/**
 * Plugin-side {@link ConnectorScanPlanProvider} for the iceberg business
 * read path. Mirrors the legacy fe-core
 * {@code IcebergScanNode#getSplits} / {@code createIcebergSplit} pipeline:
 *
 * <pre>
 *   table = loadIcebergTable(handle)
 *   scan  = table.newScan()
 *               (.useSnapshot(snapshotId)?)
 *               (.filter(IcebergPredicateConverter.convert(...))?)
 *               (.select(columnNames)?)
 *   for task in TableScanUtil.splitFiles(scan.planFiles(), scan.targetSplitSize()):
 *       emit IcebergScanRange(task)
 * </pre>
 *
 * <p>Out-of-scope deferrals (per task brief):
 * <ul>
 *   <li>manifest-cache binding (M2-Iceberg-06)</li>
 *   <li>time travel / branch / tag (M2-Iceberg-07) — the
 *       {@link IcebergTableHandle#getRefSpec()} is intentionally <b>not</b>
 *       consumed; this PR always reads the pinned {@code snapshotId} when
 *       present, otherwise the table's {@code currentSnapshot()}.</li>
 *   <li>{@code LocationProvider} URI normalisation — paths are stored
 *       as-is on the produced ranges; engine-side glue resolves them.</li>
 *   <li>Engine wiring of {@link #getCountPushdownResult} into
 *       {@link IcebergScanRange#getTableLevelRowCount()} (M2-Iceberg-05b /
 *       follow-up): this provider exposes the metadata-only count on the
 *       SPI hook, but does not eagerly populate the per-range row count
 *       because the SPI carries no signal yet that a {@code COUNT(*)}
 *       aggregate is being pushed down.</li>
 * </ul>
 */
public class IcebergScanPlanProvider implements ConnectorScanPlanProvider {

    private static final Logger LOG = LogManager.getLogger(IcebergScanPlanProvider.class);

    /** Scan-node property keys produced by {@link #getScanNodeProperties}. */
    public static final String PROP_FILE_FORMAT_TYPE = "file_format_type";
    public static final String PROP_ICEBERG_FORMAT_VERSION = "iceberg.format_version";

    /**
     * Prefix under which the merged storage / vended-credential properties
     * are exposed on the scan-node-properties map. Stripped by
     * {@link #populateScanLevelParams} when copying into
     * {@code TFileScanRangeParams.properties} (PlanNodes.thrift field 9).
     */
    public static final String PROP_LOCATION_PREFIX = "iceberg.location.";

    /**
     * Name-mapping JSON key. Set on the scan-node-properties map when the
     * iceberg table's {@link TableProperties#DEFAULT_NAME_MAPPING} is
     * non-null, and copied verbatim into
     * {@code TFileScanRangeParams.properties} by
     * {@link #populateScanLevelParams}.
     */
    public static final String PROP_NAME_MAPPING = "iceberg.name-mapping";

    /**
     * Scan-node-property key carrying the iceberg snapshot id selected
     * for this plan (handle's pinned snapshot, or
     * {@code table.currentSnapshot()} when unpinned). Used by
     * {@link #appendExplainInfo} to render the time-travel coordinate.
     */
    public static final String PROP_SNAPSHOT_ID = "iceberg.snapshot_id";

    /**
     * Scan-node-property key carrying the diagnostic ref label
     * ({@code "branch:<name>"} / {@code "tag:<name>"}) when the handle
     * was resolved from a ref. Engine-side scan resolution always reads
     * {@link #PROP_SNAPSHOT_ID}; this key is purely for EXPLAIN.
     */
    public static final String PROP_REF_SPEC = "iceberg.ref_spec";

    /**
     * Session-property key consulted by {@link #getCountPushdownResult}
     * when the table has only position deletes. Mirrors the legacy
     * {@code SessionVariable#ignoreIcebergDanglingDelete}; absent / "false"
     * means count cannot be pushed down through dangling deletes.
     */
    public static final String SESSION_VAR_IGNORE_DANGLING_DELETE = "ignore_iceberg_dangling_delete";

    private static final String ICEBERG_TOTAL_RECORDS = "total-records";
    private static final String ICEBERG_TOTAL_POSITION_DELETES = "total-position-deletes";
    private static final String ICEBERG_TOTAL_EQUALITY_DELETES = "total-equality-deletes";

    private static final int MIN_DELETE_FILE_VERSION = 2;
    private static final int MIN_ROW_LINEAGE_VERSION = 3;

    private final BiFunction<String, String, Table> tableLoader;
    private final Map<String, String> catalogProperties;
    private final ConnectorContext context;
    private final String defaultTimeZone;
    private final IcebergPluginManifestCache manifestCache;
    private final boolean manifestCacheEnabled;
    private final String catalogName;

    private final LongAdder manifestCacheHits = new LongAdder();
    private final LongAdder manifestCacheMisses = new LongAdder();
    private final LongAdder manifestCacheFailures = new LongAdder();

    /**
     * Construct a provider with the manifest cache disabled. Equivalent
     * to {@link #IcebergScanPlanProvider(BiFunction, Map, ConnectorContext,
     * IcebergPluginManifestCache, boolean)} with a {@code null} cache and
     * {@code false} enable flag.
     */
    public IcebergScanPlanProvider(BiFunction<String, String, Table> tableLoader,
                                   Map<String, String> catalogProperties,
                                   ConnectorContext context) {
        this(tableLoader, catalogProperties, context, null, false);
    }

    /**
     * Construct a provider.
     *
     * @param tableLoader      {@code (db, table) -> iceberg.Table}; typically backed
     *                         by the iceberg plugin's
     *                         {@code iceberg.table} {@code MetaCacheHandle}
     *                         so cache-driven invalidation propagates.
     * @param catalogProperties iceberg connector properties (used for the
     *                         explain summary and for cache-key derivation).
     * @param context          connector context (may be {@code null} in tests).
     * @param manifestCache    plugin-private manifest cache (per-connector
     *                         singleton owned by the connector — may be
     *                         {@code null} when the cache is disabled).
     * @param manifestCacheEnabled when {@code true}, scan planning routes
     *                         through {@link #planFileScanTaskWithManifestCache}
     *                         and falls back to {@link TableScan#planFiles()}
     *                         on any error.
     */
    public IcebergScanPlanProvider(BiFunction<String, String, Table> tableLoader,
                                   Map<String, String> catalogProperties,
                                   ConnectorContext context,
                                   IcebergPluginManifestCache manifestCache,
                                   boolean manifestCacheEnabled) {
        this.tableLoader = Objects.requireNonNull(tableLoader, "tableLoader");
        this.catalogProperties = catalogProperties == null
                ? Collections.emptyMap()
                : catalogProperties;
        this.context = context;
        this.defaultTimeZone = "UTC";
        this.manifestCache = manifestCache;
        this.manifestCacheEnabled = manifestCacheEnabled && manifestCache != null;
        this.catalogName = context != null ? context.getCatalogName() : "default";
    }

    @Override
    public ConnectorScanRangeType getScanRangeType() {
        return ConnectorScanRangeType.FILE_SCAN;
    }

    @Override
    public List<ConnectorScanRange> planScan(ConnectorScanRequest req) {
        Objects.requireNonNull(req, "req");
        IcebergTableHandle iceHandle =
                (IcebergTableHandle) Objects.requireNonNull(req.getTable(), "handle");
        Long effectiveSnapshotId = resolveEffectiveSnapshotId(req, iceHandle);
        return doPlanScan(req.getSession(), iceHandle, req.getColumns(),
                req.getFilter(), effectiveSnapshotId);
    }

    /**
     * Picks the effective snapshot id to read: an
     * {@link IcebergConnectorMvccSnapshot} on
     * {@link ConnectorScanRequest#getMvccSnapshot()} takes priority, then a
     * {@link ConnectorTableVersion.BySnapshotId} on
     * {@link ConnectorScanRequest#getVersion()}, finally the snapshot id
     * already pinned on the handle (which is the primary resolution path
     * — set by {@link
     * org.apache.doris.connector.iceberg.IcebergConnectorMetadata#getTableHandle(
     * org.apache.doris.connector.api.ConnectorSession, String, String,
     * java.util.Optional, java.util.Optional)}).
     */
    private static Long resolveEffectiveSnapshotId(ConnectorScanRequest req, IcebergTableHandle handle) {
        if (req.getMvccSnapshot().isPresent()) {
            ConnectorMvccSnapshot mvcc = req.getMvccSnapshot().get();
            if (mvcc instanceof IcebergConnectorMvccSnapshot) {
                return ((IcebergConnectorMvccSnapshot) mvcc).snapshotId();
            }
        }
        if (req.getVersion().isPresent()) {
            ConnectorTableVersion v = req.getVersion().get();
            if (v instanceof ConnectorTableVersion.BySnapshotId) {
                return ((ConnectorTableVersion.BySnapshotId) v).snapshotId();
            }
            // Other version kinds are resolved at getTableHandle time and pinned
            // onto the handle itself; the scan path only honours the cheap
            // BySnapshotId case to stay engine-side-deterministic.
        }
        return handle.getSnapshotId();
    }

    private List<ConnectorScanRange> doPlanScan(
            ConnectorSession session,
            IcebergTableHandle iceHandle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter,
            Long pinnedSnapshotId) {
        Table table = tableLoader.apply(iceHandle.getDbName(), iceHandle.getTableName());
        Objects.requireNonNull(table, "iceberg table not loaded");

        if (table.currentSnapshot() == null && pinnedSnapshotId == null) {
            // Unsnapshotted table: nothing to scan. Mirrors legacy fast-path
            // where TableScan over a snapshot-less table yields zero files.
            return Collections.emptyList();
        }

        TableScan scan = table.newScan();
        if (pinnedSnapshotId != null) {
            scan = scan.useSnapshot(pinnedSnapshotId);
        }

        if (filter.isPresent()) {
            Expression expr = IcebergPredicateConverter.convert(filter.get(), table.schema());
            if (!(expr instanceof True)) {
                scan = scan.filter(expr);
            }
        }

        if (columns != null && !columns.isEmpty()) {
            List<String> names = new ArrayList<>(columns.size());
            for (ConnectorColumnHandle ch : columns) {
                names.add(((IcebergColumnHandle) ch).getName());
            }
            scan = scan.select(names);
        }

        int formatVersion = resolveFormatVersion(iceHandle, table);
        String tz = resolveTimeZone(session);

        List<ConnectorScanRange> ranges = new ArrayList<>();
        // Note: only `tasks` is owned by the try-with-resources because
        // TableScanUtil.splitFiles returns a CloseableIterable that
        // already cascades close() to its source iterable; double-closing
        // raw planFiles() iterables (some iceberg backends instrument
        // them with single-shot Timers) raises "stop() called multiple
        // times".
        CloseableIterable<FileScanTask> rawTasks = planFileScanTask(scan, table);
        try (CloseableIterable<FileScanTask> tasks = TableScanUtil.splitFiles(
                rawTasks, scan.targetSplitSize())) {
            for (FileScanTask task : tasks) {
                ranges.add(buildRange(task, formatVersion, tz));
            }
        } catch (IOException e) {
            throw new DorisConnectorException("Failed to plan iceberg scan for "
                    + iceHandle.getDbName() + "." + iceHandle.getTableName(), e);
        }

        LOG.info("Iceberg scan plan: table={}.{}, snapshot={}, formatVersion={}, splits={}",
                iceHandle.getDbName(), iceHandle.getTableName(),
                pinnedSnapshotId != null ? pinnedSnapshotId : effectiveSnapshotId(iceHandle, table),
                formatVersion, ranges.size());
        return ranges;
    }

    @Override
    public Map<String, String> getScanNodeProperties(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) Objects.requireNonNull(handle, "handle");
        Table table = tableLoader.apply(iceHandle.getDbName(), iceHandle.getTableName());
        Objects.requireNonNull(table, "iceberg table not loaded");

        Map<String, String> props = new HashMap<>();
        String defaultFormat = table.properties().getOrDefault(
                TableProperties.DEFAULT_FILE_FORMAT, "parquet");
        props.put(PROP_FILE_FORMAT_TYPE, defaultFormat.toUpperCase());
        props.put(PROP_ICEBERG_FORMAT_VERSION,
                String.valueOf(resolveFormatVersion(iceHandle, table)));

        // Vended credentials: merge the catalog-level storage props with any
        // per-table credentials iceberg attached to table.io() (REST catalog
        // vending), and surface them under a stable prefix that
        // populateScanLevelParams strips when copying into Thrift.
        Map<String, String> mergedStorage = IcebergVendedCredentialsUtil
                .mergeStorageProperties(table, catalogProperties);
        props.putAll(IcebergVendedCredentialsUtil.withPrefix(mergedStorage, PROP_LOCATION_PREFIX));

        // Schema name mapping (id-less parquet/orc reads): only set when the
        // table actually defines it, so that the Thrift map stays compact.
        String nameMapping = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
        if (nameMapping != null && !nameMapping.isEmpty()) {
            props.put(PROP_NAME_MAPPING, nameMapping);
        }

        Long snapshotId = effectiveSnapshotId(iceHandle, table);
        if (snapshotId != null) {
            props.put(PROP_SNAPSHOT_ID, snapshotId.toString());
        }
        if (iceHandle.getRefSpec() != null) {
            props.put(PROP_REF_SPEC, iceHandle.getRefSpec());
        }
        return props;
    }

    /**
     * Pushes the iceberg-specific scan-node properties into the Thrift
     * scan-range params consumed by BE:
     * <ul>
     *   <li>Strips {@link #PROP_LOCATION_PREFIX} from each storage /
     *       vended-credential key so BE sees raw {@code s3.*} /
     *       {@code oss.*} / {@code gs.*} / {@code adlfs.*} keys.</li>
     *   <li>Copies {@link #PROP_NAME_MAPPING} verbatim — BE consumes this
     *       key directly when applying iceberg name-mapping during
     *       parquet/orc reads.</li>
     * </ul>
     */
    @Override
    public void populateScanLevelParams(TFileScanRangeParams params,
                                        Map<String, String> nodeProperties) {
        Objects.requireNonNull(params, "params");
        if (nodeProperties == null || nodeProperties.isEmpty()) {
            return;
        }
        if (params.getProperties() == null) {
            params.setProperties(new HashMap<>());
        }
        Map<String, String> out = params.getProperties();
        for (Map.Entry<String, String> e : nodeProperties.entrySet()) {
            String key = e.getKey();
            if (key.startsWith(PROP_LOCATION_PREFIX)) {
                out.put(key.substring(PROP_LOCATION_PREFIX.length()), e.getValue());
            }
        }
        String nameMapping = nodeProperties.get(PROP_NAME_MAPPING);
        if (nameMapping != null) {
            out.put(PROP_NAME_MAPPING, nameMapping);
        }
    }

    /**
     * Metadata-only {@code COUNT(*)} pushdown derived from the iceberg
     * snapshot summary. Mirrors the legacy fe-core
     * {@code IcebergScanNode#getCountFromSnapshot} algorithm exactly:
     * <ol>
     *   <li>Resolve the snapshot to use ({@code handle.snapshotId} when
     *       set, otherwise {@code table.currentSnapshot()}).</li>
     *   <li>No snapshot → empty table → {@code Optional.of(0)}.</li>
     *   <li>{@code total-equality-deletes != 0} → cannot push,
     *       {@code Optional.empty()}.</li>
     *   <li>{@code total-position-deletes == 0} → exact count is
     *       {@code total-records}.</li>
     *   <li>Position deletes present and the session enables
     *       {@link #SESSION_VAR_IGNORE_DANGLING_DELETE} → {@code total-records
     *       - total-position-deletes}; otherwise cannot push.</li>
     * </ol>
     *
     * <p>The {@code filter} argument is only honoured to the extent that
     * a non-trivial filter forces no-pushdown (any filter could exclude
     * rows that the snapshot summary cannot subtract). Iceberg {@code True}
     * filters are treated as no-op, mirroring legacy behaviour where the
     * scan node only pushes count when the WHERE clause is fully empty.</p>
     */
    @Override
    public Optional<Long> getCountPushdownResult(ConnectorSession session,
                                                 ConnectorTableHandle handle,
                                                 Optional<ConnectorExpression> filter) {
        if (filter.isPresent()) {
            // Any residual filter requires per-row evaluation; no metadata-only count.
            return Optional.empty();
        }
        IcebergTableHandle iceHandle = (IcebergTableHandle) Objects.requireNonNull(handle, "handle");
        Table table = tableLoader.apply(iceHandle.getDbName(), iceHandle.getTableName());
        Objects.requireNonNull(table, "iceberg table not loaded");

        Snapshot snapshot = iceHandle.getSnapshotId() != null
                ? table.snapshot(iceHandle.getSnapshotId())
                : table.currentSnapshot();
        if (snapshot == null) {
            return Optional.of(0L);
        }

        Map<String, String> summary = snapshot.summary();
        String eqDeletes = summary != null ? summary.get(ICEBERG_TOTAL_EQUALITY_DELETES) : null;
        if (eqDeletes != null && !"0".equals(eqDeletes)) {
            return Optional.empty();
        }

        String totalRecords = summary != null ? summary.get(ICEBERG_TOTAL_RECORDS) : null;
        if (totalRecords == null) {
            return Optional.empty();
        }

        String posDeletesStr = summary.get(ICEBERG_TOTAL_POSITION_DELETES);
        long posDeletes = posDeletesStr != null ? Long.parseLong(posDeletesStr) : 0L;
        if (posDeletes == 0L) {
            return Optional.of(Long.parseLong(totalRecords));
        }

        if (ignoreDanglingDelete(session)) {
            return Optional.of(Long.parseLong(totalRecords) - posDeletes);
        }
        return Optional.empty();
    }

    @Override
    public void appendExplainInfo(StringBuilder output, String prefix,
                                  Map<String, String> nodeProperties) {
        String fv = nodeProperties.get(PROP_ICEBERG_FORMAT_VERSION);
        String fmt = nodeProperties.get(PROP_FILE_FORMAT_TYPE);
        String snap = nodeProperties.get(PROP_SNAPSHOT_ID);
        String ref = nodeProperties.get(PROP_REF_SPEC);
        if (fv != null) {
            output.append(prefix).append("iceberg.format_version=").append(fv).append('\n');
        }
        if (fmt != null) {
            output.append(prefix).append("file_format_type=").append(fmt).append('\n');
        }
        if (snap != null) {
            output.append(prefix).append("iceberg snapshot=").append(snap).append('\n');
        }
        if (ref != null) {
            output.append(prefix).append("iceberg ref=").append(ref).append('\n');
        }
        if (manifestCacheEnabled) {
            CacheStats stats = getCacheStats();
            output.append(prefix).append("manifest cache: hits=").append(stats.hits)
                    .append(", misses=").append(stats.misses)
                    .append(", failures=").append(stats.failures).append('\n');
        }
    }

    /** Snapshot of the per-provider cumulative manifest-cache counters.
     *  Counters accumulate across {@link #planScan} calls (per-query
     *  isolation is intentionally <i>not</i> done here — the legacy
     *  {@code recordManifestCacheProfile} SummaryProfile path that
     *  flushed counters per query lives in fe-core and cannot be
     *  reproduced from the plugin layer). */
    public CacheStats getCacheStats() {
        return new CacheStats(
                manifestCacheHits.sum(),
                manifestCacheMisses.sum(),
                manifestCacheFailures.sum());
    }

    /** Visible for tests: the cache passed at construction time, or
     *  {@code null} when disabled. */
    public IcebergPluginManifestCache getManifestCache() {
        return manifestCache;
    }

    /** Visible for tests: whether the manifest-cache code path is active. */
    public boolean isManifestCacheEnabled() {
        return manifestCacheEnabled;
    }

    /** Hit/miss/failure counters for the plugin manifest cache. */
    public static final class CacheStats {
        public final long hits;
        public final long misses;
        public final long failures;

        CacheStats(long hits, long misses, long failures) {
            this.hits = hits;
            this.misses = misses;
            this.failures = failures;
        }
    }

    // -------- helpers --------

    /** Dispatch between the legacy {@code scan.planFiles()} path and the
     *  manifest-cache-aware path. Mirrors the legacy
     *  {@code IcebergScanNode#planFileScanTask} fallback semantics:
     *  on any cache error increment {@link #manifestCacheFailures} and
     *  fall back to the uncached scan. */
    private CloseableIterable<FileScanTask> planFileScanTask(TableScan scan, Table icebergTable) {
        if (!manifestCacheEnabled) {
            return scan.planFiles();
        }
        try {
            return planFileScanTaskWithManifestCache(scan, icebergTable);
        } catch (Exception e) {
            manifestCacheFailures.increment();
            LOG.warn("Plan with manifest cache failed, fallback to scan.planFiles(): {}",
                    e.getMessage(), e);
            return scan.planFiles();
        }
    }

    /**
     * Manifest-cache-aware port of the legacy fe-core
     * {@code IcebergScanNode#planFileScanTaskWithManifestCache}
     * (lines 630-749). The algorithm is preserved verbatim:
     *
     * <ol>
     *   <li>Resolve the scan's snapshot; empty when null.</li>
     *   <li>Build per-spec {@link ResidualEvaluator}s and a single
     *       {@link InclusiveMetricsEvaluator} from the scan's residual
     *       filter (defaulting to {@code alwaysTrue} when the scan
     *       carries no filter — the plugin uses
     *       {@link Expressions#alwaysTrue()} because the residual
     *       filter is applied via {@link TableScan#filter} above and
     *       iceberg may have rewritten it).</li>
     *   <li>Phase 1: walk the snapshot's delete manifests, prune by
     *       {@link ManifestEvaluator}, then load delete files via
     *       {@link IcebergPluginManifestCache#getOrLoadDeleteFiles}.
     *       Build a {@link DeleteFileIndex} for sequence-aware lookup.</li>
     *   <li>Phase 2: walk the snapshot's matching data manifests, load
     *       data files via the cache, prune each by metrics + residual,
     *       and emit a {@link BaseFileScanTask} carrying the matched
     *       deletes.</li>
     * </ol>
     *
     * <p>Note: the legacy implementation depended on fe-core
     * {@code ExternalTable} for the cache key; the plugin uses a
     * {@code (catalogName, manifestPath, content)} key instead — the
     * iceberg table object is still passed through to the cache for
     * loading the manifest contents on miss.</p>
     */
    private CloseableIterable<FileScanTask> planFileScanTaskWithManifestCache(
            TableScan scan, Table icebergTable) throws IOException {
        Snapshot snapshot = scan.snapshot();
        if (snapshot == null) {
            return CloseableIterable.withNoopClose(Collections.emptyList());
        }

        Expression filterExpr = scan.filter() != null ? scan.filter() : Expressions.alwaysTrue();
        Map<Integer, PartitionSpec> specsById = icebergTable.specs();
        boolean caseSensitive = true;

        Map<Integer, ResidualEvaluator> residualEvaluators = new HashMap<>();
        specsById.forEach((id, spec) -> residualEvaluators.put(id,
                ResidualEvaluator.of(spec, filterExpr, caseSensitive)));

        InclusiveMetricsEvaluator metricsEvaluator =
                new InclusiveMetricsEvaluator(icebergTable.schema(), filterExpr, caseSensitive);

        // ========== Phase 1: delete manifests ==========
        List<DeleteFile> deleteFiles = new ArrayList<>();
        List<ManifestFile> deleteManifests = snapshot.deleteManifests(icebergTable.io());
        for (ManifestFile manifest : deleteManifests) {
            if (manifest.content() != ManifestContent.DELETES) {
                continue;
            }
            PartitionSpec spec = specsById.get(manifest.partitionSpecId());
            if (spec == null) {
                continue;
            }
            ManifestEvaluator evaluator =
                    ManifestEvaluator.forPartitionFilter(filterExpr, spec, caseSensitive);
            if (!evaluator.eval(manifest)) {
                continue;
            }
            ManifestCacheValue value = manifestCache.getOrLoadDeleteFiles(
                    ManifestCacheKey.of(catalogName, manifest),
                    manifest, icebergTable, this::recordCacheAccess);
            deleteFiles.addAll(value.getDeleteFiles());
        }

        IcebergPluginInternals.DeleteFileLookup deleteIndex =
                IcebergPluginInternals.buildDeleteFileLookup(deleteFiles, specsById, caseSensitive);

        // ========== Phase 2: data manifests ==========
        List<FileScanTask> tasks = new ArrayList<>();
        try (CloseableIterable<ManifestFile> dataManifests =
                     IcebergPluginInternals.matchingDataManifests(
                             snapshot.dataManifests(icebergTable.io()),
                             specsById, filterExpr)) {
            for (ManifestFile manifest : dataManifests) {
                if (manifest.content() != ManifestContent.DATA) {
                    continue;
                }
                PartitionSpec spec = specsById.get(manifest.partitionSpecId());
                if (spec == null) {
                    continue;
                }
                ResidualEvaluator residualEvaluator = residualEvaluators.get(manifest.partitionSpecId());
                if (residualEvaluator == null) {
                    continue;
                }

                ManifestCacheValue value = manifestCache.getOrLoadDataFiles(
                        ManifestCacheKey.of(catalogName, manifest),
                        manifest, icebergTable, this::recordCacheAccess);

                for (DataFile dataFile : value.getDataFiles()) {
                    if (!metricsEvaluator.eval(dataFile)) {
                        continue;
                    }
                    if (residualEvaluator.residualFor(dataFile.partition())
                            .equals(Expressions.alwaysFalse())) {
                        continue;
                    }
                    List<DeleteFile> deletes = deleteIndex.forDataFile(
                            dataFile.dataSequenceNumber(), dataFile);
                    tasks.add(new BaseFileScanTask(
                            dataFile,
                            deletes.toArray(new DeleteFile[0]),
                            SchemaParser.toJson(icebergTable.schema()),
                            PartitionSpecParser.toJson(spec),
                            residualEvaluator));
                }
            }
        }

        return CloseableIterable.withNoopClose(tasks);
    }

    private void recordCacheAccess(boolean cacheHit) {
        if (cacheHit) {
            manifestCacheHits.increment();
        } else {
            manifestCacheMisses.increment();
        }
    }

    private IcebergScanRange buildRange(FileScanTask task, int formatVersion, String tz) {
        DataFile dataFile = task.file();
        String path = dataFile.path().toString();

        IcebergScanRange.Builder b = IcebergScanRange.builder()
                .path(path)
                .start(task.start())
                .length(task.length())
                .fileSize(dataFile.fileSizeInBytes())
                .formatVersion(formatVersion)
                .partitionSpecId(dataFile.specId())
                .originalFilePath(path)
                .tableLevelRowCount(-1L);

        // partition data + identity values (best-effort; null on non-identity / unsupported).
        PartitionSpec spec = task.spec();
        if (spec != null && spec.isPartitioned() && dataFile.partition() != null) {
            try {
                b.partitionDataJson(IcebergPartitionDataUtil.getPartitionDataJson(
                        dataFile.partition(), spec, tz));
            } catch (RuntimeException e) {
                LOG.warn("Failed to serialize iceberg partitionDataJson for {}: {}", path, e.toString());
            }
            try {
                Map<String, String> ids = IcebergPartitionDataUtil.buildIdentityPartitionValues(
                        dataFile.partition(), spec, tz);
                if (ids != null && !ids.isEmpty()) {
                    b.partitionValues(ids);
                }
            } catch (RuntimeException e) {
                LOG.warn("Failed to derive iceberg partitionValues for {}: {}", path, e.toString());
            }
        }

        if (formatVersion >= MIN_ROW_LINEAGE_VERSION) {
            // -1 means upgraded-from-v2; the column is NULL for those rows.
            Long firstRowId = dataFile.firstRowId();
            Long fileSeq = dataFile.fileSequenceNumber();
            b.firstRowId(firstRowId != null ? firstRowId : -1L);
            b.lastUpdatedSequenceNumber(
                    fileSeq != null && firstRowId != null ? fileSeq : -1L);
        }

        if (formatVersion >= MIN_DELETE_FILE_VERSION) {
            List<DeleteFile> deletes = task.deletes();
            if (deletes != null && !deletes.isEmpty()) {
                List<IcebergDeleteFileDescriptor> descriptors = new ArrayList<>(deletes.size());
                for (DeleteFile df : deletes) {
                    descriptors.add(toDescriptor(df));
                }
                b.deleteFiles(descriptors);
            }
        }

        return b.build();
    }

    private static IcebergDeleteFileDescriptor toDescriptor(DeleteFile df) {
        FileContent content = df.content();
        FileFormat format = df.format();
        String path = df.path().toString();

        if (content == FileContent.POSITION_DELETES) {
            int posFieldId = MetadataColumns.DELETE_FILE_POS.fieldId();
            Type posType = MetadataColumns.DELETE_FILE_POS.type();
            Long lower = decodeLong(df.lowerBounds(), posFieldId, posType);
            Long upper = decodeLong(df.upperBounds(), posFieldId, posType);

            if (format == FileFormat.PUFFIN) {
                return IcebergDeleteFileDescriptor.builder()
                        .kind(IcebergDeleteFileDescriptor.Kind.DELETION_VECTOR)
                        .path(path)
                        .fileFormat(format)
                        .positionLowerBound(lower)
                        .positionUpperBound(upper)
                        .contentOffset(df.contentOffset() != null ? df.contentOffset() : -1L)
                        .contentSizeInBytes(df.contentSizeInBytes() != null ? df.contentSizeInBytes() : -1L)
                        .build();
            }
            return IcebergDeleteFileDescriptor.builder()
                    .kind(IcebergDeleteFileDescriptor.Kind.POSITION_DELETE)
                    .path(path)
                    .fileFormat(format)
                    .positionLowerBound(lower)
                    .positionUpperBound(upper)
                    .build();
        }

        if (content == FileContent.EQUALITY_DELETES) {
            List<Integer> fieldIdList = df.equalityFieldIds();
            if (fieldIdList == null || fieldIdList.isEmpty()) {
                throw new IllegalStateException(
                        "Iceberg equality delete file has no equalityFieldIds: " + path);
            }
            int[] ids = new int[fieldIdList.size()];
            for (int i = 0; i < fieldIdList.size(); i++) {
                ids[i] = fieldIdList.get(i);
            }
            return IcebergDeleteFileDescriptor.builder()
                    .kind(IcebergDeleteFileDescriptor.Kind.EQUALITY_DELETE)
                    .path(path)
                    .fileFormat(format)
                    .fieldIds(ids)
                    .build();
        }

        throw new IllegalStateException("Unknown iceberg delete content: " + content);
    }

    private static Long decodeLong(Map<Integer, ByteBuffer> bounds, int fieldId, Type type) {
        if (bounds == null) {
            return null;
        }
        ByteBuffer bytes = bounds.get(fieldId);
        if (bytes == null) {
            return null;
        }
        return Conversions.fromByteBuffer(type, bytes);
    }

    private static int resolveFormatVersion(IcebergTableHandle handle, Table table) {
        if (handle.getFormatVersion() != null) {
            return handle.getFormatVersion();
        }
        // Fall back to the table's spec for unresolved handles. Default v2 when unknown.
        return 2;
    }

    private static Long effectiveSnapshotId(IcebergTableHandle handle, Table table) {
        if (handle.getSnapshotId() != null) {
            return handle.getSnapshotId();
        }
        return table.currentSnapshot() != null ? table.currentSnapshot().snapshotId() : null;
    }

    private String resolveTimeZone(ConnectorSession session) {
        String tz = session != null ? session.getProperty("time_zone", String.class) : null;
        return tz == null || tz.isEmpty() ? defaultTimeZone : tz;
    }

    private static boolean ignoreDanglingDelete(ConnectorSession session) {
        if (session == null) {
            return false;
        }
        Map<String, String> sessionProps = session.getSessionProperties();
        if (sessionProps == null) {
            return false;
        }
        String raw = sessionProps.get(SESSION_VAR_IGNORE_DANGLING_DELETE);
        return "true".equalsIgnoreCase(raw);
    }
}
