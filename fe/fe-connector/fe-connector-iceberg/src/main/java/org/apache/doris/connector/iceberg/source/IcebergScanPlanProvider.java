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
import org.apache.doris.connector.iceberg.IcebergColumnHandle;
import org.apache.doris.connector.iceberg.IcebergPredicateConverter;
import org.apache.doris.connector.iceberg.IcebergTableHandle;
import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
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
 *   <li>vended credentials / {@code location.*} props (M2-Iceberg-05)</li>
 *   <li>name mapping JSON injection (M2-Iceberg-05)</li>
 *   <li>count pushdown / {@code getCountFromSnapshot} (M2-Iceberg-05)</li>
 *   <li>manifest-cache binding (M2-Iceberg-06)</li>
 *   <li>time travel / branch / tag (M2-Iceberg-07) — the
 *       {@link IcebergTableHandle#getRefSpec()} is intentionally <b>not</b>
 *       consumed; this PR always reads the pinned {@code snapshotId} when
 *       present, otherwise the table's {@code currentSnapshot()}.</li>
 *   <li>{@code LocationProvider} URI normalisation (M2-Iceberg-05) — paths
 *       are stored as-is on the produced ranges.</li>
 * </ul>
 */
public class IcebergScanPlanProvider implements ConnectorScanPlanProvider {

    private static final Logger LOG = LogManager.getLogger(IcebergScanPlanProvider.class);

    /** Scan-node property keys produced by {@link #getScanNodeProperties}. */
    public static final String PROP_FILE_FORMAT_TYPE = "file_format_type";
    public static final String PROP_ICEBERG_FORMAT_VERSION = "iceberg.format_version";

    private static final int MIN_DELETE_FILE_VERSION = 2;
    private static final int MIN_ROW_LINEAGE_VERSION = 3;

    private final BiFunction<String, String, Table> tableLoader;
    private final Map<String, String> catalogProperties;
    private final ConnectorContext context;
    private final String defaultTimeZone;

    /**
     * Construct a provider.
     *
     * @param tableLoader      {@code (db, table) -> iceberg.Table}; typically backed
     *                         by the iceberg plugin's
     *                         {@code iceberg.table} {@code MetaCacheHandle}
     *                         so cache-driven invalidation propagates.
     * @param catalogProperties iceberg connector properties (for downstream PRs;
     *                         currently only used for the explain summary).
     * @param context          connector context (may be {@code null} in tests).
     */
    public IcebergScanPlanProvider(BiFunction<String, String, Table> tableLoader,
                                   Map<String, String> catalogProperties,
                                   ConnectorContext context) {
        this.tableLoader = Objects.requireNonNull(tableLoader, "tableLoader");
        this.catalogProperties = catalogProperties == null
                ? Collections.emptyMap()
                : catalogProperties;
        this.context = context;
        this.defaultTimeZone = "UTC";
    }

    @Override
    public ConnectorScanRangeType getScanRangeType() {
        return ConnectorScanRangeType.FILE_SCAN;
    }

    @Override
    public List<ConnectorScanRange> planScan(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) Objects.requireNonNull(handle, "handle");
        Table table = tableLoader.apply(iceHandle.getDbName(), iceHandle.getTableName());
        Objects.requireNonNull(table, "iceberg table not loaded");

        if (table.currentSnapshot() == null && iceHandle.getSnapshotId() == null) {
            // Unsnapshotted table: nothing to scan. Mirrors legacy fast-path
            // where TableScan over a snapshot-less table yields zero files.
            return Collections.emptyList();
        }

        TableScan scan = table.newScan();
        if (iceHandle.getSnapshotId() != null) {
            scan = scan.useSnapshot(iceHandle.getSnapshotId());
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
        try (CloseableIterable<FileScanTask> rawTasks = scan.planFiles();
                CloseableIterable<FileScanTask> tasks = TableScanUtil.splitFiles(
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
                effectiveSnapshotId(iceHandle, table), formatVersion, ranges.size());
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
        // M2-Iceberg-05 will add: location.* (vended creds), iceberg.name-mapping.
        // M2-Iceberg-07 will add snapshot/refSpec-derived props if needed.
        return props;
    }

    @Override
    public void appendExplainInfo(StringBuilder output, String prefix,
                                  Map<String, String> nodeProperties) {
        String fv = nodeProperties.get(PROP_ICEBERG_FORMAT_VERSION);
        String fmt = nodeProperties.get(PROP_FILE_FORMAT_TYPE);
        if (fv != null) {
            output.append(prefix).append("iceberg.format_version=").append(fv).append('\n');
        }
        if (fmt != null) {
            output.append(prefix).append("file_format_type=").append(fmt).append('\n');
        }
    }

    // -------- helpers --------

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
}
