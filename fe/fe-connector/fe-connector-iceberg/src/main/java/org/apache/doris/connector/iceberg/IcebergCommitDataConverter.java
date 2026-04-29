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

import org.apache.doris.thrift.TFileContent;
import org.apache.doris.thrift.TIcebergColumnStats;
import org.apache.doris.thrift.TIcebergCommitData;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Plugin-side conversion utilities used by the Iceberg WriteOps path.
 *
 * <p>Bridges the BE → FE write protocol (thrift-serialized
 * {@link TIcebergCommitData} fragments) into iceberg {@link DataFile}
 * objects ready for an {@code AppendFiles} / {@code OverwriteFiles} /
 * {@code ReplacePartitions} commit.</p>
 *
 * <p>This class is intentionally a plugin-internal mirror of fe-core's
 * {@code IcebergWriterHelper} so the plugin module remains free of any
 * fe-core dependency (constraint enforced by the connector SPI module
 * boundary). Once M3-15 cuts the production INSERT path over to the SPI,
 * the legacy helper will be retired.</p>
 */
final class IcebergCommitDataConverter {

    private IcebergCommitDataConverter() {
    }

    /**
     * Resolves the table-default file format from iceberg table properties.
     * Falls back to {@code parquet} when unset.
     */
    static FileFormat resolveFileFormat(Table table) {
        String format = table.properties()
                .getOrDefault(TableProperties.DEFAULT_FILE_FORMAT, "parquet");
        return FileFormat.valueOf(format.toUpperCase());
    }

    /**
     * Decodes a collection of thrift-serialized {@link TIcebergCommitData}
     * fragments produced by BE writers into iceberg {@link DataFile}s ready
     * for commit. Each fragment is expected to be a single
     * {@code TIcebergCommitData} encoded with {@link TBinaryProtocol}.
     *
     * <p>This convenience entry point is preserved for callers that only
     * deal with append/overwrite paths and is equivalent to
     * {@link #decodeFragments(Table, Collection)}{@code .dataFiles()},
     * additionally asserting that no row-level delete fragments are
     * present (which would be silently dropped otherwise).</p>
     */
    static List<DataFile> decodeDataFiles(Table table, Collection<byte[]> fragments) {
        DecodedCommitFiles decoded = decodeFragments(table, fragments);
        if (!decoded.deleteFiles().isEmpty()) {
            throw new IllegalStateException(
                    "Iceberg commit produced " + decoded.deleteFiles().size()
                            + " delete file(s) but caller only requested data files; "
                            + "use decodeFragments() to handle row-level deletes");
        }
        return decoded.dataFiles();
    }

    /**
     * Decodes a collection of thrift-serialized {@link TIcebergCommitData}
     * fragments into a partitioned view of {@link DataFile}s and
     * {@link DeleteFile}s, dispatched on
     * {@link TIcebergCommitData#getFileContent()}.
     *
     * <p>Fragments with {@code file_content == null} are treated as
     * {@code DATA} (legacy BE behaviour). Position deletes, equality
     * deletes and deletion vectors are routed into delete files using
     * the per-fragment {@code partition_spec_id} when set, falling back
     * to {@link Table#spec()} otherwise.</p>
     */
    static DecodedCommitFiles decodeFragments(Table table, Collection<byte[]> fragments) {
        if (fragments == null || fragments.isEmpty()) {
            return new DecodedCommitFiles(new ArrayList<>(0), new ArrayList<>(0));
        }
        TDeserializer deserializer;
        try {
            deserializer = new TDeserializer(new TBinaryProtocol.Factory());
        } catch (TException e) {
            throw new IllegalStateException("Failed to create thrift deserializer", e);
        }

        FileFormat defaultFormat = resolveFileFormat(table);
        PartitionSpec defaultSpec = table.spec();

        List<DataFile> dataFiles = new ArrayList<>();
        List<DeleteFile> deleteFiles = new ArrayList<>();
        for (byte[] payload : fragments) {
            if (payload == null || payload.length == 0) {
                continue;
            }
            TIcebergCommitData commit = new TIcebergCommitData();
            try {
                deserializer.deserialize(commit, payload);
            } catch (TException e) {
                throw new IllegalStateException(
                        "Failed to deserialize TIcebergCommitData fragment", e);
            }
            TFileContent content = commit.isSetFileContent() ? commit.getFileContent() : TFileContent.DATA;
            if (content == TFileContent.DATA) {
                dataFiles.add(toDataFile(commit, defaultFormat, defaultSpec, table));
            } else {
                PartitionSpec spec = resolveSpec(table, commit, defaultSpec);
                deleteFiles.add(toDeleteFile(commit, defaultFormat, spec, content));
            }
        }
        return new DecodedCommitFiles(dataFiles, deleteFiles);
    }

    private static PartitionSpec resolveSpec(Table table, TIcebergCommitData commit, PartitionSpec defaultSpec) {
        if (!commit.isSetPartitionSpecId()) {
            return defaultSpec;
        }
        PartitionSpec spec = table.specs().get(commit.getPartitionSpecId());
        if (spec == null) {
            throw new IllegalStateException(
                    "Unknown partition spec id " + commit.getPartitionSpecId() + " for iceberg table " + table.name());
        }
        return spec;
    }

    private static DeleteFile toDeleteFile(
            TIcebergCommitData commit,
            FileFormat defaultFormat,
            PartitionSpec spec,
            TFileContent content) {
        boolean isDeletionVector = content == TFileContent.DELETION_VECTOR
                || (commit.isSetContentOffset() && commit.isSetContentSizeInBytes());
        FileFormat effectiveFormat = isDeletionVector ? FileFormat.PUFFIN : defaultFormat;
        FileMetadata.Builder builder = FileMetadata.deleteFileBuilder(spec)
                .withPath(commit.getFilePath())
                .withFormat(effectiveFormat)
                .withFileSizeInBytes(commit.getFileSize())
                .withRecordCount(commit.getRowCount());

        switch (content) {
            case POSITION_DELETES:
            case DELETION_VECTOR:
                builder.ofPositionDeletes();
                break;
            case EQUALITY_DELETES:
                List<Integer> equalityFieldIds = commit.getEqualityFieldIds();
                if (equalityFieldIds == null || equalityFieldIds.isEmpty()) {
                    throw new IllegalStateException(
                            "Iceberg EQUALITY_DELETES commit data must carry equality_field_ids, file="
                                    + commit.getFilePath());
                }
                int[] ids = new int[equalityFieldIds.size()];
                for (int i = 0; i < ids.length; i++) {
                    ids[i] = equalityFieldIds.get(i);
                }
                builder.ofEqualityDeletes(ids);
                break;
            default:
                throw new IllegalStateException(
                        "Unsupported iceberg delete file content: " + content);
        }

        if (isDeletionVector) {
            if (!commit.isSetContentOffset() || !commit.isSetContentSizeInBytes()) {
                throw new IllegalStateException(
                        "Iceberg DELETION_VECTOR commit data must carry content_offset and content_size_in_bytes, file="
                                + commit.getFilePath());
            }
            builder.withContentOffset(commit.getContentOffset());
            builder.withContentSizeInBytes(commit.getContentSizeInBytes());
        }

        if (commit.isSetReferencedDataFilePath()
                && commit.getReferencedDataFilePath() != null
                && !commit.getReferencedDataFilePath().isEmpty()) {
            builder.withReferencedDataFile(commit.getReferencedDataFilePath());
        }

        if (spec.isPartitioned()) {
            List<String> partitionValues = commit.getPartitionValues();
            if (partitionValues == null || partitionValues.isEmpty()) {
                throw new IllegalStateException(
                        "Partitioned iceberg delete file requires partition values, file=" + commit.getFilePath());
            }
            builder.withPartition(toPartitionData(partitionValues, spec));
        }

        return builder.build();
    }

    /**
     * Holder for the result of {@link #decodeFragments(Table, Collection)},
     * exposing the data files and delete files produced by a single BE-side
     * write task in the order BE emitted them.
     */
    static final class DecodedCommitFiles {
        private final List<DataFile> dataFiles;
        private final List<DeleteFile> deleteFiles;

        DecodedCommitFiles(List<DataFile> dataFiles, List<DeleteFile> deleteFiles) {
            this.dataFiles = dataFiles;
            this.deleteFiles = deleteFiles;
        }

        List<DataFile> dataFiles() {
            return dataFiles;
        }

        List<DeleteFile> deleteFiles() {
            return deleteFiles;
        }

        boolean hasDeletes() {
            return !deleteFiles.isEmpty();
        }

        boolean isEmpty() {
            return dataFiles.isEmpty() && deleteFiles.isEmpty();
        }
    }

    private static DataFile toDataFile(
            TIcebergCommitData commit,
            FileFormat format,
            PartitionSpec spec,
            Table table) {
        DataFiles.Builder builder = DataFiles.builder(spec)
                .withPath(commit.getFilePath())
                .withFileSizeInBytes(commit.getFileSize())
                .withRecordCount(commit.getRowCount())
                .withMetrics(buildMetrics(commit))
                .withSortOrder(table.sortOrder())
                .withFormat(format);
        if (spec.isPartitioned()) {
            List<String> partitionValues = commit.getPartitionValues();
            if (partitionValues == null || partitionValues.isEmpty()) {
                throw new IllegalStateException(
                        "Partitioned iceberg table requires partition values, file=" + commit.getFilePath());
            }
            builder.withPartition(toPartitionData(partitionValues, spec));
        }
        return builder.build();
    }

    private static PartitionData toPartitionData(List<String> humanReadable, PartitionSpec spec) {
        PartitionData data = new PartitionData(spec.partitionType());
        List<Types.NestedField> fields = spec.partitionType().fields();
        for (int i = 0; i < humanReadable.size(); i++) {
            String value = humanReadable.get(i);
            if (value == null || "null".equals(value)) {
                data.set(i, null);
                continue;
            }
            data.set(i, parsePartitionValue(value, fields.get(i).type()));
        }
        return data;
    }

    /**
     * Parses a Backend-supplied human-readable partition value into the
     * iceberg internal representation expected by {@link PartitionData}.
     *
     * <p>Supports the primitive set covered by Doris partition columns:
     * boolean/integer/long/float/double, string, date, timestamp(z), decimal.
     * Unsupported types raise {@link IllegalArgumentException} so the commit
     * path fails loudly rather than producing a silently-wrong manifest.</p>
     */
    static Object parsePartitionValue(String value, org.apache.iceberg.types.Type type) {
        switch (type.typeId()) {
            case STRING:
                return value;
            case BOOLEAN:
                return Boolean.parseBoolean(value);
            case INTEGER:
                return Integer.parseInt(value);
            case LONG:
                return Long.parseLong(value);
            case FLOAT:
                return Float.parseFloat(value);
            case DOUBLE:
                return Double.parseDouble(value);
            case DECIMAL:
                return new BigDecimal(value);
            case DATE:
                return (int) LocalDate.parse(value, DateTimeFormatter.ISO_LOCAL_DATE).toEpochDay();
            case TIMESTAMP:
                return parseTimestampMicros(value);
            default:
                throw new IllegalArgumentException(
                        "Unsupported iceberg partition value type: " + type);
        }
    }

    private static long parseTimestampMicros(String value) {
        String normalized = value.contains("T") ? value : value.replace(' ', 'T');
        LocalDateTime ldt = LocalDateTime.parse(normalized);
        long seconds = ldt.toEpochSecond(ZoneOffset.UTC);
        int nanos = ldt.getNano();
        return seconds * 1_000_000L + nanos / 1_000L;
    }

    private static Metrics buildMetrics(TIcebergCommitData commit) {
        Map<Integer, Long> columnSizes = new HashMap<>();
        Map<Integer, Long> valueCounts = new HashMap<>();
        Map<Integer, Long> nullValueCounts = new HashMap<>();
        Map<Integer, ByteBuffer> lowerBounds = new HashMap<>();
        Map<Integer, ByteBuffer> upperBounds = new HashMap<>();
        if (commit.isSetColumnStats()) {
            TIcebergColumnStats stats = commit.column_stats;
            if (stats.isSetColumnSizes()) {
                columnSizes = stats.column_sizes;
            }
            if (stats.isSetValueCounts()) {
                valueCounts = stats.value_counts;
            }
            if (stats.isSetNullValueCounts()) {
                nullValueCounts = stats.null_value_counts;
            }
            if (stats.isSetLowerBounds()) {
                lowerBounds = stats.lower_bounds;
            }
            if (stats.isSetUpperBounds()) {
                upperBounds = stats.upper_bounds;
            }
        }
        return new Metrics(commit.getRowCount(), columnSizes, valueCounts,
                nullValueCounts, null, lowerBounds, upperBounds);
    }
}
