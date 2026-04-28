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

import org.apache.doris.connector.api.scan.ConnectorDeleteFile;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.iceberg.FileFormat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Plugin-side iceberg scan range. Mirrors the legacy {@code IcebergSplit}
 * data shape and produces {@code TIcebergFileDesc / TIcebergDeleteFileDesc}
 * with bit-level parity to the legacy
 * {@code IcebergScanNode#setIcebergParams} translation.
 *
 * <p>Limitations vs. legacy:</p>
 * <ul>
 *   <li>System-table splits are out of scope: {@link #populateRangeParams}
 *       throws on a systable instance. Systables go through their own
 *       {@code IcebergMetadataScanPlanProvider} chain.</li>
 *   <li>{@code deleteFilesByReferencedDataFile} bookkeeping (delete-file
 *       dedup across data files) is not done here; that is fe-core
 *       scan-node state and will be reproduced inside the plugin scan
 *       provider in a follow-up PR (M2-Iceberg-04).</li>
 *   <li>Delete-file paths are taken as-is. Callers are responsible for
 *       resolving them against the plugin's own LocationProvider before
 *       passing them in (legacy path went through
 *       {@code LocationPath.toStorageLocation()}).</li>
 * </ul>
 */
public final class IcebergScanRange implements ConnectorScanRange {

    private static final long serialVersionUID = 1L;

    /** Iceberg format-version threshold at which delete files become legal. */
    public static final int MIN_DELETE_FILE_SUPPORT_VERSION = 2;

    private static final String TABLE_FORMAT_TYPE = "iceberg";
    private static final int FILE_CONTENT_DATA = 0;

    private final String path;
    private final long start;
    private final long length;
    private final long fileSize;

    private final int formatVersion;
    private final Integer partitionSpecId;
    private final String partitionDataJson;
    private final Map<String, String> partitionValues;
    private final String originalFilePath;

    private final List<IcebergDeleteFileDescriptor> deleteFiles;

    private final Long firstRowId;
    private final Long lastUpdatedSequenceNumber;

    private final long tableLevelRowCount;

    private final Map<String, String> properties;

    private IcebergScanRange(Builder b) {
        this.path = Objects.requireNonNull(b.path, "path is required");
        this.start = b.start;
        this.length = b.length;
        this.fileSize = b.fileSize;
        this.formatVersion = b.formatVersion;
        this.partitionSpecId = b.partitionSpecId;
        this.partitionDataJson = b.partitionDataJson;
        this.partitionValues = b.partitionValues == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(b.partitionValues));
        this.originalFilePath = Objects.requireNonNull(b.originalFilePath,
                "originalFilePath is required");
        this.deleteFiles = b.deleteFiles == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(b.deleteFiles));
        this.firstRowId = b.firstRowId;
        this.lastUpdatedSequenceNumber = b.lastUpdatedSequenceNumber;
        this.tableLevelRowCount = b.tableLevelRowCount;
        this.properties = b.properties == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new HashMap<>(b.properties));
        if (this.formatVersion >= 3 && (this.firstRowId == null
                || this.lastUpdatedSequenceNumber == null)) {
            throw new IllegalStateException(
                    "iceberg v3 scan range requires firstRowId and lastUpdatedSequenceNumber");
        }
    }

    @Override
    public ConnectorScanRangeType getRangeType() {
        return ConnectorScanRangeType.FILE_SCAN;
    }

    @Override
    public Optional<String> getPath() {
        return Optional.of(path);
    }

    @Override
    public long getStart() {
        return start;
    }

    @Override
    public long getLength() {
        return length;
    }

    @Override
    public long getFileSize() {
        return fileSize;
    }

    @Override
    public String getTableFormatType() {
        return TABLE_FORMAT_TYPE;
    }

    @Override
    public Map<String, String> getPartitionValues() {
        return partitionValues;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * The SPI-typed delete-file list. The iceberg plugin carries the
     * richer information through {@link #getIcebergDeleteFiles()}; this
     * generic view is intentionally empty so that engine-side generic
     * code does not double-count them.
     */
    @Override
    public List<ConnectorDeleteFile> getDeleteFiles() {
        return Collections.emptyList();
    }

    public int getFormatVersion() {
        return formatVersion;
    }

    public Integer getPartitionSpecId() {
        return partitionSpecId;
    }

    public String getPartitionDataJson() {
        return partitionDataJson;
    }

    public String getOriginalFilePath() {
        return originalFilePath;
    }

    public Long getFirstRowId() {
        return firstRowId;
    }

    public Long getLastUpdatedSequenceNumber() {
        return lastUpdatedSequenceNumber;
    }

    public long getTableLevelRowCount() {
        return tableLevelRowCount;
    }

    /** Plugin-typed delete file list (immutable). */
    public List<IcebergDeleteFileDescriptor> getIcebergDeleteFiles() {
        return deleteFiles;
    }

    /**
     * Populates iceberg-specific Thrift fields. Mirrors the legacy
     * {@code IcebergScanNode#setIcebergParams} field-for-field: the
     * caller (engine glue) is expected to have already set
     * {@code formatDesc.tableFormatType = "iceberg"} from
     * {@link #getTableFormatType()} and to call
     * {@code rangeDesc.setTableFormatParams(formatDesc)} after this
     * method returns.
     */
    @Override
    public void populateRangeParams(TTableFormatFileDesc formatDesc, TFileRangeDesc rangeDesc) {
        Objects.requireNonNull(formatDesc, "formatDesc");
        Objects.requireNonNull(rangeDesc, "rangeDesc");

        formatDesc.setTableLevelRowCount(tableLevelRowCount);

        TIcebergFileDesc fileDesc = new TIcebergFileDesc();
        fileDesc.setFormatVersion(formatVersion);
        fileDesc.setOriginalFilePath(originalFilePath);
        if (partitionSpecId != null) {
            fileDesc.setPartitionSpecId(partitionSpecId);
        }
        if (partitionDataJson != null) {
            fileDesc.setPartitionDataJson(partitionDataJson);
        }
        if (formatVersion >= 3) {
            fileDesc.setFirstRowId(firstRowId);
            fileDesc.setLastUpdatedSequenceNumber(lastUpdatedSequenceNumber);
        }
        if (formatVersion < MIN_DELETE_FILE_SUPPORT_VERSION) {
            fileDesc.setContent(FILE_CONTENT_DATA);
        } else {
            fileDesc.setDeleteFiles(new ArrayList<>());
            for (IcebergDeleteFileDescriptor descriptor : deleteFiles) {
                fileDesc.addToDeleteFiles(toThrift(descriptor));
            }
        }
        formatDesc.setIcebergParams(fileDesc);

        if (!partitionValues.isEmpty()) {
            List<String> fromPathKeys = new ArrayList<>(partitionValues.size());
            List<String> fromPathValues = new ArrayList<>(partitionValues.size());
            List<Boolean> fromPathIsNull = new ArrayList<>(partitionValues.size());
            for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
                fromPathKeys.add(entry.getKey());
                fromPathValues.add(entry.getValue() != null ? entry.getValue() : "");
                fromPathIsNull.add(entry.getValue() == null);
            }
            rangeDesc.setColumnsFromPathKeys(fromPathKeys);
            rangeDesc.setColumnsFromPath(fromPathValues);
            rangeDesc.setColumnsFromPathIsNull(fromPathIsNull);
        }
    }

    private static TIcebergDeleteFileDesc toThrift(IcebergDeleteFileDescriptor descriptor) {
        TIcebergDeleteFileDesc t = new TIcebergDeleteFileDesc();
        t.setPath(descriptor.getPath());
        applyFileFormat(t, descriptor.getFileFormat());
        switch (descriptor.getKind()) {
            case POSITION_DELETE:
                if (descriptor.getPositionLowerBound() != null) {
                    t.setPositionLowerBound(descriptor.getPositionLowerBound());
                }
                if (descriptor.getPositionUpperBound() != null) {
                    t.setPositionUpperBound(descriptor.getPositionUpperBound());
                }
                t.setContent(IcebergDeleteFileDescriptor.Kind.POSITION_DELETE.wireValue());
                break;
            case DELETION_VECTOR:
                if (descriptor.getPositionLowerBound() != null) {
                    t.setPositionLowerBound(descriptor.getPositionLowerBound());
                }
                if (descriptor.getPositionUpperBound() != null) {
                    t.setPositionUpperBound(descriptor.getPositionUpperBound());
                }
                t.setContent(IcebergDeleteFileDescriptor.Kind.DELETION_VECTOR.wireValue());
                t.setContentOffset(descriptor.getContentOffset());
                t.setContentSizeInBytes(descriptor.getContentSizeInBytes());
                break;
            case EQUALITY_DELETE:
                int[] ids = descriptor.getFieldIds();
                List<Integer> boxed = new ArrayList<>(ids.length);
                for (int id : ids) {
                    boxed.add(id);
                }
                t.setFieldIds(boxed);
                t.setContent(IcebergDeleteFileDescriptor.Kind.EQUALITY_DELETE.wireValue());
                break;
            default:
                throw new IllegalStateException("Unknown delete-file kind: " + descriptor.getKind());
        }
        return t;
    }

    private static void applyFileFormat(TIcebergDeleteFileDesc t, FileFormat fileFormat) {
        if (fileFormat == FileFormat.PARQUET) {
            t.setFileFormat(TFileFormatType.FORMAT_PARQUET);
        } else if (fileFormat == FileFormat.ORC) {
            t.setFileFormat(TFileFormatType.FORMAT_ORC);
        }
        // PUFFIN / AVRO / others: leave unset, matching legacy
        // IcebergScanNode#setDeleteFileFormat behaviour.
    }

    @Override
    public String toString() {
        return "IcebergScanRange{path='" + path + '\''
                + ", start=" + start
                + ", length=" + length
                + ", fileSize=" + fileSize
                + ", formatVersion=" + formatVersion
                + ", partitionSpecId=" + partitionSpecId
                + ", partitionDataJson='" + partitionDataJson + '\''
                + ", partitionValues=" + partitionValues
                + ", originalFilePath='" + originalFilePath + '\''
                + ", deleteFiles=" + deleteFiles
                + ", firstRowId=" + firstRowId
                + ", lastUpdatedSequenceNumber=" + lastUpdatedSequenceNumber
                + ", tableLevelRowCount=" + tableLevelRowCount
                + '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link IcebergScanRange}. */
    public static final class Builder {
        private String path;
        private long start;
        private long length = -1;
        private long fileSize = -1;
        private int formatVersion;
        private Integer partitionSpecId;
        private String partitionDataJson;
        private Map<String, String> partitionValues;
        private String originalFilePath;
        private List<IcebergDeleteFileDescriptor> deleteFiles;
        private Long firstRowId;
        private Long lastUpdatedSequenceNumber;
        private long tableLevelRowCount = -1L;
        private Map<String, String> properties;

        private Builder() {
        }

        public Builder path(String path) {
            this.path = path;
            return this;
        }

        public Builder start(long start) {
            this.start = start;
            return this;
        }

        public Builder length(long length) {
            this.length = length;
            return this;
        }

        public Builder fileSize(long fileSize) {
            this.fileSize = fileSize;
            return this;
        }

        public Builder formatVersion(int formatVersion) {
            this.formatVersion = formatVersion;
            return this;
        }

        public Builder partitionSpecId(Integer partitionSpecId) {
            this.partitionSpecId = partitionSpecId;
            return this;
        }

        public Builder partitionDataJson(String partitionDataJson) {
            this.partitionDataJson = partitionDataJson;
            return this;
        }

        public Builder partitionValues(Map<String, String> partitionValues) {
            this.partitionValues = partitionValues;
            return this;
        }

        public Builder originalFilePath(String originalFilePath) {
            this.originalFilePath = originalFilePath;
            return this;
        }

        public Builder deleteFiles(List<IcebergDeleteFileDescriptor> deleteFiles) {
            this.deleteFiles = deleteFiles;
            return this;
        }

        public Builder firstRowId(Long firstRowId) {
            this.firstRowId = firstRowId;
            return this;
        }

        public Builder lastUpdatedSequenceNumber(Long lastUpdatedSequenceNumber) {
            this.lastUpdatedSequenceNumber = lastUpdatedSequenceNumber;
            return this;
        }

        public Builder tableLevelRowCount(long tableLevelRowCount) {
            this.tableLevelRowCount = tableLevelRowCount;
            return this;
        }

        public Builder properties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public IcebergScanRange build() {
            return new IcebergScanRange(this);
        }
    }
}
