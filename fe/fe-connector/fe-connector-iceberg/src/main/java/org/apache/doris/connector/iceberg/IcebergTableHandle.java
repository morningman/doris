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

import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import java.util.Objects;

/**
 * Opaque table handle for an Iceberg table.
 *
 * <p>Carries the database (namespace) / table name coordinates plus the
 * iceberg-specific metadata snapshot identifiers required by the read path
 * (snapshot id, ref spec, format version, schema id, partition spec id and
 * metadata file location). All fields are immutable; callers wanting to
 * narrow a handle (e.g. pinning a snapshot id post time-travel resolution)
 * must build a new handle via {@link #toBuilder()}.</p>
 */
public class IcebergTableHandle implements ConnectorTableHandle {

    private static final long serialVersionUID = 2L;

    private final String dbName;
    private final String tableName;
    private final Long snapshotId;
    private final String refSpec;
    private final Integer formatVersion;
    private final Integer schemaId;
    private final Integer partitionSpecId;
    private final String metadataLocation;

    public IcebergTableHandle(String dbName, String tableName) {
        this(dbName, tableName, null, null, null, null, null, null);
    }

    public IcebergTableHandle(String dbName,
                              String tableName,
                              Long snapshotId,
                              String refSpec,
                              Integer formatVersion,
                              Integer schemaId,
                              Integer partitionSpecId,
                              String metadataLocation) {
        this.dbName = Objects.requireNonNull(dbName, "dbName");
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.snapshotId = snapshotId;
        this.refSpec = refSpec;
        this.formatVersion = formatVersion;
        this.schemaId = schemaId;
        this.partitionSpecId = partitionSpecId;
        this.metadataLocation = metadataLocation;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    /** Returns the pinned snapshot id, or {@code null} to indicate "use current". */
    public Long getSnapshotId() {
        return snapshotId;
    }

    /** Returns the iceberg ref spec (branch/tag identifier) or {@code null}. */
    public String getRefSpec() {
        return refSpec;
    }

    /** Returns the iceberg format version (1 or 2) or {@code null} if unresolved. */
    public Integer getFormatVersion() {
        return formatVersion;
    }

    /** Returns the schema id used to resolve columns, or {@code null} for current. */
    public Integer getSchemaId() {
        return schemaId;
    }

    /** Returns the partition spec id, or {@code null} if unresolved. */
    public Integer getPartitionSpecId() {
        return partitionSpecId;
    }

    /** Returns the iceberg metadata file location, or {@code null} if unresolved. */
    public String getMetadataLocation() {
        return metadataLocation;
    }

    public Builder toBuilder() {
        return new Builder()
                .dbName(dbName)
                .tableName(tableName)
                .snapshotId(snapshotId)
                .refSpec(refSpec)
                .formatVersion(formatVersion)
                .schemaId(schemaId)
                .partitionSpecId(partitionSpecId)
                .metadataLocation(metadataLocation);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IcebergTableHandle)) {
            return false;
        }
        IcebergTableHandle that = (IcebergTableHandle) o;
        return Objects.equals(dbName, that.dbName)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(snapshotId, that.snapshotId)
                && Objects.equals(refSpec, that.refSpec)
                && Objects.equals(formatVersion, that.formatVersion)
                && Objects.equals(schemaId, that.schemaId)
                && Objects.equals(partitionSpecId, that.partitionSpecId)
                && Objects.equals(metadataLocation, that.metadataLocation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbName, tableName, snapshotId, refSpec,
                formatVersion, schemaId, partitionSpecId, metadataLocation);
    }

    @Override
    public String toString() {
        return "IcebergTableHandle{"
                + dbName + "." + tableName
                + ", snapshotId=" + snapshotId
                + ", refSpec=" + refSpec
                + ", formatVersion=" + formatVersion
                + ", schemaId=" + schemaId
                + ", partitionSpecId=" + partitionSpecId
                + ", metadataLocation=" + metadataLocation
                + '}';
    }

    /** Builder for {@link IcebergTableHandle}; required fields are db/table name. */
    public static final class Builder {
        private String dbName;
        private String tableName;
        private Long snapshotId;
        private String refSpec;
        private Integer formatVersion;
        private Integer schemaId;
        private Integer partitionSpecId;
        private String metadataLocation;

        public Builder dbName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder snapshotId(Long snapshotId) {
            this.snapshotId = snapshotId;
            return this;
        }

        public Builder refSpec(String refSpec) {
            this.refSpec = refSpec;
            return this;
        }

        public Builder formatVersion(Integer formatVersion) {
            this.formatVersion = formatVersion;
            return this;
        }

        public Builder schemaId(Integer schemaId) {
            this.schemaId = schemaId;
            return this;
        }

        public Builder partitionSpecId(Integer partitionSpecId) {
            this.partitionSpecId = partitionSpecId;
            return this;
        }

        public Builder metadataLocation(String metadataLocation) {
            this.metadataLocation = metadataLocation;
            return this;
        }

        public IcebergTableHandle build() {
            return new IcebergTableHandle(dbName, tableName, snapshotId, refSpec,
                    formatVersion, schemaId, partitionSpecId, metadataLocation);
        }
    }
}
