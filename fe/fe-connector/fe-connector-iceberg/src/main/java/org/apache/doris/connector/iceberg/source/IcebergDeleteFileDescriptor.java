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

import org.apache.iceberg.FileFormat;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * Plugin-private descriptor for an iceberg delete file referenced by a
 * single data scan range. Mirrors the legacy
 * {@code IcebergDeleteFileFilter} hierarchy (PositionDelete /
 * DeletionVector / EqualityDelete) as a flat immutable POJO so that the
 * fe-connector-iceberg module does not need to import any
 * fe-core types.
 *
 * <p>The {@link Kind} value selects which optional fields apply:</p>
 * <ul>
 *   <li>{@link Kind#POSITION_DELETE} — optional
 *       {@link #positionLowerBound}/{@link #positionUpperBound} (iceberg
 *       v2 position deletes; file format is typically PARQUET / ORC /
 *       AVRO).</li>
 *   <li>{@link Kind#DELETION_VECTOR} — same optional position bounds
 *       plus required {@link #contentOffset}/{@link #contentSizeInBytes}
 *       which point at a specific blob inside a Puffin file (iceberg v3
 *       deletion vectors).</li>
 *   <li>{@link Kind#EQUALITY_DELETE} — required non-empty
 *       {@link #fieldIds}.</li>
 * </ul>
 *
 * <p>The integer wire-format value carried in {@code TIcebergDeleteFileDesc#content}
 * matches the legacy convention: data=0, position-delete=1, equality-delete=2,
 * deletion-vector=3.</p>
 */
public final class IcebergDeleteFileDescriptor implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Delete file kind. The {@link #wireValue} is the BE-side integer code. */
    public enum Kind {
        POSITION_DELETE(1),
        EQUALITY_DELETE(2),
        DELETION_VECTOR(3);

        private final int wireValue;

        Kind(int wireValue) {
            this.wireValue = wireValue;
        }

        /** Returns the BE wire-protocol integer for {@code TIcebergDeleteFileDesc#content}. */
        public int wireValue() {
            return wireValue;
        }
    }

    private final Kind kind;
    private final String path;
    private final FileFormat fileFormat;

    // POSITION_DELETE / DELETION_VECTOR optional bounds; null means absent.
    private final Long positionLowerBound;
    private final Long positionUpperBound;

    // DELETION_VECTOR specific: blob coordinates inside the Puffin file.
    private final long contentOffset;
    private final long contentSizeInBytes;

    // EQUALITY_DELETE specific.
    private final int[] fieldIds;

    private IcebergDeleteFileDescriptor(Builder b) {
        this.kind = Objects.requireNonNull(b.kind, "kind is required");
        this.path = Objects.requireNonNull(b.path, "path is required");
        this.fileFormat = Objects.requireNonNull(b.fileFormat, "fileFormat is required");
        this.positionLowerBound = b.positionLowerBound;
        this.positionUpperBound = b.positionUpperBound;
        this.contentOffset = b.contentOffset;
        this.contentSizeInBytes = b.contentSizeInBytes;
        this.fieldIds = b.fieldIds == null ? null : b.fieldIds.clone();
        if (this.kind == Kind.EQUALITY_DELETE) {
            if (this.fieldIds == null || this.fieldIds.length == 0) {
                throw new IllegalArgumentException(
                        "EQUALITY_DELETE descriptor requires a non-empty fieldIds array");
            }
        }
    }

    public Kind getKind() {
        return kind;
    }

    public String getPath() {
        return path;
    }

    public FileFormat getFileFormat() {
        return fileFormat;
    }

    /** Returns the iceberg position-delete lower bound, or {@code null} if absent. */
    public Long getPositionLowerBound() {
        return positionLowerBound;
    }

    /** Returns the iceberg position-delete upper bound, or {@code null} if absent. */
    public Long getPositionUpperBound() {
        return positionUpperBound;
    }

    /** Returns the deletion-vector blob offset inside the Puffin file. */
    public long getContentOffset() {
        return contentOffset;
    }

    /** Returns the deletion-vector blob length in bytes. */
    public long getContentSizeInBytes() {
        return contentSizeInBytes;
    }

    /** Returns a defensive copy of the equality-delete field ids, or {@code null} for non-equality kinds. */
    public int[] getFieldIds() {
        return fieldIds == null ? null : fieldIds.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IcebergDeleteFileDescriptor)) {
            return false;
        }
        IcebergDeleteFileDescriptor that = (IcebergDeleteFileDescriptor) o;
        return contentOffset == that.contentOffset
                && contentSizeInBytes == that.contentSizeInBytes
                && kind == that.kind
                && path.equals(that.path)
                && fileFormat == that.fileFormat
                && Objects.equals(positionLowerBound, that.positionLowerBound)
                && Objects.equals(positionUpperBound, that.positionUpperBound)
                && Arrays.equals(fieldIds, that.fieldIds);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(kind, path, fileFormat, positionLowerBound, positionUpperBound,
                contentOffset, contentSizeInBytes);
        result = 31 * result + Arrays.hashCode(fieldIds);
        return result;
    }

    @Override
    public String toString() {
        return "IcebergDeleteFileDescriptor{kind=" + kind
                + ", path='" + path + '\''
                + ", fileFormat=" + fileFormat
                + ", positionLowerBound=" + positionLowerBound
                + ", positionUpperBound=" + positionUpperBound
                + ", contentOffset=" + contentOffset
                + ", contentSizeInBytes=" + contentSizeInBytes
                + ", fieldIds=" + Arrays.toString(fieldIds)
                + '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link IcebergDeleteFileDescriptor}. */
    public static final class Builder {
        private Kind kind;
        private String path;
        private FileFormat fileFormat;
        private Long positionLowerBound;
        private Long positionUpperBound;
        private long contentOffset;
        private long contentSizeInBytes;
        private int[] fieldIds;

        private Builder() {
        }

        public Builder kind(Kind kind) {
            this.kind = kind;
            return this;
        }

        public Builder path(String path) {
            this.path = path;
            return this;
        }

        public Builder fileFormat(FileFormat fileFormat) {
            this.fileFormat = fileFormat;
            return this;
        }

        public Builder positionLowerBound(Long positionLowerBound) {
            this.positionLowerBound = positionLowerBound;
            return this;
        }

        public Builder positionUpperBound(Long positionUpperBound) {
            this.positionUpperBound = positionUpperBound;
            return this;
        }

        public Builder contentOffset(long contentOffset) {
            this.contentOffset = contentOffset;
            return this;
        }

        public Builder contentSizeInBytes(long contentSizeInBytes) {
            this.contentSizeInBytes = contentSizeInBytes;
            return this;
        }

        public Builder fieldIds(int[] fieldIds) {
            this.fieldIds = fieldIds == null ? null : fieldIds.clone();
            return this;
        }

        public IcebergDeleteFileDescriptor build() {
            return new IcebergDeleteFileDescriptor(this);
        }
    }
}
