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

import org.apache.doris.connector.api.handle.ConnectorColumnHandle;

import org.apache.iceberg.types.Type;

import java.util.Objects;

/**
 * Column handle for Iceberg tables.
 *
 * <p>Carries the iceberg field id (used for resolve-by-id when reading parquet
 * with column-id based mapping), the column name, raw iceberg type and
 * nullability flag. The iceberg {@link Type} is intentionally retained as the
 * native iceberg type rather than a Doris type — the Doris-side conversion
 * lives in {@code IcebergTypeMapping} and is applied at scan-plumbing time,
 * not at column-handle construction time.</p>
 */
public class IcebergColumnHandle implements ConnectorColumnHandle {

    private static final long serialVersionUID = 1L;

    private final int fieldId;
    private final String name;
    private final Type icebergType;
    private final boolean nullable;
    private final String comment;
    private final Integer position;

    public IcebergColumnHandle(int fieldId, String name, Type icebergType, boolean nullable) {
        this(fieldId, name, icebergType, nullable, null, null);
    }

    public IcebergColumnHandle(int fieldId,
                               String name,
                               Type icebergType,
                               boolean nullable,
                               String comment,
                               Integer position) {
        this.fieldId = fieldId;
        this.name = Objects.requireNonNull(name, "name");
        this.icebergType = Objects.requireNonNull(icebergType, "icebergType");
        this.nullable = nullable;
        this.comment = comment;
        this.position = position;
    }

    public int getFieldId() {
        return fieldId;
    }

    public String getName() {
        return name;
    }

    public Type getIcebergType() {
        return icebergType;
    }

    public boolean isNullable() {
        return nullable;
    }

    public String getComment() {
        return comment;
    }

    public Integer getPosition() {
        return position;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IcebergColumnHandle)) {
            return false;
        }
        IcebergColumnHandle that = (IcebergColumnHandle) o;
        return fieldId == that.fieldId
                && nullable == that.nullable
                && Objects.equals(name, that.name)
                && Objects.equals(icebergType, that.icebergType)
                && Objects.equals(comment, that.comment)
                && Objects.equals(position, that.position);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldId, name, icebergType, nullable, comment, position);
    }

    @Override
    public String toString() {
        return "IcebergColumnHandle{"
                + "fieldId=" + fieldId
                + ", name=" + name
                + ", type=" + icebergType
                + ", nullable=" + nullable
                + (position != null ? ", position=" + position : "")
                + '}';
    }
}
