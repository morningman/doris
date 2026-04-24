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

package org.apache.doris.connector.iceberg.systable;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorType;

import org.apache.iceberg.MetadataTableType;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Hard-coded {@link ConnectorColumn} schemas for the seven Iceberg metadata
 * tables exposed by the plugin via {@link IcebergSystemTableOps}.
 *
 * <p>Schemas mirror the columns produced by
 * {@code org.apache.iceberg.MetadataTableUtils#createMetadataTableInstance(...)}
 * for the corresponding {@link MetadataTableType}. They are declared statically
 * so that {@link IcebergSystemTableOps#listSysTables} stays I/O-free, as
 * required by spec §9.6.</p>
 *
 * <p>Only the seven sys tables M1-13 publishes are covered:
 * SNAPSHOTS, HISTORY, FILES (data files), ENTRIES, MANIFESTS, REFS, PARTITIONS.</p>
 */
final class IcebergMetadataTables {

    private static final ConnectorType STRING = new ConnectorType("STRING");
    private static final ConnectorType INT = new ConnectorType("INT");
    private static final ConnectorType BIGINT = new ConnectorType("BIGINT");
    private static final ConnectorType BOOLEAN = new ConnectorType("BOOLEAN");
    /** Iceberg metadata timestamps are microsecond-precision with TZ; map to DATETIMEV2(6). */
    private static final ConnectorType TIMESTAMP_TZ = new ConnectorType("DATETIMEV2", 6, 0);
    private static final ConnectorType MAP_INT_BIGINT = new ConnectorType(
            "MAP", -1, -1, Arrays.asList(INT, BIGINT));
    private static final ConnectorType MAP_INT_STRING = new ConnectorType(
            "MAP", -1, -1, Arrays.asList(INT, STRING));
    private static final ConnectorType MAP_STRING_STRING = new ConnectorType(
            "MAP", -1, -1, Arrays.asList(STRING, STRING));
    private static final ConnectorType ARRAY_LONG = new ConnectorType(
            "ARRAY", -1, -1, Collections.singletonList(BIGINT));
    private static final ConnectorType ARRAY_INT = new ConnectorType(
            "ARRAY", -1, -1, Collections.singletonList(INT));

    private static final Map<MetadataTableType, List<ConnectorColumn>> SCHEMAS;
    private static final Map<String, MetadataTableType> SUPPORTED;

    static {
        Map<MetadataTableType, List<ConnectorColumn>> schemas = new LinkedHashMap<>();
        schemas.put(MetadataTableType.SNAPSHOTS, Arrays.asList(
                col("committed_at", TIMESTAMP_TZ),
                col("snapshot_id", BIGINT),
                col("parent_id", BIGINT),
                col("operation", STRING),
                col("manifest_list", STRING),
                col("summary", MAP_STRING_STRING)));
        schemas.put(MetadataTableType.HISTORY, Arrays.asList(
                col("made_current_at", TIMESTAMP_TZ),
                col("snapshot_id", BIGINT),
                col("parent_id", BIGINT),
                col("is_current_ancestor", BOOLEAN)));
        schemas.put(MetadataTableType.FILES, Arrays.asList(
                col("content", INT),
                col("file_path", STRING),
                col("file_format", STRING),
                col("spec_id", INT),
                col("record_count", BIGINT),
                col("file_size_in_bytes", BIGINT),
                col("column_sizes", MAP_INT_BIGINT),
                col("value_counts", MAP_INT_BIGINT),
                col("null_value_counts", MAP_INT_BIGINT),
                col("nan_value_counts", MAP_INT_BIGINT),
                col("lower_bounds", MAP_INT_STRING),
                col("upper_bounds", MAP_INT_STRING),
                col("split_offsets", ARRAY_LONG),
                col("equality_ids", ARRAY_INT)));
        schemas.put(MetadataTableType.ENTRIES, Arrays.asList(
                col("status", INT),
                col("snapshot_id", BIGINT),
                col("sequence_number", BIGINT),
                col("file_sequence_number", BIGINT),
                col("data_file", STRING)));
        schemas.put(MetadataTableType.MANIFESTS, Arrays.asList(
                col("content", INT),
                col("path", STRING),
                col("length", BIGINT),
                col("partition_spec_id", INT),
                col("added_snapshot_id", BIGINT),
                col("added_data_files_count", INT),
                col("existing_data_files_count", INT),
                col("deleted_data_files_count", INT),
                col("added_delete_files_count", INT),
                col("existing_delete_files_count", INT),
                col("deleted_delete_files_count", INT)));
        schemas.put(MetadataTableType.REFS, Arrays.asList(
                col("name", STRING),
                col("type", STRING),
                col("snapshot_id", BIGINT),
                col("max_reference_age_in_ms", BIGINT),
                col("min_snapshots_to_keep", INT),
                col("max_snapshot_age_in_ms", BIGINT)));
        schemas.put(MetadataTableType.PARTITIONS, Arrays.asList(
                col("partition", STRING),
                col("spec_id", INT),
                col("record_count", BIGINT),
                col("file_count", INT),
                col("total_data_file_size_in_bytes", BIGINT),
                col("position_delete_record_count", BIGINT),
                col("position_delete_file_count", INT),
                col("equality_delete_record_count", BIGINT),
                col("equality_delete_file_count", INT),
                col("last_updated_at", TIMESTAMP_TZ),
                col("last_updated_snapshot_id", BIGINT)));
        SCHEMAS = Collections.unmodifiableMap(schemas);

        Map<String, MetadataTableType> supported = new LinkedHashMap<>();
        for (MetadataTableType t : SCHEMAS.keySet()) {
            supported.put(t.name().toLowerCase(Locale.ROOT), t);
        }
        SUPPORTED = Collections.unmodifiableMap(supported);
    }

    private IcebergMetadataTables() {
    }

    /** Lower-cased sys-table-name → {@link MetadataTableType} for the seven supported entries. */
    static Map<String, MetadataTableType> supported() {
        return SUPPORTED;
    }

    static List<ConnectorColumn> columnsOf(MetadataTableType type) {
        Objects.requireNonNull(type, "type");
        List<ConnectorColumn> cols = SCHEMAS.get(type);
        if (cols == null) {
            throw new IllegalArgumentException("Unsupported iceberg metadata table type: " + type);
        }
        return cols;
    }

    private static ConnectorColumn col(String name, ConnectorType type) {
        return new ConnectorColumn(name, type, "", true, null);
    }
}
