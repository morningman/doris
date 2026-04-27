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

package org.apache.doris.connector.paimon.systable;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.paimon.PaimonTypeMapping;

import org.apache.paimon.table.system.AggregationFieldsTable;
import org.apache.paimon.table.system.BranchesTable;
import org.apache.paimon.table.system.BucketsTable;
import org.apache.paimon.table.system.ConsumersTable;
import org.apache.paimon.table.system.FilesTable;
import org.apache.paimon.table.system.ManifestsTable;
import org.apache.paimon.table.system.OptionsTable;
import org.apache.paimon.table.system.PartitionsTable;
import org.apache.paimon.table.system.SchemasTable;
import org.apache.paimon.table.system.SnapshotsTable;
import org.apache.paimon.table.system.StatisticTable;
import org.apache.paimon.table.system.TableIndexesTable;
import org.apache.paimon.table.system.TagsTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Hard-coded {@link ConnectorColumn} schemas for the paimon system tables
 * exposed by the plugin via {@link PaimonSystemTableOps}.
 *
 * <p>Each schema mirrors the paimon SDK's {@code <SystemTable>.TABLE_TYPE}
 * static {@link RowType}, read once at class-init via reflection. No paimon
 * I/O is involved: the SDK declares these row types as compile-time constants
 * on the per-table classes (see e.g.
 * {@link SnapshotsTable#TABLE_TYPE}), so reading them satisfies the §9.6
 * "no metastore call" requirement for {@link PaimonSystemTableOps#listSysTables}.</p>
 *
 * <p>Sys tables that depend on the main table's user schema —
 * {@code audit_log}, {@code binlog}, {@code ro}, {@code row_tracking} —
 * are intentionally omitted: their column layout is not derivable without
 * loading the main table, which would violate the I/O-free rule.</p>
 */
public final class PaimonSystemTableSchemas {

    /** Lower-cased sys-table name → ordered ConnectorColumn list. */
    private static final Map<String, List<ConnectorColumn>> SCHEMAS;

    static {
        Map<String, List<ConnectorColumn>> built = new LinkedHashMap<>();
        register(built, SnapshotsTable.SNAPSHOTS, SnapshotsTable.class);
        register(built, SchemasTable.SCHEMAS, SchemasTable.class);
        register(built, OptionsTable.OPTIONS, OptionsTable.class);
        register(built, TagsTable.TAGS, TagsTable.class);
        register(built, BranchesTable.BRANCHES, BranchesTable.class);
        register(built, ConsumersTable.CONSUMERS, ConsumersTable.class);
        register(built, AggregationFieldsTable.AGGREGATION_FIELDS, AggregationFieldsTable.class);
        register(built, FilesTable.FILES, FilesTable.class);
        register(built, ManifestsTable.MANIFESTS, ManifestsTable.class);
        register(built, PartitionsTable.PARTITIONS, PartitionsTable.class);
        register(built, StatisticTable.STATISTICS, StatisticTable.class);
        register(built, BucketsTable.BUCKETS, BucketsTable.class);
        register(built, TableIndexesTable.TABLE_INDEXES, TableIndexesTable.class);
        SCHEMAS = Collections.unmodifiableMap(built);
    }

    private PaimonSystemTableSchemas() {
    }

    /** Lower-cased sys-table-name → column list, in publication order. */
    public static Map<String, List<ConnectorColumn>> supported() {
        return SCHEMAS;
    }

    public static List<ConnectorColumn> columnsOf(String name) {
        Objects.requireNonNull(name, "name");
        List<ConnectorColumn> cols = SCHEMAS.get(name.toLowerCase(Locale.ROOT));
        if (cols == null) {
            throw new IllegalArgumentException("Unsupported paimon system table: " + name);
        }
        return cols;
    }

    private static void register(Map<String, List<ConnectorColumn>> sink,
                                 String name,
                                 Class<?> tableClass) {
        RowType rowType = readTableType(tableClass);
        List<ConnectorColumn> columns = new ArrayList<>(rowType.getFields().size());
        for (DataField field : rowType.getFields()) {
            ConnectorType type = mapType(field);
            // All paimon metadata-table columns are surfaced as nullable in Doris
            // — the engine (post-M1-15 ScanNode) materialises them via the SDK and
            // the sys-table view should not propagate paimon's NOT NULL contract,
            // because intermediate filters / projections may produce empty groups.
            columns.add(new ConnectorColumn(
                    field.name().toLowerCase(Locale.ROOT),
                    type,
                    field.description() != null ? field.description() : "",
                    true,
                    null));
        }
        sink.put(name.toLowerCase(Locale.ROOT), Collections.unmodifiableList(columns));
    }

    /**
     * Best-effort paimon → connector type mapping for sys-table columns.
     *
     * <p>Falls back to STRING if the paimon SDK's {@link DataField#type()}
     * cannot be mapped by {@link PaimonTypeMapping} (e.g. nested
     * {@code ARRAY<ROW<...>>} on {@code table_indexes.dv_ranges}). Any
     * fallback is documented in the corresponding test, and the underlying
     * value remains queryable as its serialised string form.</p>
     */
    private static ConnectorType mapType(DataField field) {
        try {
            return PaimonTypeMapping.toConnectorType(field.type(), PaimonTypeMapping.Options.DEFAULT);
        } catch (RuntimeException e) {
            return new ConnectorType("STRING");
        }
    }

    private static RowType readTableType(Class<?> tableClass) {
        try {
            Field f = tableClass.getDeclaredField("TABLE_TYPE");
            f.setAccessible(true);
            return (RowType) f.get(null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException(
                    "paimon sys-table class " + tableClass.getName()
                            + " has no accessible TABLE_TYPE static field", e);
        }
    }
}
