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

package org.apache.doris.connector.hive.systable;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.systable.SysTableExecutionMode;
import org.apache.doris.connector.api.systable.SysTableSpec;
import org.apache.doris.connector.api.systable.SystemTableOps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * {@link SystemTableOps} implementation for the Hive connector.
 *
 * <p>Publishes a single {@link SysTableSpec} entry — {@code $partitions} — in
 * {@link SysTableExecutionMode#TVF TVF} mode. Hive intentionally selects the
 * TVF execution mode (rather than NATIVE like iceberg / paimon) because a
 * partitions listing reverse-look-up against the Hive Metastore is heavier
 * than the per-table metadata views iceberg / paimon expose, and reusing the
 * existing {@code partition_values} TVF infrastructure is the more conservative
 * route. See {@code plan-doc/D6-system-tables.md} §"mode 选择".</p>
 *
 * <p>Per spec §9.6 / §9.5 the spec declares
 * {@code acceptsTableVersion(false)} — partition listings are always
 * latest-state. The schema declared here is a best-effort static placeholder
 * (a single {@code partition_name} column); the real per-partition column
 * shape is resolved by the fe-core
 * {@link org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction
 * TableValuedFunction} at plan time, where the actual partition columns of
 * the underlying Hive table become available.</p>
 *
 * <p>This is the M1-15 demonstration of the TVF-mode plugin route. It is
 * complementary to M1-13 (iceberg) / M1-14 (paimon) which both validate the
 * NATIVE mode plugin route.</p>
 */
public final class HiveSystemTableOps implements SystemTableOps {

    /** Sole sys-table suffix (without the leading {@code '$'}) published by Hive. */
    public static final String PARTITIONS = "partitions";

    /**
     * Static placeholder schema for {@code $partitions}.
     * <p>The real column list (per partition key) is bound at TVF planning
     * time through {@code partition_values}'s table; this single column is
     * sufficient for {@code DESCRIBE} introspection and is not used during
     * execution.</p>
     */
    private static final ConnectorTableSchema PARTITIONS_SCHEMA = new ConnectorTableSchema(
            PARTITIONS,
            Collections.singletonList(
                    new ConnectorColumn(
                            "partition_name",
                            ConnectorType.of("STRING"),
                            "Hive partition name (best-effort placeholder; "
                                    + "real shape resolved at TVF execution time).",
                            true,
                            null)),
            "HIVE_METADATA",
            Collections.emptyMap());

    private final Map<String, SysTableSpec> specs;
    private final Set<String> suffixes;

    public HiveSystemTableOps() {
        Map<String, SysTableSpec> built = new LinkedHashMap<>();
        SysTableSpec partitionsSpec = SysTableSpec.builder()
                .name(PARTITIONS)
                .schema(PARTITIONS_SCHEMA)
                .mode(SysTableExecutionMode.TVF)
                .acceptsTableVersion(false)
                .tvfInvoker(new HivePartitionsTvfInvoker())
                .build();
        built.put(PARTITIONS, partitionsSpec);
        this.specs = Collections.unmodifiableMap(built);
        this.suffixes = Collections.unmodifiableSet(new LinkedHashSet<>(this.specs.keySet()));
    }

    @Override
    public List<SysTableSpec> listSysTables(String database, String table) {
        Objects.requireNonNull(database, "database");
        Objects.requireNonNull(table, "table");
        return Collections.unmodifiableList(new ArrayList<>(specs.values()));
    }

    @Override
    public Optional<SysTableSpec> getSysTable(String database, String table, String sysTableName) {
        Objects.requireNonNull(database, "database");
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(sysTableName, "sysTableName");
        SysTableSpec spec = specs.get(sysTableName.toLowerCase(Locale.ROOT));
        return spec == null ? Optional.empty() : Optional.of(spec);
    }

    /**
     * Set of sys-table suffixes (without the leading {@code '$'}) published by
     * this ops instance — currently always {@code {"partitions"}}.
     */
    public Set<String> listSysTableSuffixes(String database, String table) {
        Objects.requireNonNull(database, "database");
        Objects.requireNonNull(table, "table");
        return suffixes;
    }
}
