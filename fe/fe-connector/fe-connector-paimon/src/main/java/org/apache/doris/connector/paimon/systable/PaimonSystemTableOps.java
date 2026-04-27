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
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.systable.SysTableExecutionMode;
import org.apache.doris.connector.api.systable.SysTableSpec;
import org.apache.doris.connector.api.systable.SystemTableOps;

import org.apache.paimon.table.Table;

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
import java.util.function.BiFunction;

/**
 * {@link SystemTableOps} implementation for the paimon connector.
 *
 * <p>Publishes thirteen {@link SysTableSpec} entries — {@code snapshots},
 * {@code schemas}, {@code options}, {@code tags}, {@code branches},
 * {@code consumers}, {@code aggregation_fields}, {@code files},
 * {@code manifests}, {@code partitions}, {@code statistics},
 * {@code buckets}, {@code table_indexes} — all in {@link
 * SysTableExecutionMode#NATIVE NATIVE} mode. Each spec is built once at
 * construction time and is immutable; {@link #listSysTables} simply
 * returns the pre-computed list, satisfying the §9.6 "no I/O"
 * requirement.</p>
 *
 * <p>The base {@link Table} needed by each {@code SystemTableLoader.load}
 * call is obtained on demand through {@code baseTableLoader}, which is
 * wired by {@link org.apache.doris.connector.paimon.PaimonConnectorMetadata}
 * to the {@code paimon.table} {@link
 * org.apache.doris.connector.api.cache.MetaCacheHandle MetaCacheHandle}
 * (M1-11), so D3 invalidation on the main table propagates automatically.</p>
 *
 * <p><b>Versioning:</b> all thirteen sys tables are full-history views;
 * per spec §9.6 / §9.5 their specs declare {@code acceptsTableVersion(false)}.
 * fe-core therefore strips any {@code FOR VERSION / FOR TIMESTAMP / FOR BRANCH}
 * clause before invoking the factory.</p>
 *
 * <p><b>Out-of-scope sys tables:</b> {@code audit_log}, {@code binlog},
 * {@code ro}, {@code row_tracking} all derive their column layout from
 * the main table's user schema and therefore cannot be published with a
 * static (I/O-free) schema. They are intentionally not registered here
 * — see {@link PaimonSystemTableSchemas} class Javadoc.</p>
 */
public final class PaimonSystemTableOps implements SystemTableOps {

    /** Lower-cased sys-table name → spec. Order matches publication order. */
    private final Map<String, SysTableSpec> specs;

    /** Cached unmodifiable view of {@link #specs}'s key set. */
    private final Set<String> suffixes;

    public PaimonSystemTableOps(BiFunction<String, String, Table> baseTableLoader) {
        Objects.requireNonNull(baseTableLoader, "baseTableLoader");
        Map<String, SysTableSpec> built = new LinkedHashMap<>();
        for (Map.Entry<String, List<ConnectorColumn>> e : PaimonSystemTableSchemas.supported().entrySet()) {
            String name = e.getKey();
            ConnectorTableSchema schema = new ConnectorTableSchema(
                    name,
                    e.getValue(),
                    "PAIMON_METADATA",
                    Collections.emptyMap());
            SysTableSpec spec = SysTableSpec.builder()
                    .name(name)
                    .schema(schema)
                    .mode(SysTableExecutionMode.NATIVE)
                    .acceptsTableVersion(false)
                    .nativeFactory(new PaimonMetadataScanFactory(name, baseTableLoader))
                    .build();
            built.put(name, spec);
        }
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
     * Set of sys-table suffixes (without leading {@code '$'}) published by
     * this ops instance. Equivalent to the keys of {@link #listSysTables}
     * but in a cheap-to-iterate {@link Set} form for callers (e.g.
     * completion / catalog introspection) that only need names. Iteration
     * order matches publication order.
     */
    public Set<String> listSysTableSuffixes(String database, String table) {
        Objects.requireNonNull(database, "database");
        Objects.requireNonNull(table, "table");
        return suffixes;
    }
}
