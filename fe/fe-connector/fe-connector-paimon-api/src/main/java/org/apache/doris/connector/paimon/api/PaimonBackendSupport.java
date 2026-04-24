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

package org.apache.doris.connector.paimon.api;

import org.apache.doris.connector.api.timetravel.ConnectorRef;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;
import org.apache.doris.connector.api.timetravel.RefKind;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;

/**
 * Shared utility for backends that delegate to {@link CatalogFactory#createCatalog}
 * with a fixed {@code metastore} option.
 *
 * <p>Paimon's {@link CatalogFactory} dispatches by the {@code metastore}
 * option (via the paimon-core ServiceLoader on
 * {@code org.apache.paimon.factories.Factory}); each backend forces the
 * correct value before delegating, so the orchestrator does not need a
 * static {@code switch} on {@code paimon.catalog.type}. The Doris-specific
 * key {@code paimon.catalog.type} is stripped before dispatch so it does
 * not leak into Paimon's option validation.
 */
public final class PaimonBackendSupport {

    /** Doris-side catalog-type property; stripped before Paimon dispatch. */
    public static final String PAIMON_CATALOG_TYPE = "paimon.catalog.type";

    /** Paimon-side metastore dispatch option. */
    public static final String PAIMON_METASTORE_OPTION = "metastore";

    private PaimonBackendSupport() {
    }

    /**
     * Build a Catalog by forcing the Paimon {@code metastore} option to
     * {@code metastoreId}.
     */
    public static Catalog buildByMetastore(PaimonBackendContext context, String metastoreId) {
        return buildByMetastore(context, metastoreId, new HashMap<>());
    }

    /**
     * Build a Catalog by forcing the Paimon {@code metastore} option to
     * {@code metastoreId} and merging additional backend-specific options
     * (later entries override). Backend-supplied options are applied last
     * so they cannot be overridden by user properties.
     */
    public static Catalog buildByMetastore(PaimonBackendContext context,
                                           String metastoreId,
                                           Map<String, String> extraOptions) {
        Map<String, String> props = new HashMap<>(context.properties());
        props.remove(PAIMON_CATALOG_TYPE);
        props.put(PAIMON_METASTORE_OPTION, metastoreId);
        props.putAll(extraOptions);
        Options options = Options.fromMap(props);
        CatalogContext catalogContext = CatalogContext.create(options);
        return CatalogFactory.createCatalog(catalogContext);
    }

    /**
     * List branches and tags on {@code database.table} via the supplied
     * {@link Catalog}. Branches are reported via
     * {@link org.apache.paimon.utils.BranchManager#branches()} (snapshot id
     * left as {@code -1L} since paimon does not expose a branch-head snapshot
     * id without opening the branch). Tags are enumerated through
     * {@link TagManager#tags()} which already groups names by the snapshot
     * they pin, so each tag carries its concrete {@code snapshotId} and
     * {@code createdAt} timestamp.
     */
    public static List<ConnectorRef> listRefs(Catalog catalog, String database, String table) {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(database, "database");
        Objects.requireNonNull(table, "table");
        DataTable dt = loadDataTable(catalog, database, table);
        List<ConnectorRef> out = new ArrayList<>();
        for (String branch : dt.branchManager().branches()) {
            out.add(ConnectorRef.builder()
                    .name(branch)
                    .kind(RefKind.BRANCH)
                    .snapshotId(-1L)
                    .build());
        }
        SortedMap<Snapshot, List<String>> tags = dt.tagManager().tags();
        for (Map.Entry<Snapshot, List<String>> entry : tags.entrySet()) {
            Snapshot snap = entry.getKey();
            for (String name : entry.getValue()) {
                out.add(ConnectorRef.builder()
                        .name(name)
                        .kind(RefKind.TAG)
                        .snapshotId(snap.id())
                        .createdAt(Instant.ofEpochMilli(snap.timeMillis()))
                        .build());
            }
        }
        return out;
    }

    /**
     * Resolve a {@link ConnectorTableVersion} to the Paimon {@link Snapshot}
     * on {@code database.table} via the supplied {@link Catalog}. The
     * {@code instanceof} matrix covers every sealed subtype.
     *
     * <p>Semantics differ from iceberg in two places:
     * <ul>
     *   <li>{@code ByRef(BRANCH)} resolves through
     *       {@link DataTable#switchToBranch(String)} → {@code latestSnapshot()}
     *       because paimon branches are independent timelines (no global
     *       snapshot graph).
     *   <li>{@code ByRefAtTimestamp(TAG, ts)} is rejected as nonsensical
     *       (a tag is an immutable pin to one snapshot). The iceberg helper
     *       silently re-resolves by global timestamp; paimon must not.
     * </ul>
     */
    public static Snapshot resolveVersion(Catalog catalog,
                                          String database,
                                          String table,
                                          ConnectorTableVersion version) {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(database, "database");
        Objects.requireNonNull(table, "table");
        if (version == null) {
            throw new PaimonBackendException(
                    "ConnectorTableVersion is null for table " + database + "." + table);
        }
        DataTable dt = loadDataTable(catalog, database, table);
        if (version instanceof ConnectorTableVersion.BySnapshotId bySnap) {
            return requireSnapshot(dt.snapshot(bySnap.snapshotId()),
                    database, table, "snapshot id " + bySnap.snapshotId());
        }
        if (version instanceof ConnectorTableVersion.ByTimestamp byTs) {
            Snapshot snap = dt.snapshotManager().earlierOrEqualTimeMills(byTs.ts().toEpochMilli());
            return requireSnapshot(snap, database, table, "timestamp " + byTs.ts());
        }
        if (version instanceof ConnectorTableVersion.ByRef byRef) {
            return resolveRef(dt, byRef.name(), byRef.kind(), database, table);
        }
        if (version instanceof ConnectorTableVersion.ByRefAtTimestamp byRefAt) {
            return resolveRefAtTimestamp(
                    dt, byRefAt.name(), byRefAt.kind(), byRefAt.ts(), database, table);
        }
        if (version instanceof ConnectorTableVersion.ByOpaque) {
            throw new UnsupportedOperationException(
                    "opaque MVCC tokens are not yet supported for paimon"
                            + " (table " + database + "." + table + ")");
        }
        throw new PaimonBackendException(
                "unknown ConnectorTableVersion subtype "
                        + version.getClass().getName()
                        + " on table " + database + "." + table);
    }

    private static Snapshot resolveRef(DataTable dt,
                                       String refName,
                                       RefKind expectedKind,
                                       String database,
                                       String table) {
        if (expectedKind == RefKind.BRANCH) {
            DataTable branchTable = dt.switchToBranch(refName);
            Optional<Snapshot> latest = branchTable.latestSnapshot();
            return latest.orElseThrow(() -> new PaimonBackendException(
                    "paimon branch '" + refName + "' on table "
                            + database + "." + table + " has no snapshots"));
        }
        if (expectedKind == RefKind.TAG) {
            TagManager tagManager = dt.tagManager();
            Optional<Tag> tag = tagManager.get(refName);
            Tag t = tag.orElseThrow(() -> new PaimonBackendException(
                    "paimon tag '" + refName + "' not found on table "
                            + database + "." + table));
            return t.trimToSnapshot();
        }
        throw new PaimonBackendException(
                "unsupported RefKind " + expectedKind + " for paimon ref '"
                        + refName + "' on table " + database + "." + table);
    }

    private static Snapshot resolveRefAtTimestamp(DataTable dt,
                                                  String refName,
                                                  RefKind expectedKind,
                                                  Instant ts,
                                                  String database,
                                                  String table) {
        if (expectedKind == RefKind.BRANCH) {
            DataTable branchTable = dt.switchToBranch(refName);
            SnapshotManager sm = branchTable.snapshotManager();
            Snapshot snap = sm.earlierOrEqualTimeMills(ts.toEpochMilli());
            return requireSnapshot(snap, database, table,
                    "branch '" + refName + "' at timestamp " + ts);
        }
        // Tag at timestamp: a tag pins a single snapshot; combining with a
        // timestamp is incoherent. iceberg silently re-resolves by global
        // ts which is wrong for paimon (no shared timeline across refs).
        throw new PaimonBackendException(
                "paimon does not support timestamp-qualified " + expectedKind
                        + " ref '" + refName + "' on table " + database + "." + table);
    }

    private static DataTable loadDataTable(Catalog catalog, String database, String table) {
        try {
            Table t = catalog.getTable(Identifier.create(database, table));
            if (t instanceof DataTable dt) {
                return dt;
            }
            throw new PaimonBackendException(
                    "paimon table " + database + "." + table
                            + " (" + t.getClass().getName()
                            + ") does not support time travel: not a DataTable");
        } catch (Catalog.TableNotExistException e) {
            throw new PaimonBackendException(
                    "paimon table " + database + "." + table + " not found", e);
        }
    }

    private static Snapshot requireSnapshot(Snapshot snap,
                                            String database,
                                            String table,
                                            String locator) {
        if (snap == null) {
            throw new PaimonBackendException(
                    "paimon snapshot not found for " + locator
                            + " on table " + database + "." + table);
        }
        return snap;
    }
}
