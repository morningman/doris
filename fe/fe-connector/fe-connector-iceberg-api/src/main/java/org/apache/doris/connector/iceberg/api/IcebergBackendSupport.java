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

package org.apache.doris.connector.iceberg.api;

import org.apache.doris.connector.api.timetravel.ConnectorRef;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;
import org.apache.doris.connector.api.timetravel.RefKind;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.util.SnapshotUtil;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Shared helpers used by backends that delegate to the standard Iceberg
 * SDK paths — both {@code CatalogUtil.buildIcebergCatalog} construction
 * and branch/tag/snapshot resolution on an already-loaded {@link Table}.
 *
 * <p>Extracted here so every backend module ships identical time-travel
 * semantics without duplicating the switch over
 * {@link ConnectorTableVersion} subtypes.
 */
public final class IcebergBackendSupport {

    private IcebergBackendSupport() {
    }

    /**
     * Build a Catalog by forcing {@code catalog-impl} to {@code catalogImplFqcn}.
     *
     * <p>The Iceberg SDK rejects property maps that contain both {@code type}
     * and {@code catalog-impl}; this helper strips the {@code type} key before
     * dispatch so backends can use either property style at the orchestrator
     * boundary.
     */
    public static Catalog buildByImpl(IcebergBackendContext context, String catalogImplFqcn) {
        Map<String, String> props = new HashMap<>(context.properties());
        props.put(CatalogProperties.CATALOG_IMPL, catalogImplFqcn);
        props.remove(CatalogUtil.ICEBERG_CATALOG_TYPE);
        return CatalogUtil.buildIcebergCatalog(
                context.catalogName(), props, context.hadoopConf());
    }

    /**
     * List branches and tags on {@code database.table} via the supplied
     * {@link Catalog}. Each entry in {@code table.refs()} is converted to a
     * {@link ConnectorRef} with its {@link RefKind} derived from
     * {@link SnapshotRef#isBranch()} / {@link SnapshotRef#isTag()}.
     */
    public static List<ConnectorRef> listRefs(Catalog catalog, String database, String table) {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(database, "database");
        Objects.requireNonNull(table, "table");
        Table tbl = catalog.loadTable(TableIdentifier.of(database, table));
        Map<String, SnapshotRef> refs = tbl.refs();
        List<ConnectorRef> out = new ArrayList<>(refs.size());
        for (Map.Entry<String, SnapshotRef> entry : refs.entrySet()) {
            SnapshotRef ref = entry.getValue();
            RefKind kind = ref.isBranch() ? RefKind.BRANCH
                    : ref.isTag() ? RefKind.TAG
                    : RefKind.UNKNOWN;
            ConnectorRef.Builder b = ConnectorRef.builder()
                    .name(entry.getKey())
                    .kind(kind)
                    .snapshotId(ref.snapshotId());
            Snapshot snap = tbl.snapshot(ref.snapshotId());
            if (snap != null) {
                b.createdAt(Instant.ofEpochMilli(snap.timestampMillis()));
            }
            out.add(b.build());
        }
        return out;
    }

    /**
     * Resolve a {@link ConnectorTableVersion} to the Iceberg {@link Snapshot}
     * on {@code database.table} via the supplied {@link Catalog}. The
     * {@code instanceof} matrix covers every sealed subtype.
     */
    public static Snapshot resolveVersion(Catalog catalog,
                                          String database,
                                          String table,
                                          ConnectorTableVersion version) {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(database, "database");
        Objects.requireNonNull(table, "table");
        if (version == null) {
            throw new IcebergBackendException(
                    "ConnectorTableVersion is null for table " + database + "." + table);
        }
        Table tbl = catalog.loadTable(TableIdentifier.of(database, table));
        if (version instanceof ConnectorTableVersion.BySnapshotId bySnap) {
            return requireSnapshot(tbl, bySnap.snapshotId(), database, table,
                    "snapshot id " + bySnap.snapshotId());
        }
        if (version instanceof ConnectorTableVersion.ByTimestamp byTs) {
            long id = SnapshotUtil.snapshotIdAsOfTime(tbl, byTs.ts().toEpochMilli());
            return requireSnapshot(tbl, id, database, table,
                    "timestamp " + byTs.ts());
        }
        if (version instanceof ConnectorTableVersion.ByRef byRef) {
            return resolveRef(tbl, byRef.name(), byRef.kind(), database, table);
        }
        if (version instanceof ConnectorTableVersion.ByRefAtTimestamp byRefAt) {
            // Pin to the named ref first (for validation) then resolve by timestamp.
            resolveRef(tbl, byRefAt.name(), byRefAt.kind(), database, table);
            long id = SnapshotUtil.snapshotIdAsOfTime(tbl, byRefAt.ts().toEpochMilli());
            return requireSnapshot(tbl, id, database, table,
                    "ref '" + byRefAt.name() + "' at timestamp " + byRefAt.ts());
        }
        if (version instanceof ConnectorTableVersion.ByOpaque) {
            throw new UnsupportedOperationException(
                    "opaque MVCC tokens are not yet supported for iceberg"
                            + " (table " + database + "." + table + ")");
        }
        throw new IcebergBackendException(
                "unknown ConnectorTableVersion subtype "
                        + version.getClass().getName()
                        + " on table " + database + "." + table);
    }

    private static Snapshot resolveRef(Table tbl,
                                       String refName,
                                       RefKind expectedKind,
                                       String database,
                                       String table) {
        SnapshotRef ref = tbl.refs().get(refName);
        if (ref == null) {
            throw new IcebergBackendException(
                    "iceberg ref '" + refName + "' not found on table "
                            + database + "." + table);
        }
        if (expectedKind == RefKind.BRANCH && !ref.isBranch()) {
            throw new IcebergBackendException(
                    "iceberg ref '" + refName + "' on table " + database + "." + table
                            + " is not a branch");
        }
        if (expectedKind == RefKind.TAG && !ref.isTag()) {
            throw new IcebergBackendException(
                    "iceberg ref '" + refName + "' on table " + database + "." + table
                            + " is not a tag");
        }
        return requireSnapshot(tbl, ref.snapshotId(), database, table,
                expectedKind.name().toLowerCase() + " '" + refName + "'");
    }

    private static Snapshot requireSnapshot(Table tbl,
                                            long snapshotId,
                                            String database,
                                            String table,
                                            String locator) {
        Snapshot snap = tbl.snapshot(snapshotId);
        if (snap == null) {
            throw new IcebergBackendException(
                    "iceberg snapshot not found for " + locator
                            + " on table " + database + "." + table);
        }
        return snap;
    }

    /**
     * Look up the named ref of the given kind on {@code database.table}. Returns
     * empty when the ref is absent or its kind does not match {@code expectedKind}.
     * Mirrors {@link #listRefs} semantics (kind derived from
     * {@link SnapshotRef#isBranch()} / {@link SnapshotRef#isTag()}).
     */
    public static Optional<ConnectorRef> getRef(Catalog catalog,
                                                String database,
                                                String table,
                                                String name,
                                                RefKind expectedKind) {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(database, "database");
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(expectedKind, "expectedKind");
        Table tbl = catalog.loadTable(TableIdentifier.of(database, table));
        SnapshotRef ref = tbl.refs().get(name);
        if (ref == null) {
            return Optional.empty();
        }
        RefKind actual = ref.isBranch() ? RefKind.BRANCH
                : ref.isTag() ? RefKind.TAG
                : RefKind.UNKNOWN;
        if (actual != expectedKind) {
            return Optional.empty();
        }
        ConnectorRef.Builder b = ConnectorRef.builder()
                .name(name)
                .kind(actual)
                .snapshotId(ref.snapshotId());
        Snapshot snap = tbl.snapshot(ref.snapshotId());
        if (snap != null) {
            b.createdAt(Instant.ofEpochMilli(snap.timestampMillis()));
        }
        return Optional.of(b.build());
    }

    /**
     * Cherry-pick {@code snapshotId} onto the current main branch of
     * {@code database.table}. Surfaces the underlying Iceberg validation
     * error (e.g. unknown snapshot id) directly.
     */
    public static void cherrypickSnapshot(Catalog catalog,
                                          String database,
                                          String table,
                                          long snapshotId) {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(database, "database");
        Objects.requireNonNull(table, "table");
        Table tbl = catalog.loadTable(TableIdentifier.of(database, table));
        tbl.manageSnapshots().cherrypick(snapshotId).commit();
    }

    /**
     * Fast-forward or rewind {@code branch} on {@code database.table} to point
     * at {@code snapshotId}. Surfaces the underlying Iceberg validation error
     * (e.g. unknown snapshot id) directly.
     */
    public static void replaceBranch(Catalog catalog,
                                     String database,
                                     String table,
                                     String branch,
                                     long snapshotId) {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(database, "database");
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(branch, "branch");
        Table tbl = catalog.loadTable(TableIdentifier.of(database, table));
        tbl.manageSnapshots().replaceBranch(branch, snapshotId).commit();
    }
}
