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

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;

import java.util.List;

/**
 * Plugin-internal SPI implemented by every Paimon backend variant
 * (filesystem, hms, rest, aliyun-dlf). The orchestrator dispatches to the
 * matching backend based on the {@code paimon.catalog.type} property.
 *
 * <p>Implementations live in their own jars (one per {@code
 * fe-connector-paimon-backend-<name>} module) and are discovered via
 * {@link PaimonBackendFactory} ServiceLoader registrations. Backend jars
 * isolate their per-vendor Paimon SDK dependencies (e.g. {@code
 * paimon-hive-connector-3.1} for hms; alibaba-cloud DLF metastore client
 * for aliyun-dlf) so a Doris install that only uses filesystem or rest does
 * not have to ship the Hive bundle.
 */
public interface PaimonBackend {

    /**
     * Canonical lowercase backend identifier matching the value of the
     * {@code paimon.catalog.type} property (e.g. {@code "filesystem"},
     * {@code "hms"}, {@code "rest"}, {@code "aliyun-dlf"}).
     */
    String name();

    /**
     * Build the Paimon SDK {@link Catalog} for this backend.
     */
    Catalog buildCatalog(PaimonBackendContext context);

    /**
     * List all branches and tags currently known for the given table.
     *
     * <p>Default implementation opens the backend's {@link Catalog} via
     * {@link #buildCatalog(PaimonBackendContext)} and delegates to
     * {@link PaimonBackendSupport#listRefs(Catalog, String, String)}. The
     * {@code aliyun-dlf} backend's {@code buildCatalog} throws
     * {@link PaimonBackendException} (stub since M1-05), so {@code aliyun-dlf}
     * automatically surfaces the same failure here — no explicit override
     * needed for stubs. Real backends may override if they already cache a
     * {@link Catalog} instance.
     *
     * <p><strong>Paimon branch semantics:</strong> a paimon branch is a
     * schema-level isolated metadata tree, not a pointer into a shared
     * snapshot timeline (cf. iceberg). Listing therefore reports each
     * branch with {@code snapshotId == -1} (paimon does not expose a
     * branch-head snapshot id without opening the branch). Tags carry
     * their concrete snapshot id and creation time as in iceberg.
     */
    default List<ConnectorRef> listRefs(PaimonBackendContext context,
                                        String database,
                                        String table) {
        return PaimonBackendSupport.listRefs(buildCatalog(context), database, table);
    }

    /**
     * Resolve a {@link ConnectorTableVersion} to the concrete Paimon
     * {@link Snapshot} on the given table. Returns the snapshot chosen for
     * time-travel reads; the caller is responsible for pinning subsequent
     * scan planning to that snapshot id.
     *
     * <p>Default implementation opens the backend's {@link Catalog} via
     * {@link #buildCatalog(PaimonBackendContext)} and delegates to
     * {@link PaimonBackendSupport#resolveVersion(Catalog, String, String, ConnectorTableVersion)}.
     * Opaque MVCC tokens surface as {@link UnsupportedOperationException};
     * null / unknown subtypes as {@link PaimonBackendException}.
     *
     * <p><strong>Paimon branch semantics:</strong> for {@code BRANCH} refs
     * the resolution invokes {@code DataTable.switchToBranch(name)} and then
     * reads the latest snapshot of the resulting (independent) timeline.
     * The returned snapshot id is therefore branch-local; callers that need
     * to pin a scan to that branch must also propagate the branch name
     * (paimon scans take the branch via the {@code scan.snapshot-id} option
     * combined with {@code branch}) — the SPI surface only exposes the
     * snapshot id today; branch-name plumbing is tracked as a follow-up.
     * Branch-rooted writes are subject to paimon's schema-level branch
     * restrictions and are intentionally <em>not</em> exposed via {@code
     * RefOps.createOrReplaceRef}/{@code dropRef} in M1-08 (read-only).
     */
    default Snapshot resolveVersion(PaimonBackendContext context,
                                    String database,
                                    String table,
                                    ConnectorTableVersion version) {
        return PaimonBackendSupport.resolveVersion(
                buildCatalog(context), database, table, version);
    }
}
