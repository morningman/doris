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

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.Catalog;

import java.util.List;

/**
 * Plugin-internal SPI implemented by every Iceberg backend variant (hms,
 * rest, glue, dlf, s3tables, hadoop). The orchestrator dispatches to the
 * matching backend based on the {@code iceberg.catalog.type} property.
 *
 * <p>Implementations live in their own jars (one per {@code
 * fe-connector-iceberg-backend-<name>} module) and are discovered via
 * {@link IcebergBackendFactory} ServiceLoader registrations. Backend jars
 * isolate their per-vendor Iceberg SDK dependencies (e.g. {@code
 * iceberg-aws} for glue / s3tables) so a Doris install that only uses REST
 * or HMS does not have to ship the AWS bundle.
 */
public interface IcebergBackend {

    /**
     * Canonical lowercase backend identifier matching the value of the
     * {@code iceberg.catalog.type} property (e.g. {@code "hms"},
     * {@code "rest"}, {@code "glue"}).
     */
    String name();

    /**
     * Build the Iceberg SDK {@link Catalog} for this backend.
     */
    Catalog buildCatalog(IcebergBackendContext context);

    /**
     * List all branches and tags currently known for the given table.
     *
     * <p>Default implementation opens the backend's {@link Catalog} via
     * {@link #buildCatalog(IcebergBackendContext)} and delegates to
     * {@link IcebergBackendSupport#listRefs(Catalog, String, String)}. The
     * {@code dlf} backend's {@code buildCatalog} throws
     * {@link IcebergBackendException} (stub since M1-04), so {@code dlf}
     * automatically surfaces the same failure here — no explicit override
     * needed for stubs. Real backends may override if they already cache a
     * {@link Catalog} instance.
     */
    default List<ConnectorRef> listRefs(IcebergBackendContext context,
                                        String database,
                                        String table) {
        return IcebergBackendSupport.listRefs(buildCatalog(context), database, table);
    }

    /**
     * Resolve a {@link ConnectorTableVersion} to the concrete Iceberg
     * {@link Snapshot} on the given table. Returns the snapshot chosen for
     * time-travel reads; the caller is responsible for pinning subsequent
     * scan planning to that snapshot id.
     *
     * <p>Default implementation opens the backend's {@link Catalog} via
     * {@link #buildCatalog(IcebergBackendContext)} and delegates to
     * {@link IcebergBackendSupport#resolveVersion(Catalog, String, String, ConnectorTableVersion)}.
     * Opaque MVCC tokens surface as {@link UnsupportedOperationException};
     * null / unknown subtypes as {@link IcebergBackendException}.
     */
    default Snapshot resolveVersion(IcebergBackendContext context,
                                    String database,
                                    String table,
                                    ConnectorTableVersion version) {
        return IcebergBackendSupport.resolveVersion(
                buildCatalog(context), database, table, version);
    }
}
