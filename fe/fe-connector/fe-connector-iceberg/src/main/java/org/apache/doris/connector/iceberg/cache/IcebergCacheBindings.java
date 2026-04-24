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

package org.apache.doris.connector.iceberg.cache;

import org.apache.doris.connector.api.cache.CacheLoader;
import org.apache.doris.connector.api.cache.ConnectorCacheSpec;
import org.apache.doris.connector.api.cache.ConnectorMetaCacheBinding;
import org.apache.doris.connector.api.cache.ConnectorMetaCacheInvalidation;
import org.apache.doris.connector.api.cache.InvalidateScope;
import org.apache.doris.connector.api.cache.RefreshPolicy;
import org.apache.doris.connector.api.cache.RemovalListener;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Iceberg connector cache binding declarations. Each binding describes a
 * cache instance owned by the fe-core {@code ConnectorMetaCacheRegistry} on
 * behalf of a single {@code IcebergConnector}. The plugin overrides
 * {@code Connector.getMetaCacheBindings()} (M1-09 wire) so fe-core registers
 * the bindings during connector init; the plugin then obtains a
 * {@code MetaCacheHandle} per binding via
 * {@code ConnectorContext.getOrCreateCache}.
 *
 * <p>Three bindings are declared:
 * <ul>
 *   <li>{@link #ENTRY_CATALOG}: keyed by catalog name, value is the
 *       {@link Catalog} SDK instance. Singleton-style (max size 4 to tolerate
 *       transient duplicate keys), no TTL ({@link RefreshPolicy#MANUAL_ONLY}).
 *       Reacts only to {@link InvalidateScope#CATALOG} so per-table refreshes
 *       do not destroy the SDK handle. A {@link RemovalListener} closes the
 *       underlying catalog on eviction if it implements {@link Closeable}.</li>
 *   <li>{@link #ENTRY_TABLE}: keyed by {@link IcebergTableCacheKey}, value is
 *       a loaded {@link Table}. Default capacity 10_000, 1h TTL. Reacts to
 *       {@code CATALOG}, {@code DATABASE}, {@code TABLE} scopes.</li>
 *   <li>{@link #ENTRY_SNAPSHOTS}: keyed by {@link IcebergTableCacheKey},
 *       value is the materialized snapshot list. Capacity 10_000, 5min TTL
 *       (snapshot lists drift faster than schema). Reacts to {@code CATALOG},
 *       {@code DATABASE}, {@code TABLE}.</li>
 * </ul>
 *
 * <p>Per M0-07 / M1-09 the registry currently performs bulk invalidation per
 * binding. Once a key-adapter is added to {@link ConnectorMetaCacheBinding},
 * the table / snapshots bindings should grow one so a {@code TABLE}-scoped
 * invalidate evicts only the matching key.
 */
public final class IcebergCacheBindings {

    private static final Logger LOG = LogManager.getLogger(IcebergCacheBindings.class);

    public static final String ENTRY_CATALOG = "iceberg.catalog";
    public static final String ENTRY_TABLE = "iceberg.table";
    public static final String ENTRY_SNAPSHOTS = "iceberg.snapshots";

    private IcebergCacheBindings() {
    }

    /**
     * Build the {@link #ENTRY_CATALOG} binding. The loader is invoked at most
     * once per catalog name since the registry caps {@code maxSize=4}; on
     * removal the value is closed if it implements {@link Closeable}.
     */
    public static ConnectorMetaCacheBinding<String, Catalog> catalogBinding(
            CacheLoader<String, Catalog> loader) {
        Objects.requireNonNull(loader, "loader");
        ConnectorCacheSpec spec = ConnectorCacheSpec.builder()
                .maxSize(4L)
                .ttl(Duration.ofDays(36500))
                .refreshPolicy(RefreshPolicy.MANUAL_ONLY)
                .softValues(false)
                .build();
        return ConnectorMetaCacheBinding
                .<String, Catalog>builder(ENTRY_CATALOG, String.class, Catalog.class, loader)
                .defaultSpec(spec)
                .invalidationStrategy(ConnectorMetaCacheInvalidation.byScope(InvalidateScope.CATALOG))
                .removalListener(closingRemovalListener())
                .build();
    }

    /** Build the {@link #ENTRY_TABLE} binding. */
    public static ConnectorMetaCacheBinding<IcebergTableCacheKey, Table> tableBinding(
            CacheLoader<IcebergTableCacheKey, Table> loader) {
        Objects.requireNonNull(loader, "loader");
        ConnectorCacheSpec spec = ConnectorCacheSpec.builder()
                .maxSize(10_000L)
                .ttl(Duration.ofHours(1))
                .refreshPolicy(RefreshPolicy.TTL)
                .build();
        return ConnectorMetaCacheBinding
                .<IcebergTableCacheKey, Table>builder(
                        ENTRY_TABLE, IcebergTableCacheKey.class, Table.class, loader)
                .defaultSpec(spec)
                .invalidationStrategy(perTableScope())
                .build();
    }

    /** Build the {@link #ENTRY_SNAPSHOTS} binding. */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static ConnectorMetaCacheBinding<IcebergTableCacheKey, List<Snapshot>> snapshotsBinding(
            CacheLoader<IcebergTableCacheKey, List<Snapshot>> loader) {
        Objects.requireNonNull(loader, "loader");
        ConnectorCacheSpec spec = ConnectorCacheSpec.builder()
                .maxSize(10_000L)
                .ttl(Duration.ofMinutes(5))
                .refreshPolicy(RefreshPolicy.TTL)
                .build();
        Class raw = List.class;
        Class<List<Snapshot>> valueType = (Class<List<Snapshot>>) raw;
        return ConnectorMetaCacheBinding
                .<IcebergTableCacheKey, List<Snapshot>>builder(
                        ENTRY_SNAPSHOTS, IcebergTableCacheKey.class, valueType, loader)
                .defaultSpec(spec)
                .invalidationStrategy(perTableScope())
                .build();
    }

    private static ConnectorMetaCacheInvalidation perTableScope() {
        return req -> {
            InvalidateScope s = req.getScope();
            return s == InvalidateScope.CATALOG
                    || s == InvalidateScope.DATABASE
                    || s == InvalidateScope.TABLE;
        };
    }

    private static <K> RemovalListener<K, Catalog> closingRemovalListener() {
        return (key, value, cause) -> {
            if (value instanceof Closeable) {
                try {
                    ((Closeable) value).close();
                } catch (Exception e) {
                    LOG.warn("Failed to close iceberg catalog '{}' on cache removal: {}",
                            key, e.toString());
                }
            }
        };
    }
}
