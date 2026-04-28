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

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.cache.ConnectorMetaCacheBinding;
import org.apache.doris.connector.api.cache.MetaCacheHandle;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.iceberg.api.IcebergBackend;
import org.apache.doris.connector.iceberg.api.IcebergBackendContext;
import org.apache.doris.connector.iceberg.api.IcebergBackendFactory;
import org.apache.doris.connector.iceberg.cache.IcebergCacheBindings;
import org.apache.doris.connector.iceberg.cache.IcebergManifestCacheConfig;
import org.apache.doris.connector.iceberg.cache.IcebergPluginManifestCache;
import org.apache.doris.connector.iceberg.cache.IcebergTableCacheKey;
import org.apache.doris.connector.iceberg.source.IcebergScanPlanProvider;
import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Iceberg connector orchestrator. All metadata caching (catalog SDK
 * instances, loaded {@link Table}s, snapshot lists) is delegated to the
 * fe-core {@code ConnectorMetaCacheRegistry} via the bindings declared in
 * {@link IcebergCacheBindings}; this class no longer maintains private
 * caches of its own.
 *
 * <p>Backend selection is delegated to {@link IcebergBackendRegistry}: the
 * {@code iceberg.catalog.type} property names a backend (hms / rest / glue /
 * dlf / s3tables / hadoop) and the matching factory is loaded via Java
 * ServiceLoader from the {@code fe-connector-iceberg-backend-*} jars shipped
 * in the plugin's {@code lib/}.
 *
 * <p>Phase 1 provides read-only metadata operations; write operations,
 * scan planning and actions remain in fe-core temporarily.
 */
public class IcebergConnector implements Connector {

    private static final Logger LOG = LogManager.getLogger(IcebergConnector.class);

    private final Map<String, String> properties;
    private final ConnectorContext context;
    private final IcebergBackend backend;
    private final IcebergBackendContext backendContext;

    private final ConnectorMetaCacheBinding<String, Catalog> catalogBinding;
    private final ConnectorMetaCacheBinding<IcebergTableCacheKey, Table> tableBinding;
    private final ConnectorMetaCacheBinding<IcebergTableCacheKey, List<Snapshot>> snapshotsBinding;
    private final List<ConnectorMetaCacheBinding<?, ?>> bindings;

    private volatile MetaCacheHandle<String, Catalog> catalogHandle;
    private volatile MetaCacheHandle<IcebergTableCacheKey, Table> tableHandle;
    private volatile MetaCacheHandle<IcebergTableCacheKey, List<Snapshot>> snapshotsHandle;
    private volatile IcebergScanPlanProvider scanPlanProvider;

    private final IcebergManifestCacheConfig manifestCacheConfig;
    private final IcebergPluginManifestCache manifestCache;

    public IcebergConnector(Map<String, String> properties, ConnectorContext context) {
        this.properties = Collections.unmodifiableMap(properties);
        this.context = Objects.requireNonNull(context, "context");

        String catalogType = this.properties.get(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE);
        if (catalogType == null || catalogType.isEmpty()) {
            throw new DorisConnectorException(
                    "Missing '" + IcebergConnectorProperties.ICEBERG_CATALOG_TYPE + "' property");
        }
        IcebergBackendFactory factory = IcebergBackendRegistry.get(catalogType)
                .orElseThrow(() -> new DorisConnectorException(
                        "Unknown iceberg.catalog.type: '" + catalogType
                                + "'. Available backends on the plugin classpath: "
                                + IcebergBackendRegistry.availableTypes()));
        this.backend = factory.create();
        Configuration conf = buildHadoopConf(this.properties);
        this.backendContext = new IcebergBackendContext(
                context.getCatalogName(), this.properties, conf);

        this.catalogBinding = IcebergCacheBindings.catalogBinding(this::loadCatalog);
        this.tableBinding = IcebergCacheBindings.tableBinding(this::loadTable);
        this.snapshotsBinding = IcebergCacheBindings.snapshotsBinding(this::loadSnapshots);
        this.bindings = Collections.unmodifiableList(
                new ArrayList<>(java.util.Arrays.asList(catalogBinding, tableBinding, snapshotsBinding)));

        // Plugin-private manifest cache (D3: cache lives in the plugin,
        // not in fe-core). Always instantiated so toggling the enable
        // flag at runtime would be possible without rebuilding state;
        // the provider only routes through it when the flag is on.
        this.manifestCacheConfig = IcebergManifestCacheConfig.fromProperties(this.properties);
        this.manifestCache = new IcebergPluginManifestCache(manifestCacheConfig.getMaxEntries());
    }

    @Override
    public Set<ConnectorCapability> getCapabilities() {
        return EnumSet.of(ConnectorCapability.SUPPORTS_MTMV,
                ConnectorCapability.SUPPORTS_FILTER_PUSHDOWN,
                ConnectorCapability.SUPPORTS_PROJECTION_PUSHDOWN,
                ConnectorCapability.SUPPORTS_PARTITION_PRUNING,
                ConnectorCapability.SUPPORTS_VENDED_CREDENTIALS,
                ConnectorCapability.SUPPORTS_TIME_TRAVEL,
                ConnectorCapability.SUPPORTS_MVCC_SNAPSHOT,
                // M3-03: WriteOps INSERT + INSERT OVERWRITE (FULL_TABLE /
                // STATIC_PARTITION / DYNAMIC_PARTITION). Branch writes (M3-04)
                // and equality-delete / DV (M3-05) are intentionally excluded.
                ConnectorCapability.SUPPORTS_INSERT,
                ConnectorCapability.SUPPORTS_INSERT_OVERWRITE,
                ConnectorCapability.SUPPORTS_PARTITION_OVERWRITE,
                ConnectorCapability.SUPPORTS_DYNAMIC_PARTITION_INSERT);
    }

    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        IcebergScanPlanProvider local = scanPlanProvider;
        if (local == null) {
            synchronized (this) {
                local = scanPlanProvider;
                if (local == null) {
                    ensureHandles();
                    final MetaCacheHandle<IcebergTableCacheKey, Table> th = tableHandle;
                    local = new IcebergScanPlanProvider(
                            (db, tbl) -> th.get(new IcebergTableCacheKey(db, tbl)),
                            properties,
                            context,
                            manifestCache,
                            manifestCacheConfig.isEnabled());
                    scanPlanProvider = local;
                }
            }
        }
        return local;
    }

    /** Visible for tests: the per-connector plugin manifest cache singleton. */
    public IcebergPluginManifestCache getManifestCache() {
        return manifestCache;
    }

    /** Visible for tests: whether the manifest cache is enabled by catalog properties. */
    public boolean isManifestCacheEnabled() {
        return manifestCacheConfig.isEnabled();
    }

    @Override
    public List<ConnectorMetaCacheBinding<?, ?>> getMetaCacheBindings() {
        return bindings;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        ensureHandles();
        return new IcebergConnectorMetadata(
                catalogHandle.get(context.getCatalogName()),
                properties, backend, backendContext, tableHandle,
                context.getCatalogName());
    }

    /** Visible for tests: returns the catalog instance via the binding handle. */
    public Catalog getOrCreateCatalog() {
        ensureHandles();
        return catalogHandle.get(context.getCatalogName());
    }

    /** Visible for tests: returns the resolved backend (set during construction). */
    public IcebergBackend getBackend() {
        return backend;
    }

    private void ensureHandles() {
        if (catalogHandle == null) {
            synchronized (this) {
                if (catalogHandle == null) {
                    MetaCacheHandle<IcebergTableCacheKey, Table> th =
                            context.getOrCreateCache(tableBinding);
                    MetaCacheHandle<IcebergTableCacheKey, List<Snapshot>> sh =
                            context.getOrCreateCache(snapshotsBinding);
                    MetaCacheHandle<String, Catalog> ch =
                            context.getOrCreateCache(catalogBinding);
                    this.tableHandle = th;
                    this.snapshotsHandle = sh;
                    this.catalogHandle = ch;
                }
            }
        }
    }

    private Catalog loadCatalog(String catalogName) {
        LOG.info("Creating Iceberg catalog '{}' via backend '{}'", catalogName, backend.name());
        return backend.buildCatalog(backendContext);
    }

    private Table loadTable(IcebergTableCacheKey key) {
        Catalog c = catalogHandle != null
                ? catalogHandle.get(context.getCatalogName())
                : getOrCreateCatalog();
        return c.loadTable(TableIdentifier.of(key.getDatabase(), key.getTable()));
    }

    private List<Snapshot> loadSnapshots(IcebergTableCacheKey key) {
        Table t = tableHandle != null ? tableHandle.get(key) : loadTable(key);
        List<Snapshot> out = new ArrayList<>();
        for (Snapshot s : t.snapshots()) {
            out.add(s);
        }
        return out;
    }

    private static Configuration buildHadoopConf(Map<String, String> props) {
        Configuration conf = new Configuration();
        for (Map.Entry<String, String> entry : props.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("hadoop.") || key.startsWith("fs.")
                    || key.startsWith("dfs.") || key.startsWith("hive.")) {
                conf.set(key, entry.getValue());
            }
        }
        return conf;
    }

    @Override
    public void close() throws IOException {
        // Triggers the catalog binding's RemovalListener which closes the SDK Catalog.
        if (catalogHandle != null) {
            catalogHandle.invalidateAll();
        }
        manifestCache.invalidateAll();
    }
}
