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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.cache.ConnectorMetaCacheBinding;
import org.apache.doris.connector.api.cache.MetaCacheHandle;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.paimon.api.PaimonBackend;
import org.apache.doris.connector.paimon.api.PaimonBackendContext;
import org.apache.doris.connector.paimon.api.PaimonBackendFactory;
import org.apache.doris.connector.paimon.cache.PaimonCacheBindings;
import org.apache.doris.connector.paimon.cache.PaimonTableCacheKey;
import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.SnapshotManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Paimon connector orchestrator. All metadata caching (catalog SDK
 * instances, loaded {@link Table}s, snapshot lists) is delegated to the
 * fe-core {@code ConnectorMetaCacheRegistry} via the bindings declared in
 * {@link PaimonCacheBindings}; this class no longer maintains private
 * caches of its own.
 *
 * <p>Backend selection is delegated to the {@link PaimonBackendRegistry}:
 * the {@code paimon.catalog.type} property names a backend (filesystem /
 * hms / rest / aliyun-dlf) and the matching factory is loaded via Java
 * ServiceLoader from the {@code fe-connector-paimon-backend-*} jars
 * shipped in the plugin's {@code lib/}. There is no static type-to-impl
 * cascade in this class.
 */
public class PaimonConnector implements Connector {

    private static final Logger LOG = LogManager.getLogger(PaimonConnector.class);

    private final Map<String, String> properties;
    private final ConnectorContext context;
    private final PaimonBackend backend;
    private final PaimonBackendContext backendContext;

    private final ConnectorMetaCacheBinding<String, Catalog> catalogBinding;
    private final ConnectorMetaCacheBinding<PaimonTableCacheKey, Table> tableBinding;
    private final ConnectorMetaCacheBinding<PaimonTableCacheKey, List<Snapshot>> snapshotsBinding;
    private final List<ConnectorMetaCacheBinding<?, ?>> bindings;

    private volatile MetaCacheHandle<String, Catalog> catalogHandle;
    private volatile MetaCacheHandle<PaimonTableCacheKey, Table> tableHandle;
    private volatile MetaCacheHandle<PaimonTableCacheKey, List<Snapshot>> snapshotsHandle;

    public PaimonConnector(Map<String, String> properties, ConnectorContext context) {
        this.properties = Collections.unmodifiableMap(properties);
        this.context = Objects.requireNonNull(context, "context");

        String catalogType = this.properties.getOrDefault(
                PaimonConnectorProperties.PAIMON_CATALOG_TYPE,
                PaimonConnectorProperties.DEFAULT_CATALOG_TYPE);

        PaimonBackendFactory factory = PaimonBackendRegistry.get(catalogType)
                .orElseThrow(() -> new DorisConnectorException(
                        "Unknown paimon.catalog.type: '" + catalogType
                                + "'. Available backends on the plugin classpath: "
                                + PaimonBackendRegistry.availableTypes()));
        this.backend = factory.create();
        this.backendContext = new PaimonBackendContext(context.getCatalogName(), this.properties);

        this.catalogBinding = PaimonCacheBindings.catalogBinding(this::loadCatalog);
        this.tableBinding = PaimonCacheBindings.tableBinding(this::loadTable);
        this.snapshotsBinding = PaimonCacheBindings.snapshotsBinding(this::loadSnapshots);
        this.bindings = Collections.unmodifiableList(
                new ArrayList<>(Arrays.asList(catalogBinding, tableBinding, snapshotsBinding)));
    }

    @Override
    public List<ConnectorMetaCacheBinding<?, ?>> getMetaCacheBindings() {
        return bindings;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        ensureHandles();
        return new PaimonConnectorMetadata(
                catalogHandle.get(context.getCatalogName()),
                properties, backend, backendContext, tableHandle);
    }

    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        return new PaimonScanPlanProvider(properties);
    }

    /** Visible for tests: returns the catalog instance via the binding handle. */
    public Catalog getOrCreateCatalog() {
        ensureHandles();
        return catalogHandle.get(context.getCatalogName());
    }

    /** Visible for tests: returns the resolved backend (set during construction). */
    public PaimonBackend getBackend() {
        return backend;
    }

    private void ensureHandles() {
        if (catalogHandle == null) {
            synchronized (this) {
                if (catalogHandle == null) {
                    MetaCacheHandle<PaimonTableCacheKey, Table> th =
                            context.getOrCreateCache(tableBinding);
                    MetaCacheHandle<PaimonTableCacheKey, List<Snapshot>> sh =
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
        LOG.info("Creating Paimon catalog '{}' via backend '{}'", catalogName, backend.name());
        return backend.buildCatalog(backendContext);
    }

    private Table loadTable(PaimonTableCacheKey key) {
        Catalog c = catalogHandle != null
                ? catalogHandle.get(context.getCatalogName())
                : getOrCreateCatalog();
        Identifier id = Identifier.create(key.getDatabase(), key.getTable());
        try {
            return c.getTable(id);
        } catch (Catalog.TableNotExistException e) {
            throw new DorisConnectorException(
                    "paimon table not found: " + id, e);
        }
    }

    private List<Snapshot> loadSnapshots(PaimonTableCacheKey key) {
        Table t = tableHandle != null ? tableHandle.get(key) : loadTable(key);
        if (!(t instanceof DataTable)) {
            return Collections.emptyList();
        }
        SnapshotManager sm = ((DataTable) t).snapshotManager();
        try {
            return sm.safelyGetAllSnapshots();
        } catch (IOException e) {
            throw new DorisConnectorException(
                    "failed to list paimon snapshots for " + key, e);
        }
    }

    @Override
    public void close() throws IOException {
        // Triggers the catalog binding's RemovalListener which closes the SDK Catalog.
        if (catalogHandle != null) {
            catalogHandle.invalidateAll();
        }
    }
}
