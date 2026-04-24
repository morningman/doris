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
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.paimon.api.PaimonBackend;
import org.apache.doris.connector.paimon.api.PaimonBackendContext;
import org.apache.doris.connector.paimon.api.PaimonBackendFactory;
import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.Catalog;

import java.io.IOException;
import java.util.Map;

/**
 * Paimon connector orchestrator. Manages the lifecycle of a Paimon SDK
 * {@link Catalog} instance for all metadata operations.
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
    private volatile Catalog catalog;

    public PaimonConnector(Map<String, String> properties, ConnectorContext context) {
        this.properties = properties;
        this.context = context;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return new PaimonConnectorMetadata(ensureCatalog(), properties);
    }

    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        return new PaimonScanPlanProvider(properties);
    }

    private Catalog ensureCatalog() {
        if (catalog == null) {
            synchronized (this) {
                if (catalog == null) {
                    catalog = createCatalog();
                }
            }
        }
        return catalog;
    }

    private Catalog createCatalog() {
        String catalogType = properties.getOrDefault(
                PaimonConnectorProperties.PAIMON_CATALOG_TYPE,
                PaimonConnectorProperties.DEFAULT_CATALOG_TYPE);

        PaimonBackendFactory factory = PaimonBackendRegistry.get(catalogType)
                .orElseThrow(() -> new DorisConnectorException(
                        "Unknown paimon.catalog.type: '" + catalogType
                                + "'. Available backends on the plugin classpath: "
                                + PaimonBackendRegistry.availableTypes()));

        PaimonBackend backend = factory.create();
        String catalogName = context.getCatalogName();

        LOG.info("Creating Paimon catalog '{}' via backend '{}'",
                catalogName, backend.name());

        return backend.buildCatalog(new PaimonBackendContext(catalogName, properties));
    }

    @Override
    public void close() throws IOException {
        Catalog cat = catalog;
        if (cat != null) {
            try {
                cat.close();
            } catch (Exception e) {
                LOG.warn("Failed to close Paimon catalog", e);
            }
            catalog = null;
        }
    }
}
