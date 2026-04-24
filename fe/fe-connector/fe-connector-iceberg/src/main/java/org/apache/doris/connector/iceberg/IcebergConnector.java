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
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.iceberg.api.IcebergBackend;
import org.apache.doris.connector.iceberg.api.IcebergBackendContext;
import org.apache.doris.connector.iceberg.api.IcebergBackendFactory;
import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Iceberg connector orchestrator. Manages the lifecycle of an Iceberg SDK
 * {@link Catalog} instance for all metadata operations.
 *
 * <p>Backend selection is delegated to the {@link IcebergBackendRegistry}:
 * the {@code iceberg.catalog.type} property names a backend (hms / rest /
 * glue / dlf / s3tables / hadoop) and the matching factory is loaded via
 * Java ServiceLoader from the {@code fe-connector-iceberg-backend-*} jars
 * shipped in the plugin's {@code lib/}. There is no static type-to-impl
 * cascade in this class.
 *
 * <p>Phase 1 provides read-only metadata operations (list databases, list
 * tables, get schema). Write operations, scan planning, actions
 * (compaction, snapshot management), and transaction support remain in
 * fe-core temporarily.
 */
public class IcebergConnector implements Connector {

    private static final Logger LOG = LogManager.getLogger(IcebergConnector.class);

    private final Map<String, String> properties;
    private final ConnectorContext context;
    private volatile Catalog icebergCatalog;

    public IcebergConnector(Map<String, String> properties, ConnectorContext context) {
        this.properties = Collections.unmodifiableMap(properties);
        this.context = context;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return new IcebergConnectorMetadata(getOrCreateCatalog(), properties);
    }

    private Catalog getOrCreateCatalog() {
        if (icebergCatalog == null) {
            synchronized (this) {
                if (icebergCatalog == null) {
                    icebergCatalog = createCatalog();
                }
            }
        }
        return icebergCatalog;
    }

    private Catalog createCatalog() {
        String catalogType = properties.get(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE);
        if (catalogType == null || catalogType.isEmpty()) {
            throw new DorisConnectorException(
                    "Missing '" + IcebergConnectorProperties.ICEBERG_CATALOG_TYPE + "' property");
        }

        IcebergBackendFactory factory = IcebergBackendRegistry.get(catalogType)
                .orElseThrow(() -> new DorisConnectorException(
                        "Unknown iceberg.catalog.type: '" + catalogType
                                + "'. Available backends on the plugin classpath: "
                                + IcebergBackendRegistry.availableTypes()));

        IcebergBackend backend = factory.create();
        Configuration conf = buildHadoopConf(properties);
        String catalogName = context.getCatalogName();

        LOG.info("Creating Iceberg catalog '{}' via backend '{}'",
                catalogName, backend.name());

        return backend.buildCatalog(new IcebergBackendContext(catalogName, properties, conf));
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
        Catalog c = icebergCatalog;
        if (c != null) {
            if (c instanceof java.io.Closeable) {
                ((java.io.Closeable) c).close();
            }
            icebergCatalog = null;
        }
    }
}
