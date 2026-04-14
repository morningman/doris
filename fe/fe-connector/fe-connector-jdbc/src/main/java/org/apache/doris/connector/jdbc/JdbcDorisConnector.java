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

package org.apache.doris.connector.jdbc;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTestResult;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.jdbc.client.JdbcConnectorClient;
import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JDBC connector implementation. Manages the lifecycle of
 * {@link JdbcConnectorClient} (HikariCP data source, JDBC driver classloader).
 */
public class JdbcDorisConnector implements Connector {

    private static final Logger LOG = LogManager.getLogger(JdbcDorisConnector.class);

    private final Map<String, String> properties;
    private final ConnectorContext context;
    private volatile JdbcConnectorClient client;
    private volatile JdbcScanPlanProvider scanPlanProvider;

    static final String JDBC_PROPERTIES_PREFIX = "jdbc.";

    public JdbcDorisConnector(Map<String, String> properties, ConnectorContext context) {
        Map<String, String> normalized = new HashMap<>();
        // Strip "jdbc." prefix from property keys for backward compatibility.
        // Users may write "jdbc.jdbc_url" or "jdbc.user" in CREATE CATALOG;
        // internally we use the short form ("jdbc_url", "user").
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(JDBC_PROPERTIES_PREFIX)) {
                String shortKey = key.substring(JDBC_PROPERTIES_PREFIX.length());
                // Only strip if the short key is not already present
                if (!properties.containsKey(shortKey)) {
                    normalized.put(shortKey, entry.getValue());
                    continue;
                }
            }
            normalized.put(key, entry.getValue());
        }
        String rawUrl = normalized.get(JdbcConnectorProperties.JDBC_URL);
        if (rawUrl != null && !rawUrl.isEmpty()) {
            JdbcDbType dbType = JdbcDbType.parseFromUrl(rawUrl);
            normalized.put(JdbcConnectorProperties.JDBC_URL,
                    JdbcUrlNormalizer.normalize(rawUrl, dbType));
        }
        this.properties = Collections.unmodifiableMap(normalized);
        this.context = context;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return new JdbcConnectorMetadata(getOrCreateClient(), properties);
    }

    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        if (scanPlanProvider == null) {
            synchronized (this) {
                if (scanPlanProvider == null) {
                    // Use client's effective dbType instead of static URL parsing,
                    // so OceanBase Oracle mode is detected correctly
                    JdbcDbType dbType = getOrCreateClient().getDbType();
                    scanPlanProvider = new JdbcScanPlanProvider(
                            dbType, properties, context.getCatalogId());
                }
            }
        }
        return scanPlanProvider;
    }

    @Override
    public ConnectorTestResult testConnection(ConnectorSession session) {
        try {
            JdbcConnectorClient c = getOrCreateClient();
            List<String> dbs = c.getDatabaseNameList();
            LOG.info("JDBC connection test succeeded, found {} databases", dbs.size());
            return ConnectorTestResult.success(
                    "Connected successfully, found " + dbs.size() + " databases");
        } catch (Exception e) {
            LOG.warn("JDBC connection test failed", e);
            return ConnectorTestResult.failure("JDBC connection failed: " + e.getMessage());
        }
    }

    private JdbcConnectorClient getOrCreateClient() {
        if (client == null) {
            synchronized (this) {
                if (client == null) {
                    client = createClient();
                }
            }
        }
        return client;
    }

    private JdbcConnectorClient createClient() {
        String jdbcUrl = properties.get(JdbcConnectorProperties.JDBC_URL);
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            throw new DorisConnectorException("JDBC URL ('" + JdbcConnectorProperties.JDBC_URL + "') is required");
        }
        JdbcDbType dbType = JdbcDbType.parseFromUrl(jdbcUrl);
        String user = properties.getOrDefault(JdbcConnectorProperties.USER, "");
        String password = properties.getOrDefault(JdbcConnectorProperties.PASSWORD, "");
        String driverUrl = resolveDriverUrl(properties.get(JdbcConnectorProperties.DRIVER_URL));
        String driverClass = properties.get(JdbcConnectorProperties.DRIVER_CLASS);
        int poolMinSize = JdbcConnectorProperties.getInt(
                properties, JdbcConnectorProperties.CONNECTION_POOL_MIN_SIZE,
                JdbcConnectorProperties.DEFAULT_POOL_MIN_SIZE);
        int poolMaxSize = JdbcConnectorProperties.getInt(
                properties, JdbcConnectorProperties.CONNECTION_POOL_MAX_SIZE,
                JdbcConnectorProperties.DEFAULT_POOL_MAX_SIZE);
        int poolMaxWaitTime = JdbcConnectorProperties.getInt(
                properties, JdbcConnectorProperties.CONNECTION_POOL_MAX_WAIT_TIME,
                JdbcConnectorProperties.DEFAULT_POOL_MAX_WAIT_TIME);
        int poolMaxLifeTime = JdbcConnectorProperties.getInt(
                properties, JdbcConnectorProperties.CONNECTION_POOL_MAX_LIFE_TIME,
                JdbcConnectorProperties.DEFAULT_POOL_MAX_LIFE_TIME);
        boolean onlySpecifiedDatabase = Boolean.parseBoolean(
                properties.getOrDefault(JdbcConnectorProperties.ONLY_SPECIFIED_DATABASE, "false"));
        boolean enableMappingVarbinary = Boolean.parseBoolean(
                properties.getOrDefault(JdbcConnectorProperties.ENABLE_MAPPING_VARBINARY, "false"));
        boolean enableMappingTimestampTz = Boolean.parseBoolean(
                properties.getOrDefault(JdbcConnectorProperties.ENABLE_MAPPING_TIMESTAMP_TZ, "false"));

        LOG.info("Creating JDBC connector client for dbType={}, url={}", dbType, jdbcUrl);
        return JdbcConnectorClient.create(
                dbType, context.getCatalogName(), jdbcUrl, user, password,
                driverUrl, driverClass,
                poolMinSize, poolMaxSize, poolMaxWaitTime, poolMaxLifeTime,
                onlySpecifiedDatabase, properties,
                enableMappingVarbinary, enableMappingTimestampTz);
    }

    @Override
    public void close() throws IOException {
        JdbcConnectorClient c = client;
        if (c != null) {
            c.close();
            client = null;
        }
    }

    /**
     * Resolves driver URL using the environment from ConnectorContext.
     * If the URL is a plain filename (e.g., "mysql-connector-j-8.4.0.jar"),
     * resolves it using the jdbc_drivers_dir from the environment.
     */
    private String resolveDriverUrl(String driverUrl) {
        if (driverUrl == null || driverUrl.isEmpty()) {
            return driverUrl;
        }
        if (driverUrl.startsWith("file://") || driverUrl.startsWith("http://")
                || driverUrl.startsWith("https://") || driverUrl.startsWith("/")) {
            return driverUrl;
        }
        // Plain filename — resolve using jdbc_drivers_dir from environment
        Map<String, String> env = context.getEnvironment();
        String driversDir = env.get("jdbc_drivers_dir");
        if (driversDir != null && !driversDir.isEmpty()) {
            String resolved = "file://" + driversDir + "/" + driverUrl;
            LOG.info("Resolved driver_url '{}' to '{}' using jdbc_drivers_dir", driverUrl, resolved);
            return resolved;
        }
        return "file://" + driverUrl;
    }
}
