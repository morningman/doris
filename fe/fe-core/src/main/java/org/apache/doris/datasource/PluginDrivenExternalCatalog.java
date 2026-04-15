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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.JdbcResource;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.connector.ConnectorFactory;
import org.apache.doris.connector.ConnectorSessionBuilder;
import org.apache.doris.connector.DefaultConnectorContext;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTestResult;
import org.apache.doris.proto.InternalService.PJdbcTestConnectionRequest;
import org.apache.doris.proto.InternalService.PJdbcTestConnectionResult;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TJdbcTable;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TOdbcTableType;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;
import org.apache.doris.transaction.PluginDrivenTransactionManager;

import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * An {@link ExternalCatalog} backed by a Connector SPI plugin.
 *
 * <p>This adapter bridges the connector SPI ({@link Connector}) with the existing
 * ExternalCatalog hierarchy. Metadata operations are delegated to the connector's
 * {@link org.apache.doris.connector.api.ConnectorMetadata} implementation.</p>
 *
 * <p>When created via {@link CatalogFactory}, the Connector instance is provided
 * directly. After GSON deserialization (FE restart), the Connector is recreated
 * from catalog properties during {@link #initLocalObjectsImpl()}.</p>
 */
public class PluginDrivenExternalCatalog extends ExternalCatalog {

    private static final Logger LOG = LogManager.getLogger(PluginDrivenExternalCatalog.class);

    // Volatile for cross-thread visibility; all mutations also happen under synchronized(this).
    private transient volatile Connector connector;

    // When true, onClose() skips closing the connector because notifyPropertiesUpdated()
    // manages the connector lifecycle (create-before-swap pattern).
    private transient volatile boolean connectorSwapInProgress = false;

    /** No-arg constructor for GSON deserialization. */
    public PluginDrivenExternalCatalog() {
    }

    /**
     * Creates a plugin-driven catalog with an already-created Connector.
     *
     * @param catalogId unique catalog id
     * @param name catalog name
     * @param resource optional resource name
     * @param props catalog properties
     * @param comment catalog comment
     * @param connector the SPI connector instance
     */
    public PluginDrivenExternalCatalog(long catalogId, String name, String resource,
            Map<String, String> props, String comment, Connector connector) {
        super(catalogId, name, InitCatalogLog.Type.PLUGIN, comment);
        this.catalogProperty = new CatalogProperty(resource, props);
        this.connector = connector;
    }

    @Override
    protected void initLocalObjectsImpl() {
        if (connector != null) {
            transactionManager = new PluginDrivenTransactionManager();
            initPreExecutionAuthenticator();
            return;
        }
        connector = createConnectorFromProperties();
        if (connector == null) {
            String catalogType = catalogProperty.getOrDefault(CatalogMgr.CATALOG_TYPE_PROP, "");
            throw new RuntimeException("No ConnectorProvider found for plugin-driven catalog: "
                    + name + ", type: " + catalogType
                    + ". Ensure the connector plugin is installed.");
        }
        LOG.info("Recreated connector for plugin-driven catalog {}:{}, type={}",
                name, id, catalogProperty.getOrDefault(CatalogMgr.CATALOG_TYPE_PROP, ""));
        transactionManager = new PluginDrivenTransactionManager();
        initPreExecutionAuthenticator();
    }

    /**
     * Creates a new Connector from catalog properties. Extracted as a protected method
     * so tests can override without depending on the static ConnectorFactory registry.
     */
    protected Connector createConnectorFromProperties() {
        String catalogType = catalogProperty.getOrDefault(CatalogMgr.CATALOG_TYPE_PROP, "");
        return ConnectorFactory.createConnector(catalogType,
                catalogProperty.getProperties(),
                new DefaultConnectorContext(name, id));
    }

    @Override
    public void checkProperties() throws DdlException {
        super.checkProperties();
        String catalogType = catalogProperty.getOrDefault(CatalogMgr.CATALOG_TYPE_PROP, "");
        try {
            ConnectorFactory.validateProperties(catalogType, catalogProperty.getProperties());
        } catch (IllegalArgumentException e) {
            throw new DdlException(e.getMessage());
        }
        // Validate function_rules JSON if present (shared across all connector types).
        String functionRules = catalogProperty.getOrDefault("function_rules", null);
        ExternalFunctionRules.check(functionRules);
    }

    @Override
    public void checkWhenCreating() throws DdlException {
        Map<String, String> properties = catalogProperty.getProperties();
        String catalogType = properties.getOrDefault(CatalogMgr.CATALOG_TYPE_PROP, "");

        // JDBC-specific engine-level validation: driver security, checksum, URL safety.
        // These are security policies that belong in the engine, not the connector SPI.
        if ("jdbc".equalsIgnoreCase(catalogType)) {
            validateJdbcDriver(properties);
        }

        boolean testConnection = Boolean.parseBoolean(
                catalogProperty.getOrDefault(ExternalCatalog.TEST_CONNECTION,
                        String.valueOf(ExternalCatalog.DEFAULT_TEST_CONNECTION)));
        if (!testConnection) {
            return;
        }
        // Delegate FE→external connectivity testing to the connector SPI.
        ConnectorSession session = buildConnectorSession();
        ConnectorTestResult result = connector.testConnection(session);
        if (!result.isSuccess()) {
            throw new DdlException("Connectivity test failed for catalog '"
                    + name + "': " + result.getMessage());
        }
        LOG.info("Connectivity test passed for plugin-driven catalog '{}': {}", name, result);

        // JDBC-specific: test BE→external connectivity via BRPC.
        if ("jdbc".equalsIgnoreCase(catalogType)) {
            testBeToJdbcConnection(properties);
        }
    }

    /**
     * Validates JDBC driver URL format, whitelist, secure_path, file existence,
     * and computes/verifies the MD5 checksum. This ensures the SPI path has the
     * same security guarantees as the old JdbcExternalCatalog path.
     */
    private void validateJdbcDriver(Map<String, String> properties) throws DdlException {
        String driverUrl = properties.get(JdbcResource.DRIVER_URL);
        if (driverUrl == null || driverUrl.isEmpty()) {
            return;
        }
        // Validate format, whitelist, secure_path, file existence, cloud download.
        // Throws IllegalArgumentException on any violation.
        try {
            JdbcResource.getFullDriverUrl(driverUrl);
        } catch (IllegalArgumentException e) {
            throw new DdlException("JDBC driver validation failed: " + e.getMessage(), e);
        }

        // Compute and verify checksum.
        String computedChecksum = JdbcResource.computeObjectChecksum(driverUrl);
        if (properties.containsKey(JdbcResource.CHECK_SUM)) {
            String providedChecksum = properties.get(JdbcResource.CHECK_SUM);
            if (!providedChecksum.equals(computedChecksum)) {
                throw new DdlException(
                        "The provided checksum (" + providedChecksum
                                + ") does not match the computed checksum (" + computedChecksum
                                + ") for the driver_url.");
            }
        } else {
            catalogProperty.addProperty(JdbcResource.CHECK_SUM, computedChecksum);
        }
    }

    /**
     * Tests BE→JDBC connectivity by sending a BRPC test-connection request to an
     * alive backend. Replicates the old JdbcExternalCatalog.testBeToJdbcConnection().
     */
    private void testBeToJdbcConnection(Map<String, String> properties) throws DdlException {
        if (FeConstants.runningUnitTest) {
            return;
        }
        Backend aliveBe = findAliveBackend();
        TNetworkAddress address = new TNetworkAddress(aliveBe.getHost(), aliveBe.getBrpcPort());
        try {
            TTableDescriptor testThrift = buildJdbcTestConnectionThrift(properties);
            TOdbcTableType tableType = parseJdbcOdbcType(properties);
            PJdbcTestConnectionRequest request = PJdbcTestConnectionRequest.newBuilder()
                    .setJdbcTable(ByteString.copyFrom(new TSerializer().serialize(testThrift)))
                    .setJdbcTableType(tableType.getValue())
                    .setQueryStr(getJdbcTestQuery(properties))
                    .build();
            Future<PJdbcTestConnectionResult> future = BackendServiceProxy.getInstance()
                    .testJdbcConnection(address, request);
            PJdbcTestConnectionResult result = future.get();
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                throw new DdlException("Test BE Connection to JDBC Failed: "
                        + result.getStatus().getErrorMsgs(0));
            }
        } catch (TException | RpcException | ExecutionException | InterruptedException e) {
            throw new DdlException("Test BE Connection to JDBC Failed: " + e.getMessage(), e);
        }
    }

    private Backend findAliveBackend() throws DdlException {
        try {
            for (Backend be : Env.getCurrentSystemInfo().getAllBackendsByAllCluster().values()) {
                if (be.isAlive()) {
                    return be;
                }
            }
        } catch (Exception e) {
            throw new DdlException("Failed to find alive backend: " + e.getMessage(), e);
        }
        throw new DdlException("Test BE Connection to JDBC Failed: No alive backends");
    }

    private TTableDescriptor buildJdbcTestConnectionThrift(Map<String, String> props) throws DdlException {
        TJdbcTable tJdbcTable = new TJdbcTable();
        tJdbcTable.setCatalogId(this.getId());
        tJdbcTable.setJdbcUrl(props.getOrDefault(JdbcResource.JDBC_URL, ""));
        tJdbcTable.setJdbcUser(props.getOrDefault(JdbcResource.USER, ""));
        tJdbcTable.setJdbcPassword(props.getOrDefault(JdbcResource.PASSWORD, ""));
        tJdbcTable.setJdbcTableName("test_jdbc_connection");
        tJdbcTable.setJdbcDriverClass(props.getOrDefault(JdbcResource.DRIVER_CLASS, ""));
        tJdbcTable.setJdbcDriverUrl(props.getOrDefault(JdbcResource.DRIVER_URL, ""));
        tJdbcTable.setJdbcResourceName("");
        tJdbcTable.setJdbcDriverChecksum(JdbcResource.computeObjectChecksum(
                props.getOrDefault(JdbcResource.DRIVER_URL, "")));
        tJdbcTable.setConnectionPoolMinSize(
                Integer.parseInt(props.getOrDefault(JdbcResource.CONNECTION_POOL_MIN_SIZE, "1")));
        tJdbcTable.setConnectionPoolMaxSize(
                Integer.parseInt(props.getOrDefault(JdbcResource.CONNECTION_POOL_MAX_SIZE, "30")));
        tJdbcTable.setConnectionPoolMaxWaitTime(
                Integer.parseInt(props.getOrDefault(JdbcResource.CONNECTION_POOL_MAX_WAIT_TIME, "5000")));
        tJdbcTable.setConnectionPoolMaxLifeTime(
                Integer.parseInt(props.getOrDefault(JdbcResource.CONNECTION_POOL_MAX_LIFE_TIME, "1800000")));
        tJdbcTable.setConnectionPoolKeepAlive(
                Boolean.parseBoolean(props.getOrDefault(JdbcResource.CONNECTION_POOL_KEEP_ALIVE, "false")));
        TTableDescriptor tTableDescriptor = new TTableDescriptor(0, TTableType.JDBC_TABLE, 0, 0,
                "test_jdbc_connection", "");
        tTableDescriptor.setJdbcTable(tJdbcTable);
        return tTableDescriptor;
    }

    private static TOdbcTableType parseJdbcOdbcType(Map<String, String> props) {
        String jdbcUrl = props.getOrDefault(JdbcResource.JDBC_URL, "");
        try {
            String dbType = JdbcResource.parseDbType(jdbcUrl);
            return TOdbcTableType.valueOf(dbType);
        } catch (Exception e) {
            return TOdbcTableType.MYSQL;
        }
    }

    private static String getJdbcTestQuery(Map<String, String> props) {
        String jdbcUrl = props.getOrDefault(JdbcResource.JDBC_URL, "");
        try {
            String dbType = JdbcResource.parseDbType(jdbcUrl);
            if ("ORACLE".equals(dbType)) {
                return "SELECT 1 FROM dual";
            }
        } catch (Exception e) {
            // ignore
        }
        return "SELECT 1";
    }

    /**
     * Handles catalog property updates with a create-before-swap pattern to avoid
     * race conditions between ALTER CATALOG and concurrent queries.
     *
     * <p>The old implementation had a TOCTOU race: {@code super.notifyPropertiesUpdated()}
     * called {@code resetToUninitialized()} which closed the connector via {@code onClose()},
     * leaving a window where concurrent queries could trigger {@code initLocalObjectsImpl()}
     * and create an orphaned connector that was immediately overwritten.</p>
     *
     * <p>New approach:
     * <ol>
     *   <li>Create the replacement connector <b>first</b> (outside lock — may involve I/O).</li>
     *   <li>Atomically swap the connector and set {@code connectorSwapInProgress} to suppress
     *       {@code onClose()} from closing the new connector during reset.</li>
     *   <li>Let the parent reset catalog state (caches, objectCreated, etc.).</li>
     *   <li>Close the old connector <b>after</b> the swap, outside any lock.</li>
     * </ol></p>
     */
    @Override
    public void notifyPropertiesUpdated(Map<String, String> updatedProps) {
        // 1. Create replacement connector FIRST (may involve I/O — outside any lock).
        Connector newConnector = createConnectorFromProperties();
        if (newConnector == null) {
            LOG.warn("Failed to create replacement connector for catalog '{}', "
                    + "falling back to full reset", name);
            super.notifyPropertiesUpdated(updatedProps);
            return;
        }

        // 2. Atomically swap connector and suppress onClose() from closing it during reset.
        Connector oldConnector;
        synchronized (this) {
            oldConnector = connector;
            connector = newConnector;
            connectorSwapInProgress = true;
        }
        try {
            // 3. Let parent reset catalog state (caches, objectCreated, etc.).
            //    resetToUninitialized() → onClose() sees connectorSwapInProgress and skips close.
            //    After reset, concurrent makeSureInitialized() will find connector != null
            //    and reuse the new connector without creating an orphan.
            super.notifyPropertiesUpdated(updatedProps);
        } finally {
            synchronized (this) {
                connectorSwapInProgress = false;
            }
        }

        // 4. Close old connector outside any lock.
        if (oldConnector != null) {
            try {
                oldConnector.close();
            } catch (IOException e) {
                LOG.warn("Failed to close old connector for catalog {}", name, e);
            }
        }
    }

    @Override
    protected List<String> listDatabaseNames() {
        ConnectorSession session = buildConnectorSession();
        return connector.getMetadata(session).listDatabaseNames(session);
    }

    @Override
    protected List<String> listTableNamesFromRemote(SessionContext ctx, String dbName) {
        ConnectorSession session = buildConnectorSession();
        return connector.getMetadata(session).listTableNames(session, dbName);
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        ConnectorSession session = buildConnectorSession();
        return connector.getMetadata(session)
                .getTableHandle(session, dbName, tblName).isPresent();
    }

    @Override
    public String getType() {
        // Return the actual catalog type (e.g., "es", "jdbc") from properties,
        // not the internal "plugin" logType.
        return catalogProperty.getOrDefault(CatalogMgr.CATALOG_TYPE_PROP, super.getType());
    }

    /** Returns the underlying SPI connector. Ensures the catalog is initialized first. */
    public Connector getConnector() {
        makeSureInitialized();
        return connector;
    }

    @Override
    public String fromRemoteDatabaseName(String remoteDatabaseName) {
        ConnectorSession session = buildConnectorSession();
        return connector.getMetadata(session).fromRemoteDatabaseName(session, remoteDatabaseName);
    }

    @Override
    public String fromRemoteTableName(String remoteDatabaseName, String remoteTableName) {
        ConnectorSession session = buildConnectorSession();
        return connector.getMetadata(session).fromRemoteTableName(session, remoteDatabaseName, remoteTableName);
    }

    /**
     * Builds a {@link ConnectorSession} from the current thread's {@link ConnectContext}.
     */
    public ConnectorSession buildConnectorSession() {
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null) {
            return ConnectorSessionBuilder.from(ctx)
                    .withCatalogId(getId())
                    .withCatalogName(getName())
                    .withCatalogProperties(catalogProperty.getProperties())
                    .build();
        }
        return ConnectorSessionBuilder.create()
                .withCatalogId(getId())
                .withCatalogName(getName())
                .withCatalogProperties(catalogProperty.getProperties())
                .build();
    }

    @Override
    protected ExternalDatabase<? extends ExternalTable> buildDbForInit(String remoteDbName, String localDbName,
            long dbId, InitCatalogLog.Type logType, boolean checkExists) {
        // Always use PLUGIN logType regardless of what was serialized (e.g., ES from migration).
        return super.buildDbForInit(remoteDbName, localDbName, dbId, InitCatalogLog.Type.PLUGIN, checkExists);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        super.gsonPostProcess();
        // After deserializing a migrated old catalog (e.g., ES → PluginDriven), fix logType
        // so that subsequent getType() returns "plugin" and buildDbForInit uses PLUGIN path.
        if (logType != InitCatalogLog.Type.PLUGIN) {
            LOG.info("Migrating catalog '{}' logType from {} to PLUGIN", name, logType);
            logType = InitCatalogLog.Type.PLUGIN;
        }
    }

    @Override
    public void onClose() {
        super.onClose();
        if (connectorSwapInProgress) {
            // During notifyPropertiesUpdated(), connector lifecycle is managed externally.
            return;
        }
        if (connector != null) {
            try {
                connector.close();
            } catch (IOException e) {
                LOG.warn("Failed to close connector for catalog {}", name, e);
            }
            connector = null;
        }
    }
}
