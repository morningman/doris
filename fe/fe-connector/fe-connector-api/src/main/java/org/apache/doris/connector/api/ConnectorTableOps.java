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

package org.apache.doris.connector.api;

import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.timetravel.ConnectorRefSpec;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Operations on tables within a connector catalog.
 */
public interface ConnectorTableOps {

    /** Retrieves a table handle for the given database and table name. */
    default Optional<ConnectorTableHandle> getTableHandle(
            ConnectorSession session, String dbName,
            String tableName) {
        return Optional.empty();
    }

    /**
     * Time-travel-aware overload of {@link #getTableHandle}. The engine
     * passes the resolved {@link ConnectorTableVersion} (from
     * {@code FOR VERSION/TIME AS OF}) and {@link ConnectorRefSpec} (from
     * {@code @branch(...)/@tag(...)}) the user requested; the connector
     * resolves both into a snapshot pin on the returned handle. The
     * default ignores both and falls back to the legacy 3-arg overload
     * so connectors without time-travel support keep working unchanged.
     * (D5 §3.0.2 / G-D5)
     */
    default Optional<ConnectorTableHandle> getTableHandle(
            ConnectorSession session, String dbName, String tableName,
            Optional<ConnectorTableVersion> version,
            Optional<ConnectorRefSpec> refSpec) {
        return getTableHandle(session, dbName, tableName);
    }

    /** Returns the schema (columns, format, etc.) for the given table. */
    default ConnectorTableSchema getTableSchema(
            ConnectorSession session, ConnectorTableHandle handle) {
        throw new DorisConnectorException(
                "getTableSchema not implemented");
    }

    /** Returns a name-to-handle map for all columns of the table. */
    default Map<String, ConnectorColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle handle) {
        throw new DorisConnectorException(
                "getColumnHandles not implemented");
    }

    /** Lists all table names within the given database. */
    default List<String> listTableNames(ConnectorSession session,
            String dbName) {
        return Collections.emptyList();
    }

    /** Creates a new table with the given schema and properties. */
    default void createTable(ConnectorSession session,
            ConnectorTableSchema schema,
            Map<String, String> properties) {
        throw new DorisConnectorException(
                "CREATE TABLE not supported");
    }

    /** Drops the specified table. */
    default void dropTable(ConnectorSession session,
            ConnectorTableHandle handle) {
        throw new DorisConnectorException(
                "DROP TABLE not supported");
    }

    /** Returns the primary key column names for the given table. */
    default List<String> getPrimaryKeys(ConnectorSession session,
            String dbName, String tableName) {
        return Collections.emptyList();
    }

    /** Returns a human-readable comment for the given table. */
    default String getTableComment(ConnectorSession session,
            String dbName, String tableName) {
        return "";
    }

    /**
     * Executes a DML statement (INSERT/UPDATE/DELETE) directly.
     * Used for DML passthrough features like CALL EXECUTE_STMT.
     */
    default void executeStmt(ConnectorSession session, String stmt) {
        throw new DorisConnectorException("executeStmt not supported");
    }

    /**
     * Gets column metadata from a query string via PreparedStatement metadata.
     * Used for table-valued functions like query().
     */
    default ConnectorTableSchema getColumnsFromQuery(ConnectorSession session, String query) {
        throw new DorisConnectorException("getColumnsFromQuery not supported");
    }

    /**
     * Builds the Thrift {@code TTableDescriptor} that BE needs for query execution.
     *
     * <p>Each connector constructs its own typed descriptor (e.g., {@code TJdbcTable},
     * {@code TEsTable}) and wraps it in a {@code TTableDescriptor}. This keeps
     * connector-specific Thrift logic out of fe-core.</p>
     *
     * <p>The Thrift classes are provided by fe-thrift at compile time and loaded
     * from the parent classloader at runtime.</p>
     *
     * @param session connector session
     * @param tableId Doris internal table ID
     * @param tableName table display name
     * @param dbName database name
     * @param remoteName remote table name in the external data source
     * @param numCols number of columns in the schema
     * @param catalogId Doris internal catalog ID
     * @return the table descriptor, or {@code null} if the connector does not
     *         need a typed descriptor (fe-core will use a generic fallback)
     */
    default org.apache.doris.thrift.TTableDescriptor buildTableDescriptor(
            ConnectorSession session,
            long tableId, String tableName, String dbName,
            String remoteName, int numCols, long catalogId) {
        return null;
    }
}
