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

package org.apache.doris.datasource.spi;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.operations.ExternalMetadataOps;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.SessionVariable;

import java.util.List;
import java.util.Map;

/**
 * Service Provider Interface for external data source catalogs.
 *
 * <p>Each external data source (ES, Iceberg, Paimon, etc.) implements this interface
 * and registers it via Java {@link java.util.ServiceLoader}. This enables Doris to
 * discover and use data source plugins without hard-coded dependencies in fe-core.</p>
 *
 * <p>The provider is responsible for creating catalog-specific objects (Database, Table,
 * ScanNode, MetadataOps) and handling catalog initialization.</p>
 */
public interface CatalogProvider {

    /**
     * Returns the catalog type identifier.
     * This must match the "type" property used in CREATE CATALOG statements.
     * For example: "es", "hms", "iceberg", "paimon", "jdbc", etc.
     */
    String getType();

    /**
     * Create a new ExternalCatalog instance for this data source type.
     * Called by {@code CatalogFactory} when creating a new catalog via CREATE CATALOG
     * or when replaying from EditLog.
     *
     * @param catalogId the catalog id
     * @param name the catalog name
     * @param resource the resource name (nullable)
     * @param props catalog properties
     * @param comment catalog comment
     * @return a new ExternalCatalog instance
     */
    ExternalCatalog createCatalog(long catalogId, String name, String resource,
            Map<String, String> props, String comment);

    /**
     * Initialize the catalog.
     * Called during {@code makeSureInitialized()} when the catalog is first accessed.
     * The provider should set up any clients, connections, or other resources needed
     * to interact with the external data source.
     *
     * @param catalog the catalog instance to initialize
     */
    void initializeCatalog(ExternalCatalog catalog);

    /**
     * Create a database instance for this catalog type.
     *
     * @param catalog the parent catalog
     * @param dbId the database id
     * @param localDbName the local database name
     * @param remoteDbName the remote database name
     * @return a new ExternalDatabase instance
     */
    ExternalDatabase<? extends ExternalTable> createDatabase(ExternalCatalog catalog, long dbId,
            String localDbName, String remoteDbName);

    /**
     * Create a table instance for this catalog type.
     *
     * @param catalog the parent catalog
     * @param db the parent database
     * @param tblId the table id
     * @param localName the local table name
     * @param remoteName the remote table name
     * @return a new ExternalTable instance
     */
    ExternalTable createTable(ExternalCatalog catalog, ExternalDatabase<? extends ExternalTable> db,
            long tblId, String localName, String remoteName);

    /**
     * Create a scan node for query execution.
     *
     * @param id plan node id
     * @param desc tuple descriptor for the scan output
     * @param table the table being scanned
     * @param sv session variable
     * @param scanContext scan context
     * @return a new ScanNode instance specific to this data source
     */
    ScanNode createScanNode(PlanNodeId id, TupleDescriptor desc, ExternalTable table,
            SessionVariable sv, ScanContext scanContext);

    /**
     * Create the metadata operations handler for this catalog type.
     *
     * @param catalog the catalog instance
     * @param props catalog properties
     * @return a new ExternalMetadataOps instance, or null if DDL is not supported
     */
    ExternalMetadataOps createMetadataOps(ExternalCatalog catalog, Map<String, String> props);

    /**
     * List database names from the remote data source.
     *
     * @param catalog the catalog instance
     * @return list of database names
     */
    List<String> listDatabaseNames(ExternalCatalog catalog);

    /**
     * List table names from the remote data source.
     *
     * @param catalog the catalog instance
     * @param ctx session context
     * @param dbName database name
     * @return list of table names
     */
    List<String> listTableNames(ExternalCatalog catalog, SessionContext ctx, String dbName);

    /**
     * Check if a table exists in the remote data source.
     *
     * @param catalog the catalog instance
     * @param ctx session context
     * @param dbName database name
     * @param tblName table name
     * @return true if the table exists
     */
    boolean tableExist(ExternalCatalog catalog, SessionContext ctx, String dbName, String tblName);
}
