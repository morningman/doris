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

package org.apache.doris.datasource.deltalake;

import org.apache.doris.catalog.info.CreateOrReplaceBranchInfo;
import org.apache.doris.catalog.info.CreateOrReplaceTagInfo;
import org.apache.doris.catalog.info.DropBranchInfo;
import org.apache.doris.catalog.info.DropTagInfo;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HMSCachedClient;
import org.apache.doris.datasource.operations.ExternalMetadataOps;
import org.apache.doris.datasource.property.metastore.AbstractHiveProperties;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.defaults.engine.DefaultEngine;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Metadata operations for Delta Lake catalog.
 * Uses HMS for namespace (database/table) discovery and Delta Kernel for
 * table-level metadata (schema, partitions, snapshots).
 */
public class DeltaLakeMetadataOps implements ExternalMetadataOps {
    private static final Logger LOG = LogManager.getLogger(DeltaLakeMetadataOps.class);

    private final DeltaLakeExternalCatalog catalog;
    private final HMSCachedClient hmsClient;
    private volatile Engine engine;

    public DeltaLakeMetadataOps(DeltaLakeExternalCatalog catalog, AbstractHiveProperties hmsProperties) {
        this.catalog = catalog;
        this.hmsClient = HMSCachedClient.create(hmsProperties.getHiveConf());
    }

    /**
     * Get or create the Delta Kernel Engine instance.
     * Uses Hadoop Configuration from the catalog properties.
     */
    public synchronized Engine getEngine() {
        if (engine == null) {
            Configuration hadoopConf = catalog.getConfiguration();
            engine = DefaultEngine.create(hadoopConf);
        }
        return engine;
    }

    @Override
    public List<String> listDatabaseNames() {
        return catalog.getExecutionAuthenticator().execute(() -> hmsClient.getAllDatabases());
    }

    @Override
    public List<String> listTableNames(String dbName) {
        return catalog.getExecutionAuthenticator().execute(() -> {
            List<String> allTables = hmsClient.getAllTables(dbName);
            // TODO: filter for Delta Lake tables only (check table properties for delta.* or
            //       check for _delta_log directory existence)
            return allTables;
        });
    }

    @Override
    public boolean tableExist(String dbName, String tblName) {
        return catalog.getExecutionAuthenticator().execute(() -> hmsClient.tableExists(dbName, tblName));
    }

    @Override
    public boolean databaseExist(String dbName) {
        return catalog.getExecutionAuthenticator().execute(() -> {
            try {
                hmsClient.getDatabase(dbName);
                return true;
            } catch (Exception e) {
                return false;
            }
        });
    }

    /**
     * Get the table location from HMS.
     */
    public String getTableLocation(String dbName, String tblName) {
        return catalog.getExecutionAuthenticator().execute(() -> {
            org.apache.hadoop.hive.metastore.api.Table table = hmsClient.getTable(dbName, tblName);
            return table.getSd().getLocation();
        });
    }

    // ========== Read-only catalog: DDL operations not supported ==========

    @Override
    public boolean createDbImpl(String dbName, boolean ifNotExists, java.util.Map<String, String> properties)
            throws DdlException {
        throw new DdlException("Create database is not supported for Delta Lake catalog.");
    }

    @Override
    public void dropDbImpl(String dbName, boolean ifExists, boolean force) throws DdlException {
        throw new DdlException("Drop database is not supported for Delta Lake catalog.");
    }

    @Override
    public void afterDropDb(String dbName) {
        // no-op
    }

    @Override
    public boolean createTableImpl(CreateTableInfo createTableInfo) throws UserException {
        throw new DdlException("Create table is not supported for Delta Lake catalog.");
    }

    @Override
    public void dropTableImpl(ExternalTable dorisTable, boolean ifExists) throws DdlException {
        throw new DdlException("Drop table is not supported for Delta Lake catalog.");
    }

    @Override
    public void truncateTableImpl(ExternalTable dorisTable, List<String> partitions) throws DdlException {
        throw new DdlException("Truncate table is not supported for Delta Lake catalog.");
    }

    @Override
    public void createOrReplaceBranchImpl(ExternalTable dorisTable, CreateOrReplaceBranchInfo branchInfo)
            throws UserException {
        throw new UserException("Create or replace branch is not supported for Delta Lake catalog.");
    }

    @Override
    public void createOrReplaceTagImpl(ExternalTable dorisTable, CreateOrReplaceTagInfo tagInfo)
            throws UserException {
        throw new UserException("Create or replace tag is not supported for Delta Lake catalog.");
    }

    @Override
    public void dropTagImpl(ExternalTable dorisTable, DropTagInfo tagInfo) throws UserException {
        throw new UserException("Drop tag is not supported for Delta Lake catalog.");
    }

    @Override
    public void dropBranchImpl(ExternalTable dorisTable, DropBranchInfo branchInfo) throws UserException {
        throw new UserException("Drop branch is not supported for Delta Lake catalog.");
    }

    @Override
    public void close() {
        if (hmsClient != null) {
            hmsClient.close();
        }
    }
}
