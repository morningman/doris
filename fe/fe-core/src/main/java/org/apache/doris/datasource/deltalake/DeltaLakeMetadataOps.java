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

import org.apache.doris.datasource.hive.HMSCachedClient;
import org.apache.doris.datasource.property.metastore.AbstractHiveProperties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * HMS-based metadata operations for Delta Lake catalog.
 * Uses HMS for namespace (database/table) discovery.
 * Delta Kernel Engine management is inherited from {@link AbstractDeltaLakeMetadataOps}.
 */
public class DeltaLakeMetadataOps extends AbstractDeltaLakeMetadataOps {
    private static final Logger LOG = LogManager.getLogger(DeltaLakeMetadataOps.class);

    private final HMSCachedClient hmsClient;

    public DeltaLakeMetadataOps(DeltaLakeExternalCatalog catalog, AbstractHiveProperties hmsProperties) {
        super(catalog);
        this.hmsClient = HMSCachedClient.create(hmsProperties.getHiveConf());
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

    @Override
    public String getTableLocation(String dbName, String tblName) {
        return catalog.getExecutionAuthenticator().execute(() -> {
            org.apache.hadoop.hive.metastore.api.Table table = hmsClient.getTable(dbName, tblName);
            return table.getSd().getLocation();
        });
    }

    @Override
    public void close() {
        if (hmsClient != null) {
            hmsClient.close();
        }
    }
}
