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

package org.apache.doris.datasource.es;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.es.source.EsScanNode;
import org.apache.doris.datasource.operations.ExternalMetadataOps;
import org.apache.doris.datasource.spi.CatalogProvider;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.SessionVariable;

import java.util.List;
import java.util.Map;

/**
 * CatalogProvider implementation for Elasticsearch.
 */
public class EsCatalogProvider implements CatalogProvider {

    @Override
    public String getType() {
        return "es";
    }

    @Override
    public ExternalCatalog createCatalog(long catalogId, String name, String resource,
            Map<String, String> props, String comment) {
        return new EsExternalCatalog(catalogId, name, resource, props, comment);
    }

    @Override
    public void initializeCatalog(ExternalCatalog catalog) {
        // ES catalog initialization is handled by EsExternalCatalog.initLocalObjectsImpl()
        // which is called via the normal makeSureInitialized() flow.
        // The provider just needs to exist for SPI discovery.
    }

    @Override
    public ExternalDatabase<? extends ExternalTable> createDatabase(ExternalCatalog catalog, long dbId,
            String localDbName, String remoteDbName) {
        return new EsExternalDatabase(catalog, dbId, localDbName, remoteDbName);
    }

    @Override
    public ExternalTable createTable(ExternalCatalog catalog, ExternalDatabase<? extends ExternalTable> db,
            long tblId, String localName, String remoteName) {
        return new EsExternalTable(tblId, localName, remoteName,
                (EsExternalCatalog) catalog, (EsExternalDatabase) db);
    }

    @Override
    public ScanNode createScanNode(PlanNodeId id, TupleDescriptor desc, ExternalTable table,
            SessionVariable sv, ScanContext scanContext) {
        return new EsScanNode(id, desc, table instanceof EsExternalTable, scanContext);
    }

    @Override
    public ExternalMetadataOps createMetadataOps(ExternalCatalog catalog, Map<String, String> props) {
        // ES does not support DDL operations
        return null;
    }

    @Override
    public List<String> listDatabaseNames(ExternalCatalog catalog) {
        return ((EsExternalCatalog) catalog).listDatabaseNames();
    }

    @Override
    public List<String> listTableNames(ExternalCatalog catalog, SessionContext ctx, String dbName) {
        return ((EsExternalCatalog) catalog).listTableNamesFromRemote(ctx, dbName);
    }

    @Override
    public boolean tableExist(ExternalCatalog catalog, SessionContext ctx, String dbName, String tblName) {
        return ((EsExternalCatalog) catalog).tableExist(ctx, dbName, tblName);
    }
}
