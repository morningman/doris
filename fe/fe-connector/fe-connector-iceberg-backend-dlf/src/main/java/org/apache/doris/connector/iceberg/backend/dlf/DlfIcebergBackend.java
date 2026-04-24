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

package org.apache.doris.connector.iceberg.backend.dlf;

import org.apache.doris.connector.iceberg.api.IcebergBackend;
import org.apache.doris.connector.iceberg.api.IcebergBackendContext;
import org.apache.doris.connector.iceberg.api.IcebergBackendException;

import org.apache.iceberg.catalog.Catalog;

/**
 * Aliyun DLF backend for Iceberg.
 *
 * <p>Currently a stub: the real {@code DLFCatalog} implementation lives
 * in fe-core at {@code org.apache.doris.datasource.iceberg.dlf.DLFCatalog}
 * and depends on alibaba-cloud DLF SDK classes that are not yet packaged in
 * the connector plugin classloader.
 *
 * <p>Anomaly #4 (PR-task-breakdown): the previous monolithic
 * {@code IcebergConnector} hard-coded a reference to the non-existent
 * class {@code org.apache.doris.connector.iceberg.dlf.DLFCatalog}, which
 * would have failed at first use. This stub replaces the broken reference
 * with an explicit, diagnosable failure.
 *
 * <p>TODO(M1-04 split): backend stub — real impl in M3.
 */
public final class DlfIcebergBackend implements IcebergBackend {

    public static final String NAME = "dlf";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Catalog buildCatalog(IcebergBackendContext context) {
        throw new IcebergBackendException(
                "iceberg backend 'dlf' is not yet implemented in fe-connector-iceberg-backend-dlf;"
                        + " tracked in M3. Use type=hms or type=rest until then.");
    }
}
