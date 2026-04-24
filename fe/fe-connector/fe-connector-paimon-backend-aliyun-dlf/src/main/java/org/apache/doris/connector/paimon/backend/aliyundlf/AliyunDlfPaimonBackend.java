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

package org.apache.doris.connector.paimon.backend.aliyundlf;

import org.apache.doris.connector.paimon.api.PaimonBackend;
import org.apache.doris.connector.paimon.api.PaimonBackendContext;
import org.apache.doris.connector.paimon.api.PaimonBackendException;

import org.apache.paimon.catalog.Catalog;

/**
 * Aliyun DLF backend for Paimon.
 *
 * <p>Currently a stub: the real implementation reuses the Paimon
 * {@code HiveCatalog} with a custom {@code ProxyMetaStoreClient}
 * (from {@code com.aliyun.datalake:metastore-client-hive2}) plus the DLF
 * connection options ({@code metastore.client.class},
 * {@code client-pool-cache.keys}). The aliyun-datalake metastore-client
 * jar is not yet packaged in the connector plugin classloader; today the
 * equivalent code lives in fe-core at
 * {@code org.apache.doris.datasource.property.metastore.PaimonAliyunDLFMetaStoreProperties}
 * and will migrate into this module in M3.
 *
 * <p>TODO(M1-05 split): backend stub — real impl in M3, mirroring the
 * iceberg DLF stub introduced by M1-04.
 */
public class AliyunDlfPaimonBackend implements PaimonBackend {

    public static final String NAME = "aliyun-dlf";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Catalog buildCatalog(PaimonBackendContext context) {
        throw new PaimonBackendException(
                "paimon backend 'aliyun-dlf' is not yet implemented in"
                        + " fe-connector-paimon-backend-aliyun-dlf; tracked in M3."
                        + " Use type=filesystem, type=hms or type=rest until then.");
    }
}
