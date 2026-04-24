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

package org.apache.doris.connector.paimon.backend.rest;

import org.apache.doris.connector.paimon.api.PaimonBackend;
import org.apache.doris.connector.paimon.api.PaimonBackendContext;
import org.apache.doris.connector.paimon.api.PaimonBackendSupport;

import org.apache.paimon.catalog.Catalog;

/**
 * Paimon backend that builds a {@code RESTCatalog} via the Paimon
 * {@code metastore=rest} dispatch. The REST catalog implementation ships
 * inside paimon-core, so no extra runtime dependency is required.
 */
public class RestPaimonBackend implements PaimonBackend {

    public static final String NAME = "rest";
    public static final String METASTORE_ID = "rest";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Catalog buildCatalog(PaimonBackendContext context) {
        return PaimonBackendSupport.buildByMetastore(context, METASTORE_ID);
    }
}
