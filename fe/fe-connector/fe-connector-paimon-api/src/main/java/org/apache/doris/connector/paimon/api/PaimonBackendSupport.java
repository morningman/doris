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

package org.apache.doris.connector.paimon.api;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;

import java.util.HashMap;
import java.util.Map;

/**
 * Shared utility for backends that delegate to {@link CatalogFactory#createCatalog}
 * with a fixed {@code metastore} option.
 *
 * <p>Paimon's {@link CatalogFactory} dispatches by the {@code metastore}
 * option (via the paimon-core ServiceLoader on
 * {@code org.apache.paimon.factories.Factory}); each backend forces the
 * correct value before delegating, so the orchestrator does not need a
 * static {@code switch} on {@code paimon.catalog.type}. The Doris-specific
 * key {@code paimon.catalog.type} is stripped before dispatch so it does
 * not leak into Paimon's option validation.
 */
public final class PaimonBackendSupport {

    /** Doris-side catalog-type property; stripped before Paimon dispatch. */
    public static final String PAIMON_CATALOG_TYPE = "paimon.catalog.type";

    /** Paimon-side metastore dispatch option. */
    public static final String PAIMON_METASTORE_OPTION = "metastore";

    private PaimonBackendSupport() {
    }

    /**
     * Build a Catalog by forcing the Paimon {@code metastore} option to
     * {@code metastoreId}.
     */
    public static Catalog buildByMetastore(PaimonBackendContext context, String metastoreId) {
        return buildByMetastore(context, metastoreId, new HashMap<>());
    }

    /**
     * Build a Catalog by forcing the Paimon {@code metastore} option to
     * {@code metastoreId} and merging additional backend-specific options
     * (later entries override). Backend-supplied options are applied last
     * so they cannot be overridden by user properties.
     */
    public static Catalog buildByMetastore(PaimonBackendContext context,
                                           String metastoreId,
                                           Map<String, String> extraOptions) {
        Map<String, String> props = new HashMap<>(context.properties());
        props.remove(PAIMON_CATALOG_TYPE);
        props.put(PAIMON_METASTORE_OPTION, metastoreId);
        props.putAll(extraOptions);
        Options options = Options.fromMap(props);
        CatalogContext catalogContext = CatalogContext.create(options);
        return CatalogFactory.createCatalog(catalogContext);
    }
}
