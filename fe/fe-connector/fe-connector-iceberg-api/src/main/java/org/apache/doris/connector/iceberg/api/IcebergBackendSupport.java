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

package org.apache.doris.connector.iceberg.api;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;

import java.util.HashMap;
import java.util.Map;

/**
 * Shared utility for backends that delegate to {@link CatalogUtil#buildIcebergCatalog}
 * with a fixed {@code catalog-impl} class name.
 *
 * <p>The Iceberg SDK rejects property maps that contain both {@code type}
 * and {@code catalog-impl}; this helper strips the {@code type} key before
 * dispatch so backends can use either property style at the orchestrator
 * boundary.
 */
public final class IcebergBackendSupport {

    private IcebergBackendSupport() {
    }

    /**
     * Build a Catalog by forcing {@code catalog-impl} to {@code catalogImplFqcn}.
     */
    public static Catalog buildByImpl(IcebergBackendContext context, String catalogImplFqcn) {
        Map<String, String> props = new HashMap<>(context.properties());
        props.put(CatalogProperties.CATALOG_IMPL, catalogImplFqcn);
        props.remove(CatalogUtil.ICEBERG_CATALOG_TYPE);
        return CatalogUtil.buildIcebergCatalog(
                context.catalogName(), props, context.hadoopConf());
    }
}
