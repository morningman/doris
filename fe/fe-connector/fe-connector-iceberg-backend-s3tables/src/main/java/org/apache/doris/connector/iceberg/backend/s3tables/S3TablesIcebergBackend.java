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

package org.apache.doris.connector.iceberg.backend.s3tables;

import org.apache.doris.connector.iceberg.api.IcebergBackend;
import org.apache.doris.connector.iceberg.api.IcebergBackendContext;
import org.apache.doris.connector.iceberg.api.IcebergBackendSupport;

import org.apache.iceberg.catalog.Catalog;

/**
 * Iceberg backend that builds {@code software.amazon.s3tables.iceberg.S3TablesCatalog}
 * via the standard {@code catalog-impl} dispatch.
 */
public final class S3TablesIcebergBackend implements IcebergBackend {

    public static final String NAME = "s3tables";
    public static final String CATALOG_IMPL = "software.amazon.s3tables.iceberg.S3TablesCatalog";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Catalog buildCatalog(IcebergBackendContext context) {
        return IcebergBackendSupport.buildByImpl(context, CATALOG_IMPL);
    }
}
