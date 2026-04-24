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

import org.apache.iceberg.catalog.Catalog;

/**
 * Plugin-internal SPI implemented by every Iceberg backend variant (hms,
 * rest, glue, dlf, s3tables, hadoop). The orchestrator dispatches to the
 * matching backend based on the {@code iceberg.catalog.type} property.
 *
 * <p>Implementations live in their own jars (one per {@code
 * fe-connector-iceberg-backend-<name>} module) and are discovered via
 * {@link IcebergBackendFactory} ServiceLoader registrations. Backend jars
 * isolate their per-vendor Iceberg SDK dependencies (e.g. {@code
 * iceberg-aws} for glue / s3tables) so a Doris install that only uses REST
 * or HMS does not have to ship the AWS bundle.
 */
public interface IcebergBackend {

    /**
     * Canonical lowercase backend identifier matching the value of the
     * {@code iceberg.catalog.type} property (e.g. {@code "hms"},
     * {@code "rest"}, {@code "glue"}).
     */
    String name();

    /**
     * Build the Iceberg SDK {@link Catalog} for this backend.
     */
    Catalog buildCatalog(IcebergBackendContext context);
}
