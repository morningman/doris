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

/**
 * Plugin-internal SPI implemented by every Paimon backend variant
 * (filesystem, hms, rest, aliyun-dlf). The orchestrator dispatches to the
 * matching backend based on the {@code paimon.catalog.type} property.
 *
 * <p>Implementations live in their own jars (one per {@code
 * fe-connector-paimon-backend-<name>} module) and are discovered via
 * {@link PaimonBackendFactory} ServiceLoader registrations. Backend jars
 * isolate their per-vendor Paimon SDK dependencies (e.g. {@code
 * paimon-hive-connector-3.1} for hms; alibaba-cloud DLF metastore client
 * for aliyun-dlf) so a Doris install that only uses filesystem or rest does
 * not have to ship the Hive bundle.
 */
public interface PaimonBackend {

    /**
     * Canonical lowercase backend identifier matching the value of the
     * {@code paimon.catalog.type} property (e.g. {@code "filesystem"},
     * {@code "hms"}, {@code "rest"}, {@code "aliyun-dlf"}).
     */
    String name();

    /**
     * Build the Paimon SDK {@link Catalog} for this backend.
     */
    Catalog buildCatalog(PaimonBackendContext context);
}
