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

package org.apache.doris.connector.api.systable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Connector-side entry point for enumerating and resolving system /
 * metadata tables attached to a main table.
 *
 * <p>Extended by {@code ConnectorMetadata} with empty defaults; connectors
 * that declare capability {@code SUPPORTS_SYSTEM_TABLES} override these
 * methods to publish a statically-known set of sys tables. Per design
 * §9.6, {@link #listSysTables} MUST return a static enumeration and MUST
 * NOT hit the metastore at call time.</p>
 *
 * <p>Table identity is passed as a {@code (database, table)} pair in line
 * with the rest of the SPI (the typed {@code ConnectorTableId} does not
 * yet exist in {@code fe-connector-api}).</p>
 */
public interface SystemTableOps {

    /**
     * Static set of sys tables attached to the given main table.
     * Default: empty list.
     *
     * <p>Implementations MUST be cheap (no I/O) — the engine may call this
     * during query planning and metadata discovery.</p>
     */
    default List<SysTableSpec> listSysTables(String database, String table) {
        return Collections.emptyList();
    }

    /**
     * Looks up a specific sys table by name (without any leading {@code '$'}).
     * Default: empty.
     */
    default Optional<SysTableSpec> getSysTable(String database, String table, String sysTableName) {
        return Optional.empty();
    }

    /**
     * Whether the given sys table exists on the given main table.
     * Default delegates to {@link #getSysTable}.
     */
    default boolean supportsSysTable(String database, String table, String sysTableName) {
        return getSysTable(database, table, sysTableName).isPresent();
    }
}
