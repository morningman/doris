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

import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;

import java.util.Optional;

/**
 * Factory that materialises a {@link ConnectorScanPlanProvider} for a
 * {@link SysTableExecutionMode#NATIVE} system table.
 *
 * <p>Called by fe-core at plan time. The plugin is expected to reuse the
 * main table's ScanPlanProvider cache where applicable (D3 cache hit on
 * the parent table should serve the sys table too). fe-core passes along
 * an {@link ConnectorTableVersion} when the sys table declares
 * {@link SysTableSpec#acceptsTableVersion()}; otherwise the argument is
 * {@link Optional#empty()}.</p>
 *
 * <p>Table identity is passed as a {@code (database, table)} pair in line
 * with the rest of the SPI (the typed {@code ConnectorTableId} does not
 * yet exist in {@code fe-connector-api}).</p>
 */
@FunctionalInterface
public interface NativeSysTableScanFactory {

    /**
     * Creates a scan plan provider that reads the given sys table.
     *
     * @param database      main table's database, non-null
     * @param table         main table's name, non-null
     * @param sysTableName  sys table name (e.g. {@code "snapshots"}), non-null
     * @param version       optional time-travel version for sys tables that
     *                      opt in via {@link SysTableSpec#acceptsTableVersion()};
     *                      empty when the sys table is version-agnostic
     * @return a non-null provider
     */
    ConnectorScanPlanProvider create(String database,
            String table,
            String sysTableName,
            Optional<ConnectorTableVersion> version);
}
