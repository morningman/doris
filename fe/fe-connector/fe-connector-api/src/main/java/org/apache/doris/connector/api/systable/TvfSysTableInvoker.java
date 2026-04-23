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

import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;

import java.util.Optional;

/**
 * Resolves a {@link SysTableExecutionMode#TVF} system table to a concrete
 * {@link TvfInvocation} that fe-core then routes through its existing
 * metadata TVF machinery.
 *
 * <p>Table identity is passed as a {@code (database, table)} pair, matching
 * the rest of the SPI.</p>
 */
@FunctionalInterface
public interface TvfSysTableInvoker {

    /**
     * Resolves a sys-table access to a TVF invocation.
     *
     * @param database      main table's database, non-null
     * @param table         main table's name, non-null
     * @param sysTableName  sys table name (e.g. {@code "partitions"}), non-null
     * @param version       optional time-travel version (only for sys tables
     *                      that opt in via {@link SysTableSpec#acceptsTableVersion()})
     * @return a non-null invocation descriptor
     */
    TvfInvocation resolve(String database,
            String table,
            String sysTableName,
            Optional<ConnectorTableVersion> version);
}
