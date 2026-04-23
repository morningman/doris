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

/**
 * How a {@link SysTableSpec} is executed by fe-core.
 *
 * <p>Selected by the plugin in {@link SysTableSpec#mode()}; determines which
 * of the three optional execution carriers on the spec must be populated.</p>
 */
public enum SysTableExecutionMode {
    /**
     * Served by a plugin-provided {@link NativeSysTableScanFactory} that
     * plugs into the engine's standard scan path (reusing the main-table
     * ScanPlanProvider cache, filter pushdown, column projection, etc.).
     * Requires capability {@code SUPPORTS_NATIVE_SYS_TABLES}.
     */
    NATIVE,

    /**
     * Served by a plugin-provided {@link TvfSysTableInvoker} that maps the
     * sys table to an existing fe-core metadata table-valued function.
     * Requires capability {@code SUPPORTS_TVF_SYS_TABLES}.
     */
    TVF,

    /**
     * Served entirely by the plugin via its own {@code ConnectorScanPlanProvider};
     * the spec carries an opaque {@link ConnectorScanPlanProviderRef} that
     * fe-core resolves back to the provider at plan time.
     */
    CONNECTOR_SCAN_PLAN
}
