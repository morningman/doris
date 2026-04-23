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

package org.apache.doris.connector.api.dispatch;

import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import java.util.Optional;

/**
 * Optional interface implemented by "host" connectors (e.g., {@code hive})
 * that can declaratively delegate certain tables (e.g., iceberg-on-hms,
 * hudi-on-hms) to a different connector type. The engine consults
 * {@link #resolveTarget} at metadata-load time and, when a
 * {@link DispatchTarget} is returned, routes schema/scan/stats/MTMV/policy/
 * sys-table requests to the delegate while keeping listing on the host.
 *
 * <p>{@code resolveTarget} MUST be cheap and side-effect free; it is called
 * during table loading and must NOT issue additional metastore RPCs. It
 * works only on the {@link RawTableMetadata} the host already fetched.</p>
 */
public interface ConnectorDispatchOps {

    /**
     * Examine an already-fetched {@link RawTableMetadata} and decide whether
     * the table should be served by a different connector type.
     *
     * @return non-empty {@link DispatchTarget} to delegate; empty to keep the
     *     table on the host connector.
     */
    Optional<DispatchTarget> resolveTarget(ConnectorTableHandle tableHandle, RawTableMetadata raw);
}
