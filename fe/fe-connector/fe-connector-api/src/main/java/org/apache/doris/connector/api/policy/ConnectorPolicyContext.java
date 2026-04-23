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

package org.apache.doris.connector.api.policy;

import java.util.Objects;
import java.util.Optional;

/**
 * Plan-time context passed to {@link PolicyOps} when the engine is deciding
 * whether to push a row-level filter or apply a column mask.
 *
 * @param catalog          Catalog name (the engine-side catalog hosting this connector).
 * @param database         Source database / schema name.
 * @param table            Source table name.
 * @param requestingUser   Username of the SQL session driving the plan.
 * @param queryId          Optional engine query id, useful for plugin-side audit.
 */
public record ConnectorPolicyContext(
        String catalog,
        String database,
        String table,
        String requestingUser,
        Optional<String> queryId) {

    public ConnectorPolicyContext {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(database, "database");
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(requestingUser, "requestingUser");
        Objects.requireNonNull(queryId, "queryId");
    }
}
