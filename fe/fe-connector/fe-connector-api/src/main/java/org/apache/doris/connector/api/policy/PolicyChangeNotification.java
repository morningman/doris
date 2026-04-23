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

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Notification delivered to {@link PolicyOps#onPolicyChanged} when a governance
 * policy attached to one of the connector's tables changes upstream. The
 * connector may use this to invalidate any plugin-side cache; the engine still
 * owns the policy itself.
 *
 * @param catalog    Engine-side catalog name.
 * @param database   Source database / schema name.
 * @param table      Optional table name; absent for catalog-wide / schema-wide changes.
 * @param policyKind Kind of policy that changed.
 * @param eventTime  Engine-side wall-clock at which the change was observed.
 * @param policyId   Optional opaque identifier of the changed policy.
 */
public record PolicyChangeNotification(
        String catalog,
        String database,
        Optional<String> table,
        PolicyKind policyKind,
        Instant eventTime,
        Optional<String> policyId) {

    public PolicyChangeNotification {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(database, "database");
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(policyKind, "policyKind");
        Objects.requireNonNull(eventTime, "eventTime");
        Objects.requireNonNull(policyId, "policyId");
    }
}
