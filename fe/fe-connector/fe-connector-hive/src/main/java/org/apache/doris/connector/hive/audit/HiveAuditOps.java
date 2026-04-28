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

package org.apache.doris.connector.hive.audit;

import org.apache.doris.connector.api.audit.ConnectorAuditOps;

import java.util.Set;

/**
 * D8 (M2-13a) — Hive plugin audit declaration.
 *
 * <p>Declares the set of {@link AuditEventKind}s the Hive plugin may emit.
 * The Hive plugin only publishes {@link AuditEventKind#PLAN_COMPLETED}
 * events from {@code HiveScanPlanProvider#planScan(...)}.</p>
 *
 * <p>Pair this with {@code ConnectorCapability.EMITS_AUDIT_EVENTS} on the
 * connector for the engine-side audit publisher to honour the
 * implementation (see
 * {@code DefaultConnectorContext#publishAuditEvent}).</p>
 */
public final class HiveAuditOps implements ConnectorAuditOps {

    public static final HiveAuditOps INSTANCE = new HiveAuditOps();

    private static final Set<AuditEventKind> KINDS = Set.of(AuditEventKind.PLAN_COMPLETED);

    private HiveAuditOps() {
    }

    @Override
    public Set<AuditEventKind> emittedEventKinds() {
        return KINDS;
    }
}
