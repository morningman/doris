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

package org.apache.doris.connector.api.audit;

import java.util.Set;

/**
 * D8 — Connector-side audit emission surface.
 *
 * <p>This interface declares <em>which</em> audit-event kinds the connector
 * may emit. Actual event publication happens through the engine-side audit
 * publisher reachable from {@code ConnectorContext} (wired in a later
 * milestone) — this surface only carries the static declaration so the
 * engine can reject unexpected event kinds and short-circuit subscription
 * for plugins that emit nothing.</p>
 *
 * <p>Connectors must additionally declare
 * {@code ConnectorCapability.EMITS_AUDIT_EVENTS} for the engine to honour
 * the implementation.</p>
 */
public interface ConnectorAuditOps {

    /**
     * The set of {@link AuditEventKind}s the connector may emit. Returning
     * an empty set is equivalent to not implementing this interface and
     * SHOULD be paired with omitting {@code EMITS_AUDIT_EVENTS}.
     */
    Set<AuditEventKind> emittedEventKinds();

    /** Coarse classification of {@link ConnectorAuditEvent} variants. */
    enum AuditEventKind {
        PLAN_COMPLETED,
        COMMIT_COMPLETED,
        MTMV_REFRESH,
        POLICY_EVAL
    }
}
