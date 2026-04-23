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

import java.util.Objects;
import java.util.Optional;

/**
 * D8 — Sealed hierarchy of audit events emitted by a connector.
 *
 * <p>Connectors hand instances to the engine via the connector context
 * audit publisher; the engine forwards them through its existing
 * {@code AuditEventChainListener} into the standard
 * {@code AuditEventProcessor}. Plugins must NOT open a parallel audit sink
 * — that would split the audit chain.</p>
 *
 * <p>The four permitted variants cover the audit points the engine
 * cannot synthesize from its own pipeline (the connector knows what
 * actually committed / refreshed at the source).</p>
 */
public sealed interface ConnectorAuditEvent
        permits ConnectorAuditEvent.PlanCompletedAuditEvent,
                ConnectorAuditEvent.CommitCompletedAuditEvent,
                ConnectorAuditEvent.MtmvRefreshAuditEvent,
                ConnectorAuditEvent.PolicyEvalAuditEvent {

    /** Engine-side catalog name this event is attributed to. */
    String catalog();

    /** Wall-clock time of the event in epoch-millis. */
    long eventTimeMillis();

    /** Optional engine query id, if the event arose from a SQL statement. */
    Optional<String> queryId();

    /** Plan-completed event emitted after plugin-side plan generation. */
    record PlanCompletedAuditEvent(
            String catalog,
            long eventTimeMillis,
            Optional<String> queryId,
            String database,
            String table,
            long planTimeMillis,
            long scanRangeCount) implements ConnectorAuditEvent {
        public PlanCompletedAuditEvent {
            Objects.requireNonNull(catalog, "catalog");
            Objects.requireNonNull(queryId, "queryId");
            Objects.requireNonNull(database, "database");
            Objects.requireNonNull(table, "table");
        }
    }

    /** Commit-completed event emitted after a successful write commit. */
    record CommitCompletedAuditEvent(
            String catalog,
            long eventTimeMillis,
            Optional<String> queryId,
            String database,
            String table,
            String commitId,
            long rowsWritten) implements ConnectorAuditEvent {
        public CommitCompletedAuditEvent {
            Objects.requireNonNull(catalog, "catalog");
            Objects.requireNonNull(queryId, "queryId");
            Objects.requireNonNull(database, "database");
            Objects.requireNonNull(table, "table");
            Objects.requireNonNull(commitId, "commitId");
        }
    }

    /** MTMV-refresh event emitted after a base-table snapshot has been read for refresh. */
    record MtmvRefreshAuditEvent(
            String catalog,
            long eventTimeMillis,
            Optional<String> queryId,
            String database,
            String table,
            Optional<String> partitionName,
            long snapshotMarker) implements ConnectorAuditEvent {
        public MtmvRefreshAuditEvent {
            Objects.requireNonNull(catalog, "catalog");
            Objects.requireNonNull(queryId, "queryId");
            Objects.requireNonNull(database, "database");
            Objects.requireNonNull(table, "table");
            Objects.requireNonNull(partitionName, "partitionName");
        }
    }

    /** Policy-evaluation event emitted after the plugin has resolved a hint. */
    record PolicyEvalAuditEvent(
            String catalog,
            long eventTimeMillis,
            Optional<String> queryId,
            String database,
            String table,
            String requestingUser,
            String policyKind,
            boolean hintApplied) implements ConnectorAuditEvent {
        public PolicyEvalAuditEvent {
            Objects.requireNonNull(catalog, "catalog");
            Objects.requireNonNull(queryId, "queryId");
            Objects.requireNonNull(database, "database");
            Objects.requireNonNull(table, "table");
            Objects.requireNonNull(requestingUser, "requestingUser");
            Objects.requireNonNull(policyKind, "policyKind");
        }
    }
}
