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

package org.apache.doris.connector.api;

import org.apache.doris.connector.api.action.ConnectorActionOps;
import org.apache.doris.connector.api.audit.ConnectorAuditOps;
import org.apache.doris.connector.api.event.EventSourceOps;
import org.apache.doris.connector.api.mtmv.MtmvOps;
import org.apache.doris.connector.api.policy.PolicyOps;
import org.apache.doris.connector.api.systable.SystemTableOps;
import org.apache.doris.connector.api.timetravel.RefOps;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * Central metadata interface that a connector must implement.
 *
 * <p>Extends the fine-grained sub-interfaces for schema, table,
 * pushdown, statistics, and write operations. Each sub-interface
 * provides sensible defaults so that connectors only need to
 * override the methods they actually support.</p>
 */
public interface ConnectorMetadata extends
        ConnectorSchemaOps,
        ConnectorTableOps,
        ConnectorPushdownOps,
        ConnectorStatisticsOps,
        ConnectorWriteOps,
        ConnectorIdentifierOps,
        SystemTableOps,
        Closeable {

    /** Returns connector-level properties. */
    default Map<String, String> getProperties() {
        return Collections.emptyMap();
    }

    /**
     * Returns the connector's ref (branch/tag) operations, if any.
     * Defaults to {@link Optional#empty()}; only connectors that declare
     * {@code ConnectorCapability.SUPPORTS_BRANCH_TAG} are expected to override.
     */
    default Optional<RefOps> refOps() {
        return Optional.empty();
    }

    /**
     * Returns the event source for this connector. Defaults to
     * {@link EventSourceOps#NONE}; only connectors that declare
     * {@code SUPPORTS_PULL_EVENTS}/{@code SUPPORTS_PUSH_EVENTS} are
     * expected to override.
     */
    default EventSourceOps getEventSourceOps() {
        return EventSourceOps.NONE;
    }

    /**
     * Returns the connector's named action / procedure operations, if any.
     * Defaults to {@link Optional#empty()}; only connectors that declare
     * {@code ConnectorCapability.SUPPORTS_PROCEDURES} are expected to override.
     */
    default Optional<ConnectorActionOps> actionOps() {
        return Optional.empty();
    }

    /**
     * Returns the connector's MTMV (materialized-view-on-external-table)
     * operations, if any. Defaults to {@link Optional#empty()}; only
     * connectors that declare {@code ConnectorCapability.SUPPORTS_MTMV}
     * are expected to override.
     */
    default Optional<MtmvOps> mtmvOps() {
        return Optional.empty();
    }

    /**
     * Returns the connector's governance-policy hint operations, if any.
     * Defaults to {@link Optional#empty()}; only connectors that declare
     * {@code ConnectorCapability.SUPPORTS_RLS_HINT} or
     * {@code SUPPORTS_MASK_HINT} are expected to override.
     */
    default Optional<PolicyOps> policyOps() {
        return Optional.empty();
    }

    /**
     * Returns the connector's audit emission declaration, if any. Defaults
     * to {@link Optional#empty()}; only connectors that declare
     * {@code ConnectorCapability.EMITS_AUDIT_EVENTS} are expected to
     * override.
     */
    default Optional<ConnectorAuditOps> auditOps() {
        return Optional.empty();
    }

    @Override
    default void close() throws IOException {
    }
}
