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

/**
 * Enumerates the optional capabilities a connector may declare.
 * The planner and execution engine use these to decide which
 * pushdown and write paths are available.
 */
public enum ConnectorCapability {
    SUPPORTS_FILTER_PUSHDOWN,
    SUPPORTS_PROJECTION_PUSHDOWN,
    SUPPORTS_LIMIT_PUSHDOWN,
    SUPPORTS_PARTITION_PRUNING,
    SUPPORTS_INSERT,
    SUPPORTS_DELETE,
    SUPPORTS_UPDATE,
    SUPPORTS_MERGE,
    SUPPORTS_CREATE_TABLE,
    SUPPORTS_MVCC_SNAPSHOT,
    SUPPORTS_METASTORE_EVENTS,
    SUPPORTS_STATISTICS,
    SUPPORTS_VENDED_CREDENTIALS,
    SUPPORTS_ACID_TRANSACTIONS,
    SUPPORTS_TIME_TRAVEL,
    /**
     * Indicates the connector supports multiple concurrent writers (sink instances).
     *
     * <p>Connectors that do NOT declare this capability will use GATHER distribution
     * (single writer), which is the safe default for transactional sinks like JDBC
     * where each writer commits independently.</p>
     *
     * <p>File-based connectors (Hive, Iceberg, etc.) that can safely handle
     * parallel writers should declare this capability.</p>
     */
    SUPPORTS_PARALLEL_WRITE,
    /**
     * Indicates the connector supports passthrough query via the {@code query()} TVF.
     *
     * <p>Connectors declaring this capability must implement
     * {@link ConnectorTableOps#getColumnsFromQuery} to provide column metadata
     * for arbitrary SQL queries passed through to the remote data source.</p>
     */
    SUPPORTS_PASSTHROUGH_QUERY,

    // === D1 write-path extensions (M0-01 reserved, impl in M3) ===
    /** D1: Connector supports {@code INSERT OVERWRITE} replacing whole table contents. */
    SUPPORTS_INSERT_OVERWRITE,
    /** D1: Connector supports overwriting a specific set of partitions atomically. */
    SUPPORTS_PARTITION_OVERWRITE,
    /** D1: Connector supports {@code INSERT} that dynamically creates partitions on demand. */
    SUPPORTS_DYNAMIC_PARTITION_INSERT,
    /** D1: Connector supports inserts that participate in an explicit external transaction. */
    SUPPORTS_TXN_INSERT,
    /** D1: Connector supports row-level UPSERT (merge on primary key). */
    SUPPORTS_UPSERT,
    /** D1: Connector supports row-level DELETE expressed as predicates. */
    SUPPORTS_ROW_LEVEL_DELETE,
    /** D1: Connector supports position-based delete files (e.g. Iceberg position deletes). */
    SUPPORTS_POSITION_DELETE,
    /** D1: Connector supports equality-based delete files (e.g. Iceberg equality deletes). */
    SUPPORTS_EQUALITY_DELETE,
    /** D1: Connector supports deletion vectors (e.g. Delta / Paimon DV). */
    SUPPORTS_DELETION_VECTOR,
    /** D1: Connector supports {@code MERGE INTO} as a single atomic statement. */
    SUPPORTS_MERGE_INTO,
    /** D1: Connector exposes callable stored procedures via the procedure SPI. */
    SUPPORTS_PROCEDURES,
    /** D1: Connector supports named branches/tags (e.g. Iceberg refs). */
    SUPPORTS_BRANCH_TAG,
    /** D1: Connector supports writes targeted at a historical snapshot / time-travel write. */
    SUPPORTS_TIME_TRAVEL_WRITE,
    /** D1: Connector provides failover-safe transactional commit semantics. */
    SUPPORTS_FAILOVER_SAFE_TXN,

    // === D6 system tables (M0-01 reserved, impl in M1) ===
    /** D6: Connector exposes system / metadata tables (umbrella capability). */
    SUPPORTS_SYSTEM_TABLES,
    /** D6: System tables are served natively by the connector via standard table reads. */
    SUPPORTS_NATIVE_SYS_TABLES,
    /** D6: System tables are served via table-valued functions (TVF) instead of native reads. */
    SUPPORTS_TVF_SYS_TABLES,

    // === D7 events (M0-01 reserved, impl in M2) ===
    /** D7: Connector supports pull-based metastore event polling. */
    SUPPORTS_PULL_EVENTS,
    /** D7: Connector supports push-based metastore event delivery. */
    SUPPORTS_PUSH_EVENTS,
    /** D7: Connector manages its own event loop and does not need the framework scheduler. */
    REQUIRES_SELF_MANAGED_EVENT_LOOP,
    /** D7: Connector supports per-table event filtering at the source. */
    SUPPORTS_PER_TABLE_FILTER,
    /** D7: Connector emits ref (branch/tag) change events. */
    EMITS_REF_EVENTS,
    /** D7: Connector emits data-changed events that include snapshot identifiers. */
    EMITS_DATA_CHANGED_WITH_SNAPSHOT,

    // === D8 governance (M0-01 reserved, impl in M2) ===
    /** D8: Connector supports materialized-view-on-external-table (MTMV) base tables. */
    SUPPORTS_MTMV,
    /** D8: Connector can supply row-level security predicates as planner hints. */
    SUPPORTS_RLS_HINT,
    /** D8: Connector can supply column-masking expressions as planner hints. */
    SUPPORTS_MASK_HINT,
    /** D8: Connector emits audit events for governance/compliance pipelines. */
    EMITS_AUDIT_EVENTS,

    // === D10 dispatch (M0-01 reserved, impl in M4) ===
    /** D10: Connector emits tables that may be delegated to another connector for execution. */
    EMITS_DELEGATABLE_TABLES,
    /** D10: Connector can accept delegated tables originating from a Hive Metastore connector. */
    ACCEPTS_DELEGATION_FROM_HMS,
    /** D10: Connector supports listing delegated tables on behalf of another connector. */
    SUPPORTS_DELEGATING_LISTING
}
