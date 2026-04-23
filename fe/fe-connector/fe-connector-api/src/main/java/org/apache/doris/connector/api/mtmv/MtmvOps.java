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

package org.apache.doris.connector.api.mtmv;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.timetravel.ConnectorMvccSnapshot;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * D8 — Materialized-view-on-external-table (MTMV) plugin surface.
 *
 * <p>Implemented by connectors whose tables can serve as MTMV base tables.
 * Connectors must additionally declare
 * {@code ConnectorCapability.SUPPORTS_MTMV} for the engine to honour the
 * implementation. Connectors that return an {@link MtmvOps} instance without
 * the capability declared, or vice versa, are considered misconfigured.</p>
 *
 * <p>The plan-time {@link ConnectorMvccSnapshot} parameter (D5) is strictly
 * distinct from the refresh-time {@link ConnectorMtmvSnapshot}: the former
 * pins reads, the latter records freshness for change detection.</p>
 *
 * <p>Identity is carried as {@code (database, table)} string pair —
 * {@code ConnectorTableId} has not yet been introduced (deferred per M0-15).</p>
 */
public interface MtmvOps {

    /**
     * Lists partitions of the base table indexed by their plugin-defined
     * partition name. Returns a single {@link ConnectorPartitionItem.UnpartitionedItem}
     * entry for unpartitioned tables.
     */
    Map<String, ConnectorPartitionItem> listPartitions(
            String database, String table, Optional<ConnectorMvccSnapshot> snapshot);

    /** Returns the partition layout kind. */
    ConnectorPartitionType getPartitionType(
            String database, String table, Optional<ConnectorMvccSnapshot> snapshot);

    /** Returns the names of the partitioning columns. Empty when unpartitioned. */
    Set<String> getPartitionColumnNames(
            String database, String table, Optional<ConnectorMvccSnapshot> snapshot);

    /** Returns the partitioning columns with full type metadata. */
    List<ConnectorColumn> getPartitionColumns(
            String database, String table, Optional<ConnectorMvccSnapshot> snapshot);

    /** Returns the snapshot marker for a single partition at refresh time. */
    ConnectorMtmvSnapshot getPartitionSnapshot(
            String database, String table, String partitionName,
            MtmvRefreshHint hint, Optional<ConnectorMvccSnapshot> snapshot);

    /** Returns the table-level snapshot marker (used for unpartitioned tables). */
    ConnectorMtmvSnapshot getTableSnapshot(
            String database, String table,
            MtmvRefreshHint hint, Optional<ConnectorMvccSnapshot> snapshot);

    /**
     * Returns the newest plugin-side update version-or-time of the table, used
     * by the MTMV scheduler to fast-path "no change" decisions.
     */
    long getNewestUpdateVersionOrTime(String database, String table);

    /** Whether the partitioning columns admit NULL values. */
    boolean isPartitionColumnAllowNull(String database, String table);

    /**
     * Whether the table is currently a valid MV base table. Connectors should
     * return {@code false} if the partition spec has evolved in ways the
     * engine cannot reason about (e.g. Iceberg partition evolution), forcing
     * the planner to disable the materialization.
     */
    boolean isValidRelatedTable(String database, String table);

    /**
     * Whether the engine should auto-trigger refresh on detected upstream
     * changes for this table. Defaults to {@code true}.
     */
    default boolean needAutoRefresh(String database, String table) {
        return true;
    }
}
