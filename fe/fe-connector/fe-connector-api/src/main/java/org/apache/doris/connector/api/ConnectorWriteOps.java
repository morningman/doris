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

import org.apache.doris.connector.api.handle.ConnectorDeleteHandle;
import org.apache.doris.connector.api.handle.ConnectorInsertHandle;
import org.apache.doris.connector.api.handle.ConnectorMergeHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.write.ConnectorTransactionContext;
import org.apache.doris.connector.api.write.ConnectorTxnCapability;
import org.apache.doris.connector.api.write.ConnectorWriteConfig;
import org.apache.doris.connector.api.write.NoopTransactionContext;
import org.apache.doris.connector.api.write.WriteIntent;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

/**
 * Write (DML) operations that a connector may support.
 *
 * <p>Follows a two-phase lifecycle for each write operation:</p>
 * <ol>
 *   <li>{@code begin*} — initialize the write, return an opaque handle</li>
 *   <li>{@code finish*} — commit using collected BE fragments; or {@code abort*} on failure</li>
 * </ol>
 *
 * <p>All methods have default implementations that throw
 * {@link DorisConnectorException}, so connectors only override what they support.</p>
 */
public interface ConnectorWriteOps {

    // ──────────────────── Capability Queries ────────────────────

    /** Returns {@code true} if this connector supports INSERT operations. */
    default boolean supportsInsert() {
        return false;
    }

    /** Returns {@code true} if this connector supports DELETE operations. */
    default boolean supportsDelete() {
        return false;
    }

    /** Returns {@code true} if this connector supports MERGE (INSERT + DELETE) operations. */
    default boolean supportsMerge() {
        return false;
    }

    // ──────────────────── Write Configuration ────────────────────

    /**
     * Returns the write configuration for this table.
     *
     * <p>The engine uses the returned {@link ConnectorWriteConfig} to select the
     * appropriate Thrift data sink type and pass properties to BE.</p>
     *
     * @param session current session
     * @param handle the target table handle
     * @param columns the columns being written (ordered to match INSERT column list)
     * @return write configuration describing sink type, format, location, etc.
     */
    default ConnectorWriteConfig getWriteConfig(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumn> columns) {
        throw new DorisConnectorException("Write not supported");
    }

    // ──────────────────── INSERT ────────────────────

    /**
     * Begins an insert operation and returns an opaque handle.
     *
     * @param session current session
     * @param handle the target table handle
     * @param columns the columns being inserted (ordered to match INSERT column list)
     * @return an opaque insert handle carrying connector-internal state
     */
    default ConnectorInsertHandle beginInsert(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumn> columns) {
        throw new DorisConnectorException("INSERT not supported");
    }

    /**
     * Begins an insert operation with an explicit {@link WriteIntent},
     * allowing the connector to honor OverwriteMode / UPSERT / branch /
     * staticPartitions / DeleteMode / writeAtVersion.
     *
     * <p>Default delegates to the 3-arg {@link #beginInsert} and ignores
     * the intent, preserving v1 semantics. Connectors that declare any
     * of {@code SUPPORTS_INSERT_OVERWRITE / SUPPORTS_UPSERT / ...}
     * MUST override this overload.</p>
     *
     * @param session current session
     * @param handle the target table handle
     * @param columns the columns being inserted (ordered to match INSERT column list)
     * @param intent the high-level write intent (must not be null)
     * @return an opaque insert handle carrying connector-internal state
     */
    default ConnectorInsertHandle beginInsert(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumn> columns,
            WriteIntent intent) {
        Objects.requireNonNull(intent, "intent");
        return beginInsert(session, handle, columns);
    }

    /**
     * Returns the set of transaction capabilities this connector supports.
     * Default is the empty set, meaning only single-statement transactions.
     */
    default EnumSet<ConnectorTxnCapability> txnCapabilities() {
        return EnumSet.noneOf(ConnectorTxnCapability.class);
    }

    /**
     * Opens a fresh {@link ConnectorTransactionContext} for an upcoming
     * DML statement (D1 §2.3, M3-09).
     *
     * <p>Default returns a {@link NoopTransactionContext} so connectors
     * that do not (yet) honour the new commit-model SPI keep working
     * untouched. Connectors that advertise any of
     * {@link ConnectorTxnCapability#MULTI_STATEMENT},
     * {@link ConnectorTxnCapability#COMMIT_RETRY} or
     * {@link ConnectorTxnCapability#FAILOVER_SAFE} via
     * {@link #txnCapabilities()} MUST override this method and return a
     * context whose {@link ConnectorTransactionContext#txnCapabilities()}
     * is a subset of the connector-level set.</p>
     *
     * <p><b>M3-09 scope</b>: the returned context is currently only
     * inspected for declarative purposes. fe-core's
     * {@code PluginDrivenInsertExecutor} continues to drive
     * {@code beginInsert / finishInsert / abortInsert} on the existing
     * {@link org.apache.doris.connector.api.handle.ConnectorInsertHandle}
     * path; wiring the context into commit/abort retry and failover
     * resumption is deferred to the M3-15 cutover PR.</p>
     */
    default ConnectorTransactionContext beginTransaction(
            ConnectorSession session,
            ConnectorTableHandle handle,
            WriteIntent intent) {
        Objects.requireNonNull(intent, "intent");
        return NoopTransactionContext.forIntent(intent);
    }

    /**
     * Commits the insert operation using collected fragments from BE.
     *
     * @param session current session
     * @param handle the insert handle from {@link #beginInsert}
     * @param fragments serialized commit info collected from BE nodes
     */
    default void finishInsert(ConnectorSession session,
            ConnectorInsertHandle handle,
            Collection<byte[]> fragments) {
        throw new DorisConnectorException("INSERT not supported");
    }

    /**
     * Aborts a previously started insert operation.
     * Called on failure to clean up any partial writes.
     *
     * @param session current session
     * @param handle the insert handle from {@link #beginInsert}
     */
    default void abortInsert(ConnectorSession session,
            ConnectorInsertHandle handle) {
        // default: no-op — connector may not require explicit cleanup
    }

    // ──────────────────── DELETE ────────────────────

    /**
     * Begins a delete operation and returns an opaque handle.
     *
     * @param session current session
     * @param handle the target table handle
     * @return an opaque delete handle
     */
    default ConnectorDeleteHandle beginDelete(
            ConnectorSession session,
            ConnectorTableHandle handle) {
        throw new DorisConnectorException("DELETE not supported");
    }

    /**
     * Commits the delete operation using collected fragments.
     *
     * @param session current session
     * @param handle the delete handle from {@link #beginDelete}
     * @param fragments serialized commit info collected from BE nodes
     */
    default void finishDelete(ConnectorSession session,
            ConnectorDeleteHandle handle,
            Collection<byte[]> fragments) {
        throw new DorisConnectorException("DELETE not supported");
    }

    /**
     * Aborts a previously started delete operation.
     *
     * @param session current session
     * @param handle the delete handle from {@link #beginDelete}
     */
    default void abortDelete(ConnectorSession session,
            ConnectorDeleteHandle handle) {
        // default: no-op
    }

    // ──────────────────── MERGE (INSERT + DELETE) ────────────────────

    /**
     * Begins a merge (combined insert+delete) operation.
     * Used by connectors that support merge-on-read (e.g., Iceberg).
     *
     * @param session current session
     * @param handle the target table handle
     * @return an opaque merge handle
     */
    default ConnectorMergeHandle beginMerge(
            ConnectorSession session,
            ConnectorTableHandle handle) {
        throw new DorisConnectorException("MERGE not supported");
    }

    /**
     * Commits the merge operation using collected fragments.
     *
     * @param session current session
     * @param handle the merge handle from {@link #beginMerge}
     * @param fragments serialized commit info collected from BE nodes
     */
    default void finishMerge(ConnectorSession session,
            ConnectorMergeHandle handle,
            Collection<byte[]> fragments) {
        throw new DorisConnectorException("MERGE not supported");
    }

    /**
     * Aborts a previously started merge operation.
     *
     * @param session current session
     * @param handle the merge handle from {@link #beginMerge}
     */
    default void abortMerge(ConnectorSession session,
            ConnectorMergeHandle handle) {
        // default: no-op
    }
}
