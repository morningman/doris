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

package org.apache.doris.connector.api.write;

import java.util.Collections;
import java.util.Set;

/**
 * Opaque, plugin-owned transaction context produced when a write operation
 * begins (D1, design doc §2.2).
 *
 * <p>One {@code ConnectorTransactionContext} is created at the beginning
 * of an {@code INSERT / UPDATE / DELETE / MERGE} statement (see
 * {@link org.apache.doris.connector.api.ConnectorWriteOps#beginTransaction})
 * and survives until that statement commits or aborts. It carries
 * connector-specific state — Iceberg {@code Transaction}, Hive ACID
 * txn id / write id / lock id, Paimon {@code BatchWriteBuilder}, etc.</p>
 *
 * <p><b>M3-09 scope (this PR)</b>: declarative only. fe-core does not
 * yet consume the returned context — the existing
 * {@code beginInsert / finishInsert / abortInsert} signatures keep
 * working unchanged, and {@link #txnCapabilities()} is exposed so the
 * future commit-model refactor (M3-15 / cutover PRs) can route
 * MULTI_STATEMENT / COMMIT_RETRY / FAILOVER_SAFE based on connector
 * advertisements without re-touching every plugin.</p>
 */
public interface ConnectorTransactionContext extends java.io.Serializable {
    /** Returns the plugin-owned transaction id (stable for the lifetime of the txn). */
    String txnId();

    /** Returns the engine-visible label for this transaction (e.g., for EXPLAIN / logs). */
    String label();

    /** Serializes the context for FE→BE transport or failover persistence. */
    byte[] serialize();

    /**
     * Returns {@code true} if this context can be safely reconstructed after a
     * coordinator failover from the bytes returned by {@link #serialize()}.
     */
    boolean supportsFailover();

    /**
     * Returns the per-context transaction capabilities. Must be a subset of
     * the connector-level capabilities reported by
     * {@link org.apache.doris.connector.api.ConnectorWriteOps#txnCapabilities()}.
     *
     * <p>Plugins MAY return narrower per-table sets — e.g., the hive plugin
     * advertises {@code MULTI_STATEMENT, FAILOVER_SAFE} at connector level
     * but returns the empty set when the target table is non-ACID.</p>
     *
     * <p>Default returns the empty set, mirroring single-statement
     * non-failover-safe semantics.</p>
     */
    default Set<ConnectorTxnCapability> txnCapabilities() {
        return Collections.emptySet();
    }
}
