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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.write.ConnectorTransactionContext;
import org.apache.doris.connector.api.write.ConnectorTxnCapability;
import org.apache.doris.connector.api.write.WriteIntent;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/**
 * Hive-specific {@link ConnectorTransactionContext} (D1, M3-09).
 *
 * <p>Wraps a {@link HiveAcidContext} so the same per-statement state
 * the existing {@code beginInsert / finishInsert / abortInsert} path
 * already keeps (open HMS txn id, write id, lock id, heartbeat future
 * for ACID; staging location for non-ACID) becomes the canonical
 * txn-context handle the future commit-model PR will route through.</p>
 *
 * <p>Capabilities advertised at the connector level via
 * {@link HiveConnectorMetadata#txnCapabilities()} are
 * {@code MULTI_STATEMENT, FAILOVER_SAFE} — the union of what an ACID
 * table transaction may need. Per-context capabilities returned by
 * {@link #txnCapabilities()} narrow that set:</p>
 * <ul>
 *   <li><b>ACID tables</b>:
 *       {@link ConnectorTxnCapability#MULTI_STATEMENT} (HMS txns can
 *       carry multiple write ids over one txnId) +
 *       {@link ConnectorTxnCapability#FAILOVER_SAFE} (HMS
 *       {@code commit_txn} is atomic; the (txnId, writeId, lockId)
 *       triple is enough to resume on a new coordinator).</li>
 *   <li><b>Non-ACID tables</b>: empty — legacy non-atomic FS rename
 *       commits cannot be retried by HMS, are not multi-statement, and
 *       have no durable failover handle.</li>
 * </ul>
 *
 * <p>{@link ConnectorTxnCapability#COMMIT_RETRY} is intentionally
 * <b>not</b> advertised even for ACID tables: HMS rejects duplicate
 * {@code commit_txn} calls on the same txnId, so retry must rebuild a
 * fresh transaction rather than re-issue the failed commit.</p>
 */
public final class HiveTransactionContext implements ConnectorTransactionContext {

    private static final long serialVersionUID = 1L;

    static final Set<ConnectorTxnCapability> CONNECTOR_CAPABILITIES = EnumSet.of(
            ConnectorTxnCapability.MULTI_STATEMENT,
            ConnectorTxnCapability.FAILOVER_SAFE);

    static final Set<ConnectorTxnCapability> ACID_CAPABILITIES = EnumSet.of(
            ConnectorTxnCapability.MULTI_STATEMENT,
            ConnectorTxnCapability.FAILOVER_SAFE);

    static final Set<ConnectorTxnCapability> NON_ACID_CAPABILITIES = Collections.emptySet();

    private final transient HiveAcidContext acidContext;
    private final String txnId;
    private final String label;
    private final Set<ConnectorTxnCapability> capabilities;

    public HiveTransactionContext(HiveAcidContext acidContext) {
        this.acidContext = Objects.requireNonNull(acidContext, "acidContext");
        if (acidContext.isAcid()) {
            // Stable, plugin-defined id == HMS-allocated txnId.
            this.txnId = "hive-acid-" + acidContext.getTxnId();
            this.capabilities = ACID_CAPABILITIES;
        } else {
            this.txnId = "hive-fs-" + acidContext.getDbName() + "."
                    + acidContext.getTableName() + "-"
                    + System.nanoTime();
            this.capabilities = NON_ACID_CAPABILITIES;
        }
        WriteIntent intent = acidContext.getIntent();
        this.label = "hive:" + acidContext.getDbName() + "." + acidContext.getTableName()
                + ":" + intent.overwriteMode().name()
                + (acidContext.isAcid() ? ":acid" : ":fs");
    }

    @Override
    public String txnId() {
        return txnId;
    }

    @Override
    public String label() {
        return label;
    }

    @Override
    public byte[] serialize() {
        // ACID context is failover-safe in principle (HMS state is
        // durable) but the wire format is not yet defined; the M3-15
        // cutover PR will introduce a (txnId, writeId, lockId)
        // encoding. For now refuse explicitly so any premature caller
        // crashes loudly instead of silently losing transactional state.
        throw new DorisConnectorException(
                "HiveTransactionContext serialization deferred to the M3-15 cutover PR");
    }

    @Override
    public boolean supportsFailover() {
        // See serialize(): capability is advertised, wire format pending.
        return false;
    }

    @Override
    public Set<ConnectorTxnCapability> txnCapabilities() {
        return capabilities;
    }

    public boolean isAcid() {
        return acidContext.isAcid();
    }

    /** Package-private accessor for tests and future commit-model wiring. */
    HiveAcidContext getAcidContext() {
        return acidContext;
    }
}
