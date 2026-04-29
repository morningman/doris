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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.write.ConnectorTransactionContext;
import org.apache.doris.connector.api.write.ConnectorTxnCapability;
import org.apache.doris.connector.api.write.WriteIntent;

import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchWriteBuilder;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Paimon-specific {@link ConnectorTransactionContext} (D1, M3-09).
 *
 * <p>Wraps the {@link BatchWriteBuilder} produced by
 * {@code Table#newBatchWriteBuilder()} so a future commit-model PR can
 * accumulate {@link org.apache.paimon.table.sink.CommitMessage}s across
 * statements before issuing a single
 * {@link org.apache.paimon.table.sink.BatchTableCommit#commit}.</p>
 *
 * <p>Capabilities advertised:</p>
 * <ul>
 *   <li>{@link ConnectorTxnCapability#MULTI_STATEMENT}: a single
 *       {@code BatchWriteBuilder} can collect commit messages from
 *       multiple staged BE writers before
 *       {@code BatchTableCommit#commit}.</li>
 *   <li>{@link ConnectorTxnCapability#COMMIT_RETRY}: paimon's
 *       snapshot-bumping commit can race; on PK conflicts the caller
 *       can rebuild a fresh commit and retry. Wiring of the retry
 *       policy is deferred to M3-15.</li>
 * </ul>
 *
 * <p>{@link ConnectorTxnCapability#FAILOVER_SAFE} is intentionally
 * <b>not</b> advertised: paimon snapshot bumps are atomic on disk, but
 * the staged BE-side files referenced by a half-committed
 * {@code BatchTableCommit} are not yet recoverable across coordinator
 * restarts via a portable snapshot encoder.</p>
 */
public final class PaimonTransactionContext implements ConnectorTransactionContext {

    private static final long serialVersionUID = 1L;

    static final Set<ConnectorTxnCapability> CAPABILITIES = EnumSet.of(
            ConnectorTxnCapability.MULTI_STATEMENT,
            ConnectorTxnCapability.COMMIT_RETRY);

    private final String dbName;
    private final String tableName;
    private final transient Table table;
    private final transient BatchWriteBuilder writeBuilder;
    private final WriteIntent intent;
    private final String txnId;
    private final String label;

    public PaimonTransactionContext(
            String dbName,
            String tableName,
            Table table,
            BatchWriteBuilder writeBuilder,
            WriteIntent intent) {
        this.dbName = Objects.requireNonNull(dbName, "dbName");
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.table = Objects.requireNonNull(table, "table");
        this.writeBuilder = Objects.requireNonNull(writeBuilder, "writeBuilder");
        this.intent = Objects.requireNonNull(intent, "intent");
        this.txnId = "paimon-" + UUID.randomUUID();
        this.label = "paimon:" + dbName + "." + tableName
                + ":" + intent.overwriteMode().name()
                + (intent.branch().isPresent() ? "@" + intent.branch().get() : "");
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
        throw new DorisConnectorException(
                "PaimonTransactionContext serialization deferred to the M3-15 cutover PR");
    }

    @Override
    public boolean supportsFailover() {
        return false;
    }

    @Override
    public Set<ConnectorTxnCapability> txnCapabilities() {
        return CAPABILITIES;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public WriteIntent getIntent() {
        return intent;
    }

    /** Package-private accessor for tests and future commit-model wiring. */
    Table getTable() {
        return table;
    }

    /** Package-private accessor for tests and future commit-model wiring. */
    BatchWriteBuilder getWriteBuilder() {
        return writeBuilder;
    }
}
