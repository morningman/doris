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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.write.ConnectorTransactionContext;
import org.apache.doris.connector.api.write.ConnectorTxnCapability;
import org.apache.doris.connector.api.write.WriteIntent;

import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Iceberg-specific {@link ConnectorTransactionContext} (D1, M3-09).
 *
 * <p>Wraps the {@link Transaction} produced by
 * {@code table.newTransaction()} so a future commit-model PR can keep
 * staging {@code AppendFiles / OverwriteFiles / RowDelta} operations
 * across multiple statements before a single
 * {@link Transaction#commitTransaction()}.</p>
 *
 * <p>Capabilities advertised:</p>
 * <ul>
 *   <li>{@link ConnectorTxnCapability#MULTI_STATEMENT}: iceberg
 *       {@code Transaction} accumulates pending updates and commits
 *       atomically on demand.</li>
 *   <li>{@link ConnectorTxnCapability#COMMIT_RETRY}: iceberg
 *       {@code commit()} surfaces {@code CommitFailedException} on
 *       optimistic concurrency conflicts; the operation type chosen by
 *       {@code IcebergConnectorMetadata#finishInsert} (AppendFiles /
 *       RowDelta / ReplacePartitions) honours iceberg's
 *       {@code commit.retry.num-retries} property.</li>
 *   <li>{@link ConnectorTxnCapability#FAILOVER_SAFE}: iceberg
 *       table-metadata mutation is a single atomic
 *       {@code metadata.json} swap on the catalog; once committed, a
 *       failed-over coordinator observes the new snapshot deterministically
 *       on the next read.</li>
 * </ul>
 */
public final class IcebergTransactionContext implements ConnectorTransactionContext {

    private static final long serialVersionUID = 1L;

    static final Set<ConnectorTxnCapability> CAPABILITIES = EnumSet.of(
            ConnectorTxnCapability.MULTI_STATEMENT,
            ConnectorTxnCapability.COMMIT_RETRY,
            ConnectorTxnCapability.FAILOVER_SAFE);

    private final String dbName;
    private final String tableName;
    private final transient Table table;
    private final transient Transaction transaction;
    private final WriteIntent intent;
    private final String txnId;
    private final String label;

    public IcebergTransactionContext(
            String dbName,
            String tableName,
            Table table,
            Transaction transaction,
            WriteIntent intent) {
        this.dbName = Objects.requireNonNull(dbName, "dbName");
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.table = Objects.requireNonNull(table, "table");
        this.transaction = Objects.requireNonNull(transaction, "transaction");
        this.intent = Objects.requireNonNull(intent, "intent");
        this.txnId = "iceberg-" + UUID.randomUUID();
        this.label = "iceberg:" + dbName + "." + tableName
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
        // Iceberg Transaction state lives entirely inside the iceberg
        // Catalog's metadata.json swap. M3-09 deliberately does not
        // ship a wire format because fe-core does not yet persist the
        // context. The follow-up M3-15 PR will replace this with a
        // catalog-coordinate + pending-operation snapshot encoder.
        throw new DorisConnectorException(
                "IcebergTransactionContext serialization deferred to the M3-15 cutover PR");
    }

    @Override
    public boolean supportsFailover() {
        // Capability is advertised at the connector / context level
        // (iceberg metadata.json commit is atomic) but the transient
        // Transaction handle itself cannot resume across a coordinator
        // restart until M3-15 ships the snapshot encoder.
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
    Transaction getTransaction() {
        return transaction;
    }
}
