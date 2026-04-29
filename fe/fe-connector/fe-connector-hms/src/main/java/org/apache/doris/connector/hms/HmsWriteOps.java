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

package org.apache.doris.connector.hms;

import java.util.List;

/**
 * Phase 3 / Phase 4 write surface of the HMS client.
 *
 * <p>Kept as a separate interface from {@link HmsClient} so connector
 * read paths can keep depending only on the read-only metadata API
 * while plugin write paths cast the same client to {@code HmsWriteOps}
 * when an actual write is requested. Tests typically mock this
 * interface directly to drive the plugin commit driver without a real
 * metastore.</p>
 *
 * <p>The ACID block (open/commit/abort transaction, allocate write id,
 * acquire/heartbeat lock, register dynamic partitions) mirrors the
 * upstream {@code IMetaStoreClient} surface used by
 * {@code fe-core/.../HiveTransaction.java} so semantics stay
 * one-to-one with the legacy code path.</p>
 */
public interface HmsWriteOps {

    // ──────────────────── Phase 3: non-ACID partition lifecycle ────────────────────

    /**
     * Registers new partitions in the metastore.
     *
     * @param dbName     database name
     * @param tableName  table name
     * @param partitions partition specs to add (may be empty: no-op)
     */
    void addPartitions(String dbName, String tableName, List<HmsPartitionSpec> partitions);

    /**
     * Drops a single partition. {@code deleteData=true} asks the
     * metastore to delete the underlying files; the plugin commit
     * driver always passes {@code false} and manages files itself.
     */
    void dropPartition(String dbName, String tableName, List<String> values, boolean deleteData);

    /**
     * Re-points an existing partition at {@code newLocation} (used by
     * {@code INSERT OVERWRITE PARTITION} when the partition pre-exists
     * and the plugin staged the new data outside its original directory).
     */
    void alterPartitionLocation(String dbName, String tableName,
            List<String> values, String newLocation);

    // ──────────────────── Phase 4: ACID transaction lifecycle ────────────────────

    /** Opens a new HMS transaction. Returns the txn id. */
    long openTxn(String user);

    /** Commits the given HMS txn. */
    void commitTxn(long txnId);

    /** Aborts the given HMS txn. Best-effort — exceptions are translated to {@link HmsClientException}. */
    void abortTxn(long txnId);

    /** Allocates a write id for {@code (db.table)} under the open {@code txnId}. */
    long allocateWriteId(long txnId, String dbName, String tableName);

    /**
     * Acquires a table-level (and partition-level) lock for the txn.
     *
     * @param sharedWrite when {@code true} requests {@code SHARED_WRITE}
     *                    semantics (concurrent INSERTs allowed); when
     *                    {@code false} requests {@code EXCLUSIVE}, used
     *                    by {@code INSERT OVERWRITE} on ACID tables.
     * @return the metastore lock id
     */
    long acquireLock(long txnId, String user, String queryId,
            String dbName, String tableName, boolean sharedWrite);

    /**
     * Sends a single heartbeat for {@code (txnId, lockId)}. The plugin
     * commit driver schedules this on its own executor; the HMS client
     * implementation just forwards to {@code IMetaStoreClient#heartbeat}.
     */
    void heartbeat(long txnId, long lockId);

    /** Registers the partitions touched by the write so the metastore can update its compactor view. */
    void addDynamicPartitions(long txnId, long writeId, String dbName, String tableName,
            List<String> partitionNames, HmsAcidOperation op);
}
