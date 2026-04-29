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

import org.apache.doris.connector.api.write.WriteIntent;
import org.apache.doris.connector.hms.HmsAcidOperation;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Plugin-side state shared by an in-flight Hive INSERT / DELETE / MERGE.
 *
 * <p>Carries the table coordinates, the resolved {@link WriteIntent},
 * the BE-visible staging location, and (when the underlying table is
 * an ACID table) the open transaction id, write id, lock id, and the
 * {@link ScheduledFuture} backing the heartbeat the metadata layer
 * scheduled at {@code beginInsert} time. Mutable only through the
 * {@link Builder} and {@link #cancelHeartbeat()} helper.</p>
 *
 * <p>For non-ACID tables {@link #isAcid()} is {@code false} and the
 * txn / write id / lock fields are zero — no HMS txn calls happen on
 * those paths, mirroring the legacy {@code HMSTransaction} which only
 * touches HMS metadata (add_partitions / drop_partition /
 * alter_partition).</p>
 */
public final class HiveAcidContext {

    private final String dbName;
    private final String tableName;
    private final WriteIntent intent;
    private final String stagingLocation;
    private final boolean isAcid;
    private final HmsAcidOperation acidOperation;
    private final long txnId;
    private final long writeId;
    private final long lockId;
    private final ScheduledFuture<?> heartbeatFuture;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private HiveAcidContext(Builder b) {
        this.dbName = Objects.requireNonNull(b.dbName, "dbName");
        this.tableName = Objects.requireNonNull(b.tableName, "tableName");
        this.intent = Objects.requireNonNull(b.intent, "intent");
        this.stagingLocation = b.stagingLocation;
        this.isAcid = b.isAcid;
        this.acidOperation = b.acidOperation;
        this.txnId = b.txnId;
        this.writeId = b.writeId;
        this.lockId = b.lockId;
        this.heartbeatFuture = b.heartbeatFuture;
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

    public String getStagingLocation() {
        return stagingLocation;
    }

    public boolean isAcid() {
        return isAcid;
    }

    public HmsAcidOperation getAcidOperation() {
        return acidOperation;
    }

    public long getTxnId() {
        return txnId;
    }

    public long getWriteId() {
        return writeId;
    }

    public long getLockId() {
        return lockId;
    }

    public Optional<ScheduledFuture<?>> getHeartbeatFuture() {
        return Optional.ofNullable(heartbeatFuture);
    }

    /**
     * Cancels the heartbeat task once. Returns {@code true} if this
     * call is the one that performed the cancellation, {@code false}
     * if a previous {@code cancelHeartbeat} (or finishInsert /
     * abortInsert) already cancelled it.
     */
    public boolean cancelHeartbeat() {
        if (!closed.compareAndSet(false, true)) {
            return false;
        }
        if (heartbeatFuture != null) {
            heartbeatFuture.cancel(false);
        }
        return true;
    }

    public static Builder builder(String dbName, String tableName, WriteIntent intent) {
        return new Builder(dbName, tableName, intent);
    }

    /** Mutable builder. */
    public static final class Builder {
        private final String dbName;
        private final String tableName;
        private final WriteIntent intent;
        private String stagingLocation;
        private boolean isAcid;
        private HmsAcidOperation acidOperation;
        private long txnId;
        private long writeId;
        private long lockId;
        private ScheduledFuture<?> heartbeatFuture;

        private Builder(String dbName, String tableName, WriteIntent intent) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.intent = intent;
        }

        public Builder stagingLocation(String v) {
            this.stagingLocation = v;
            return this;
        }

        public Builder acid(boolean v) {
            this.isAcid = v;
            return this;
        }

        public Builder acidOperation(HmsAcidOperation v) {
            this.acidOperation = v;
            return this;
        }

        public Builder txnId(long v) {
            this.txnId = v;
            return this;
        }

        public Builder writeId(long v) {
            this.writeId = v;
            return this;
        }

        public Builder lockId(long v) {
            this.lockId = v;
            return this;
        }

        public Builder heartbeatFuture(ScheduledFuture<?> v) {
            this.heartbeatFuture = v;
            return this;
        }

        public HiveAcidContext build() {
            return new HiveAcidContext(this);
        }
    }
}
