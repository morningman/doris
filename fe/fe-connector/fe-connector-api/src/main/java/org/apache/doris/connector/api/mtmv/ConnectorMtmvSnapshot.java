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

/**
 * Refresh-time snapshot marker for an MTMV base table or partition.
 *
 * <p>Strictly distinct from
 * {@code org.apache.doris.connector.api.timetravel.ConnectorMvccSnapshot}:
 * an {@link ConnectorMtmvSnapshot} is recorded by the MTMV refresh job and
 * later compared against a freshly produced one to decide whether the
 * materialization is still up-to-date. Two snapshots are considered equal
 * iff their permitted subtype matches and {@link #marker()} matches.</p>
 */
public sealed interface ConnectorMtmvSnapshot
        permits ConnectorMtmvSnapshot.VersionMtmvSnapshot,
                ConnectorMtmvSnapshot.TimestampMtmvSnapshot,
                ConnectorMtmvSnapshot.MaxTimestampMtmvSnapshot,
                ConnectorMtmvSnapshot.SnapshotIdMtmvSnapshot {

    /** Numeric marker used for equality / ordering. Semantics vary per subtype. */
    long marker();

    /** Monotonic plugin-side version number (e.g. Hive partition version). */
    record VersionMtmvSnapshot(long version) implements ConnectorMtmvSnapshot {
        @Override
        public long marker() {
            return version;
        }
    }

    /** Last commit / update epoch-millis for the partition or table. */
    record TimestampMtmvSnapshot(long epochMillis) implements ConnectorMtmvSnapshot {
        @Override
        public long marker() {
            return epochMillis;
        }
    }

    /** Maximum epoch-millis across all underlying files (Hive-style mtime). */
    record MaxTimestampMtmvSnapshot(long epochMillis) implements ConnectorMtmvSnapshot {
        @Override
        public long marker() {
            return epochMillis;
        }
    }

    /** Iceberg-style numeric snapshot id (or any opaque numeric snapshot key). */
    record SnapshotIdMtmvSnapshot(long snapshotId) implements ConnectorMtmvSnapshot {
        @Override
        public long marker() {
            return snapshotId;
        }
    }
}
