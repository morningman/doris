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

package org.apache.doris.connector.iceberg.event;

import org.apache.doris.connector.api.event.EventCursor;

import org.apache.iceberg.catalog.TableIdentifier;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * {@link EventCursor} for the Iceberg snapshot-polling event source.
 *
 * <p>Iceberg has no global notification log, so the cursor carries
 * <ul>
 *   <li>a monotonic {@code tickId} bumped on every successful poll —
 *       this is what the engine uses to compare cursors and detect
 *       progress (see {@link #compareTo(EventCursor)}); and</li>
 *   <li>a snapshot of every known {@code (db, table)} → last seen
 *       {@code TableState(snapshotId, schemaId)} so the next poll can
 *       diff against the catalog without re-emitting historical
 *       events.</li>
 * </ul>
 *
 * <p>The map is stored as a {@link SortedMap} so {@code equals} /
 * {@code hashCode} / serialization are stable across JVMs.</p>
 */
public final class IcebergEventCursor implements EventCursor {

    /** Per-table state carried in the cursor. */
    public static final class TableState {
        private final long snapshotId;
        private final int schemaId;

        public TableState(long snapshotId, int schemaId) {
            this.snapshotId = snapshotId;
            this.schemaId = schemaId;
        }

        public long snapshotId() {
            return snapshotId;
        }

        public int schemaId() {
            return schemaId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TableState)) {
                return false;
            }
            TableState that = (TableState) o;
            return snapshotId == that.snapshotId && schemaId == that.schemaId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(snapshotId, schemaId);
        }

        @Override
        public String toString() {
            return "TableState{snapshotId=" + snapshotId + ", schemaId=" + schemaId + "}";
        }
    }

    private final long tickId;
    private final SortedMap<String, TableState> tables;

    public IcebergEventCursor(long tickId, Map<String, TableState> tables) {
        Objects.requireNonNull(tables, "tables");
        if (tickId < 0) {
            throw new IllegalArgumentException("tickId must be >= 0, got " + tickId);
        }
        this.tickId = tickId;
        this.tables = Collections.unmodifiableSortedMap(new TreeMap<>(tables));
    }

    public long tickId() {
        return tickId;
    }

    public SortedMap<String, TableState> tables() {
        return tables;
    }

    /** Build the cursor map key for a {@link TableIdentifier}. */
    public static String key(String db, String table) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(table, "table");
        return db + "\u0001" + table;
    }

    public static String key(TableIdentifier id) {
        Objects.requireNonNull(id, "id");
        return key(id.namespace().toString(), id.name());
    }

    @Override
    public int compareTo(EventCursor other) {
        if (!(other instanceof IcebergEventCursor)) {
            throw new IllegalArgumentException(
                    "cannot compare IcebergEventCursor against " + other.getClass().getName());
        }
        return Long.compare(this.tickId, ((IcebergEventCursor) other).tickId);
    }

    @Override
    public String describe() {
        return "IcebergEventCursor{tickId=" + tickId + ", tables=" + tables.size() + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IcebergEventCursor)) {
            return false;
        }
        IcebergEventCursor that = (IcebergEventCursor) o;
        return tickId == that.tickId && tables.equals(that.tables);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tickId, tables);
    }

    @Override
    public String toString() {
        return describe();
    }
}
