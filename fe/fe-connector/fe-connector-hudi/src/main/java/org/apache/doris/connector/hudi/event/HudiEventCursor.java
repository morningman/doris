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

package org.apache.doris.connector.hudi.event;

import org.apache.doris.connector.api.event.EventCursor;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * {@link EventCursor} for the Hudi self-managed timeline watcher.
 *
 * <p>The Hudi event source is self-managed (the engine never calls
 * {@code poll(...)}) so the cursor is mostly used by the dispatcher to
 * persist a graceful-restart anchor: a monotonic {@code tickId} bumped
 * by the watcher on every full pass, plus the last completed instant
 * timestamp seen for every known {@code (db, table)}.
 *
 * <p>Cursor binary blob magic: {@code 0xCE 0x01} (hive: no magic;
 * iceberg: {@code 0xCC 0x01}; paimon: {@code 0xCD 0x01}).
 */
public final class HudiEventCursor implements EventCursor {

    private final long tickId;
    private final SortedMap<String, String> lastInstantPerTable;

    public HudiEventCursor(long tickId, Map<String, String> lastInstantPerTable) {
        Objects.requireNonNull(lastInstantPerTable, "lastInstantPerTable");
        if (tickId < 0) {
            throw new IllegalArgumentException("tickId must be >= 0, got " + tickId);
        }
        this.tickId = tickId;
        this.lastInstantPerTable =
                Collections.unmodifiableSortedMap(new TreeMap<>(lastInstantPerTable));
    }

    public static HudiEventCursor empty() {
        return new HudiEventCursor(0L, Collections.emptySortedMap());
    }

    public long tickId() {
        return tickId;
    }

    public SortedMap<String, String> lastInstantPerTable() {
        return lastInstantPerTable;
    }

    @Override
    public int compareTo(EventCursor other) {
        if (!(other instanceof HudiEventCursor)) {
            throw new IllegalArgumentException(
                    "cannot compare HudiEventCursor against " + other.getClass().getName());
        }
        return Long.compare(this.tickId, ((HudiEventCursor) other).tickId);
    }

    @Override
    public String describe() {
        return "HudiEventCursor{tickId=" + tickId
                + ", tables=" + lastInstantPerTable.size() + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HudiEventCursor)) {
            return false;
        }
        HudiEventCursor that = (HudiEventCursor) o;
        return tickId == that.tickId && lastInstantPerTable.equals(that.lastInstantPerTable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tickId, lastInstantPerTable);
    }

    @Override
    public String toString() {
        return describe();
    }
}
