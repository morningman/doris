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

package org.apache.doris.connector.api.event;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Immutable filter applied to {@link EventSourceOps#poll(EventCursor, int,
 * java.time.Duration, EventFilter)} so plugins may skip events the engine
 * does not care about.
 *
 * <p>{@link #ALL} matches every event; otherwise the filter matches an
 * event if its database is in {@link #databases()} or its
 * {@code (database, table)} pair is in {@link #tables()}.</p>
 */
public final class EventFilter {

    /** Match-everything sentinel. */
    public static final EventFilter ALL = new EventFilter(Set.of(), Set.of(), true);

    private final Set<String> databases;
    private final Set<TableIdentifier> tables;
    private final boolean catchAll;

    public EventFilter(Set<String> databases, Set<TableIdentifier> tables, boolean catchAll) {
        Objects.requireNonNull(databases, "databases");
        Objects.requireNonNull(tables, "tables");
        this.databases = Set.copyOf(databases);
        this.tables = Set.copyOf(tables);
        this.catchAll = catchAll;
    }

    public Set<String> databases() {
        return databases;
    }

    public Set<TableIdentifier> tables() {
        return tables;
    }

    public boolean catchAll() {
        return catchAll;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final Set<String> databases = new HashSet<>();
        private final Set<TableIdentifier> tables = new HashSet<>();
        private boolean catchAll;

        public Builder database(String db) {
            databases.add(Objects.requireNonNull(db, "db"));
            return this;
        }

        public Builder table(TableIdentifier id) {
            tables.add(Objects.requireNonNull(id, "id"));
            return this;
        }

        public Builder catchAll(boolean v) {
            this.catchAll = v;
            return this;
        }

        public EventFilter build() {
            return new EventFilter(databases, tables, catchAll);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EventFilter)) {
            return false;
        }
        EventFilter that = (EventFilter) o;
        return catchAll == that.catchAll && databases.equals(that.databases) && tables.equals(that.tables);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databases, tables, catchAll);
    }

    @Override
    public String toString() {
        return "EventFilter{databases=" + databases + ", tables=" + tables + ", catchAll=" + catchAll + "}";
    }
}
