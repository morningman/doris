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

package org.apache.doris.connector.api.action;

import java.util.Objects;
import java.util.Optional;

/**
 * Target object of an action invocation. Used in lieu of a
 * {@code ConnectorTableId} type (not yet introduced in the connector
 * SPI); carries a database name and an optional table name.
 */
public final class ActionTarget {

    private final String database;
    private final String table;

    private ActionTarget(String database, String table) {
        Objects.requireNonNull(database, "database");
        if (database.isBlank()) {
            throw new IllegalArgumentException("database must not be blank");
        }
        if (table != null && table.isBlank()) {
            throw new IllegalArgumentException("table must not be blank when present");
        }
        this.database = database;
        this.table = table;
    }

    /** Creates a TABLE-scope target. */
    public static ActionTarget ofTable(String database, String table) {
        Objects.requireNonNull(table, "table");
        return new ActionTarget(database, table);
    }

    /** Creates a SCHEMA-scope target. */
    public static ActionTarget ofSchema(String database) {
        return new ActionTarget(database, null);
    }

    public String database() {
        return database;
    }

    public Optional<String> table() {
        return Optional.ofNullable(table);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ActionTarget)) {
            return false;
        }
        ActionTarget other = (ActionTarget) o;
        return database.equals(other.database) && Objects.equals(table, other.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, table);
    }

    @Override
    public String toString() {
        return "ActionTarget{database=" + database + ", table=" + Optional.ofNullable(table) + "}";
    }
}
