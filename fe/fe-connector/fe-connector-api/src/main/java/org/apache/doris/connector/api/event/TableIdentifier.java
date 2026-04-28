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

import org.apache.doris.connector.api.ConnectorTableId;

import java.util.Objects;

/**
 * Lightweight {@code (database, table)} value pair used to scope event
 * filtering on {@link EventFilter}.
 *
 * <p>This is intentionally <strong>not</strong> the typed
 * {@link ConnectorTableId} used across the rest of the SPI; it is a minimal
 * pair purpose-built for event subscription and persisted as an event-payload
 * key. Use {@link #fromConnectorTableId} / {@link #toConnectorTableId} to
 * convert between the two surfaces.</p>
 */
public final class TableIdentifier {
    private final String database;
    private final String table;

    public TableIdentifier(String database, String table) {
        this.database = Objects.requireNonNull(database, "database");
        this.table = Objects.requireNonNull(table, "table");
    }

    /** Builds an event-payload {@code TableIdentifier} from a typed {@link ConnectorTableId}. */
    public static TableIdentifier fromConnectorTableId(ConnectorTableId id) {
        Objects.requireNonNull(id, "id");
        return new TableIdentifier(id.database(), id.table());
    }

    public String database() {
        return database;
    }

    public String table() {
        return table;
    }

    /** Returns the typed {@link ConnectorTableId} for this event-payload key. */
    public ConnectorTableId toConnectorTableId() {
        return ConnectorTableId.of(database, table);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TableIdentifier)) {
            return false;
        }
        TableIdentifier that = (TableIdentifier) o;
        return database.equals(that.database) && table.equals(that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, table);
    }

    @Override
    public String toString() {
        return "TableIdentifier{" + database + "." + table + "}";
    }
}
