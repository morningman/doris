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

package org.apache.doris.connector.api;

import java.io.Serializable;
import java.util.Objects;

/**
 * Typed, immutable identifier for a connector-side table — the
 * {@code (database, table)} pair the engine and plugins exchange across the
 * SPI surface.
 *
 * <p>Replaces the historical {@code (String database, String table)}
 * parameter pair that appeared on {@link
 * org.apache.doris.connector.api.timetravel.RefOps RefOps},
 * {@link org.apache.doris.connector.api.mtmv.MtmvOps MtmvOps},
 * {@link org.apache.doris.connector.api.policy.PolicyOps PolicyOps},
 * {@link org.apache.doris.connector.api.systable.SystemTableOps
 * SystemTableOps},
 * {@link org.apache.doris.connector.api.action.ActionTarget ActionTarget} and
 * {@link org.apache.doris.connector.api.cache.InvalidateRequest
 * InvalidateRequest} so any future SPI addition can take a single typed id
 * rather than two stringly-typed parameters.</p>
 *
 * <p>Distinct from {@code ConnectorTableHandle}: the handle represents
 * plugin-internal table state (snapshot, location, etc.); this id is the
 * logical {@code (database, table)} identity. Both database and table are
 * required and must be non-{@code null}.</p>
 *
 * <p>{@link Serializable} with {@code serialVersionUID = 1L} because fe-core
 * may persist the id into {@code EditLog} or render it into a thrift
 * {@code string} field (folded as {@code "db.tbl"}).</p>
 */
public final class ConnectorTableId implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String database;
    private final String table;

    private ConnectorTableId(String database, String table) {
        this.database = database;
        this.table = table;
    }

    /**
     * Creates a new {@code ConnectorTableId} for the given pair. Both
     * arguments must be non-{@code null}.
     */
    public static ConnectorTableId of(String database, String table) {
        Objects.requireNonNull(database, "database");
        Objects.requireNonNull(table, "table");
        return new ConnectorTableId(database, table);
    }

    public String database() {
        return database;
    }

    public String table() {
        return table;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorTableId)) {
            return false;
        }
        ConnectorTableId other = (ConnectorTableId) o;
        return database.equals(other.database) && table.equals(other.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, table);
    }

    @Override
    public String toString() {
        return database + "." + table;
    }
}
