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

package org.apache.doris.connector.iceberg.cache;

import java.util.Objects;

/**
 * Immutable composite key (database, table) used by per-table Iceberg cache
 * bindings such as {@link IcebergCacheBindings#ENTRY_TABLE} and
 * {@link IcebergCacheBindings#ENTRY_SNAPSHOTS}. Equality and hash code use
 * both fields so the cache treats {@code (db, t)} and {@code (db2, t)} as
 * distinct entries.
 */
public final class IcebergTableCacheKey {

    private final String database;
    private final String table;

    public IcebergTableCacheKey(String database, String table) {
        this.database = Objects.requireNonNull(database, "database");
        this.table = Objects.requireNonNull(table, "table");
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IcebergTableCacheKey)) {
            return false;
        }
        IcebergTableCacheKey that = (IcebergTableCacheKey) o;
        return database.equals(that.database) && table.equals(that.table);
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
