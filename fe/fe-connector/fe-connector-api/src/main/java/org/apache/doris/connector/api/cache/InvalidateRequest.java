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

package org.apache.doris.connector.api.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable description of a cache invalidation request.
 *
 * <p>Use the static factory methods to construct instances; the constructor is
 * private to enforce per-scope argument validation.</p>
 */
public final class InvalidateRequest {

    private final InvalidateScope scope;
    private final String database;
    private final String table;
    private final List<String> partitionKeys;
    private final String sysName;

    private InvalidateRequest(InvalidateScope scope,
                              String database,
                              String table,
                              List<String> partitionKeys,
                              String sysName) {
        this.scope = scope;
        this.database = database;
        this.table = table;
        this.partitionKeys = partitionKeys;
        this.sysName = sysName;
    }

    /** Invalidate all bindings in the catalog (REFRESH CATALOG full). */
    public static InvalidateRequest ofCatalog() {
        return new InvalidateRequest(InvalidateScope.CATALOG, null, null, Collections.emptyList(), null);
    }

    /** Invalidate all bindings whose key includes the given database. */
    public static InvalidateRequest ofDatabase(String database) {
        requireNonEmpty(database, "database");
        return new InvalidateRequest(InvalidateScope.DATABASE, database, null, Collections.emptyList(), null);
    }

    /** Invalidate all bindings keyed on the specified table. */
    public static InvalidateRequest ofTable(String database, String table) {
        requireNonEmpty(database, "database");
        requireNonEmpty(table, "table");
        return new InvalidateRequest(InvalidateScope.TABLE, database, table, Collections.emptyList(), null);
    }

    /** Invalidate partition list / partition meta of the specified table. */
    public static InvalidateRequest ofPartitions(String database, String table, List<String> partitionKeys) {
        requireNonEmpty(database, "database");
        requireNonEmpty(table, "table");
        if (partitionKeys == null) {
            throw new IllegalArgumentException("partitionKeys must not be null");
        }
        List<String> copy = Collections.unmodifiableList(new ArrayList<>(partitionKeys));
        for (String key : copy) {
            if (key == null) {
                throw new IllegalArgumentException("partitionKeys must not contain null entries");
            }
        }
        return new InvalidateRequest(InvalidateScope.PARTITIONS, database, table, copy, null);
    }

    /** Invalidate sys-table caches of the specified table. */
    public static InvalidateRequest ofSysTable(String database, String table, String sysName) {
        requireNonEmpty(database, "database");
        requireNonEmpty(table, "table");
        requireNonEmpty(sysName, "sysName");
        return new InvalidateRequest(InvalidateScope.SYS_TABLE, database, table, Collections.emptyList(), sysName);
    }

    private static void requireNonEmpty(String value, String name) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be null or empty");
        }
    }

    public InvalidateScope getScope() {
        return scope;
    }

    public Optional<String> getDatabase() {
        return Optional.ofNullable(database);
    }

    public Optional<String> getTable() {
        return Optional.ofNullable(table);
    }

    /** Returns the partition keys; immutable, empty list if not partition scope. */
    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public Optional<String> getSysName() {
        return Optional.ofNullable(sysName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof InvalidateRequest)) {
            return false;
        }
        InvalidateRequest that = (InvalidateRequest) o;
        return scope == that.scope
                && Objects.equals(database, that.database)
                && Objects.equals(table, that.table)
                && Objects.equals(partitionKeys, that.partitionKeys)
                && Objects.equals(sysName, that.sysName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scope, database, table, partitionKeys, sysName);
    }

    @Override
    public String toString() {
        return "InvalidateRequest{scope=" + scope
                + ", database=" + database
                + ", table=" + table
                + ", partitionKeys=" + partitionKeys
                + ", sysName=" + sysName
                + '}';
    }
}
