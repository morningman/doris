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

package org.apache.doris.connector.api.credential;

import java.net.URI;
import java.util.Objects;
import java.util.Optional;

/** Immutable request describing the storage location credentials are needed for. */
public final class StorageRequest {

    private final URI path;
    private final String catalog;
    private final Optional<String> database;
    private final Optional<String> table;

    private StorageRequest(Builder b) {
        if (b.path == null) {
            throw new IllegalArgumentException("path is required");
        }
        if (b.catalog == null || b.catalog.isEmpty()) {
            throw new IllegalArgumentException("catalog is required");
        }
        this.path = b.path;
        this.catalog = b.catalog;
        this.database = b.database == null ? Optional.empty() : b.database;
        this.table = b.table == null ? Optional.empty() : b.table;
    }

    public URI path() {
        return path;
    }

    public String catalog() {
        return catalog;
    }

    public Optional<String> database() {
        return database;
    }

    public Optional<String> table() {
        return table;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StorageRequest)) {
            return false;
        }
        StorageRequest that = (StorageRequest) o;
        return path.equals(that.path)
                && catalog.equals(that.catalog)
                && database.equals(that.database)
                && table.equals(that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, catalog, database, table);
    }

    @Override
    public String toString() {
        return "StorageRequest{path=" + path
                + ", catalog=" + catalog
                + ", database=" + database
                + ", table=" + table
                + '}';
    }

    /** Builder for {@link StorageRequest}. */
    public static final class Builder {
        private URI path;
        private String catalog;
        private Optional<String> database = Optional.empty();
        private Optional<String> table = Optional.empty();

        private Builder() {
        }

        public Builder path(URI path) {
            this.path = path;
            return this;
        }

        public Builder catalog(String catalog) {
            this.catalog = catalog;
            return this;
        }

        public Builder database(String database) {
            this.database = Optional.ofNullable(database);
            return this;
        }

        public Builder table(String table) {
            this.table = Optional.ofNullable(table);
            return this;
        }

        public StorageRequest build() {
            return new StorageRequest(this);
        }
    }
}
