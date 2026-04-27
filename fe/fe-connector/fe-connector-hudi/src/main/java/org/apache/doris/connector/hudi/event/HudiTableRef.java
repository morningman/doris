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

import java.util.Locale;
import java.util.Objects;

/**
 * Identifies one Hudi table the watcher is responsible for. {@code db}
 * and {@code table} are normalised to lower case; {@code basePath} is
 * the table's storage location (used by the timeline lister).
 */
public final class HudiTableRef {

    private final String db;
    private final String table;
    private final String basePath;

    public HudiTableRef(String db, String table, String basePath) {
        this.db = Objects.requireNonNull(db, "db").toLowerCase(Locale.ROOT);
        this.table = Objects.requireNonNull(table, "table").toLowerCase(Locale.ROOT);
        this.basePath = Objects.requireNonNull(basePath, "basePath");
    }

    public String db() {
        return db;
    }

    public String table() {
        return table;
    }

    public String basePath() {
        return basePath;
    }

    public String key() {
        return db + "\u0001" + table;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HudiTableRef)) {
            return false;
        }
        HudiTableRef that = (HudiTableRef) o;
        return db.equals(that.db) && table.equals(that.table) && basePath.equals(that.basePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(db, table, basePath);
    }

    @Override
    public String toString() {
        return "HudiTableRef{" + db + "." + table + "@" + basePath + "}";
    }
}
