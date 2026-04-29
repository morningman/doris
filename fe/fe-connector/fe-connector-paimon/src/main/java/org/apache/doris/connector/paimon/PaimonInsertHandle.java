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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.api.handle.ConnectorInsertHandle;
import org.apache.doris.connector.api.write.WriteIntent;

import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchWriteBuilder;

import java.util.Objects;

/**
 * Plugin-side state for an in-flight Paimon INSERT / INSERT OVERWRITE.
 *
 * <p>Captures the {@link BatchWriteBuilder} created at {@code beginInsert}
 * time together with the resolved {@link Table} (already switched to the
 * target branch, when {@link WriteIntent#branch()} is set) and the
 * originating {@link WriteIntent} so that {@code finishInsert} only needs
 * to deserialize the BE-supplied commit messages and call
 * {@link BatchWriteBuilder#newCommit()} {@code .commit(messages)}.</p>
 *
 * <p>UPSERT semantics for primary-key tables are conveyed entirely
 * through the BE-side commit messages (paimon's {@code _DORIS_DELETE_SIGN_}
 * row-op encoding); no extra plugin-side state is required and the same
 * commit path serves both APPEND and UPSERT.</p>
 */
public final class PaimonInsertHandle implements ConnectorInsertHandle {

    private final String dbName;
    private final String tableName;
    private final Table table;
    private final BatchWriteBuilder writeBuilder;
    private final WriteIntent intent;

    public PaimonInsertHandle(
            String dbName,
            String tableName,
            Table table,
            BatchWriteBuilder writeBuilder,
            WriteIntent intent) {
        this.dbName = Objects.requireNonNull(dbName, "dbName");
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.table = Objects.requireNonNull(table, "table");
        this.writeBuilder = Objects.requireNonNull(writeBuilder, "writeBuilder");
        this.intent = Objects.requireNonNull(intent, "intent");
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public Table getTable() {
        return table;
    }

    public BatchWriteBuilder getWriteBuilder() {
        return writeBuilder;
    }

    public WriteIntent getIntent() {
        return intent;
    }
}
