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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.handle.ConnectorInsertHandle;
import org.apache.doris.connector.api.write.WriteIntent;

import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;

import java.util.Objects;

/**
 * Plugin-side state for an in-flight Iceberg INSERT / INSERT OVERWRITE.
 *
 * <p>Captures the iceberg {@link Transaction} created at {@code beginInsert}
 * time together with the target {@link WriteIntent} so that {@code finishInsert}
 * can dispatch APPEND / OVERWRITE / REPLACE-PARTITIONS without re-resolving
 * connector state.</p>
 *
 * <p>If the supplied intent carries a branch (M3-04), {@code finishInsert}
 * routes each iceberg operation to that branch via {@code .toBranch(name)};
 * {@code IcebergConnectorMetadata#beginInsert} validates the branch up
 * front and rejects unknown names or refs that resolve to a tag.</p>
 */
public final class IcebergInsertHandle implements ConnectorInsertHandle {

    private final String dbName;
    private final String tableName;
    private final Table table;
    private final Transaction transaction;
    private final WriteIntent intent;

    public IcebergInsertHandle(
            String dbName,
            String tableName,
            Table table,
            Transaction transaction,
            WriteIntent intent) {
        this.dbName = Objects.requireNonNull(dbName, "dbName");
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.table = Objects.requireNonNull(table, "table");
        this.transaction = Objects.requireNonNull(transaction, "transaction");
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

    public Transaction getTransaction() {
        return transaction;
    }

    public WriteIntent getIntent() {
        return intent;
    }
}
