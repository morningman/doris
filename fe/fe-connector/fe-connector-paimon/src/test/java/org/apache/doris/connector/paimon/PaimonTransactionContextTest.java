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

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.write.ConnectorTransactionContext;
import org.apache.doris.connector.api.write.ConnectorTxnCapability;
import org.apache.doris.connector.api.write.WriteIntent;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BranchManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class PaimonTransactionContextTest {

    private Catalog catalog;
    private PaimonConnectorMetadata metadata;

    @BeforeEach
    void setUp() {
        catalog = Mockito.mock(Catalog.class);
        metadata = new PaimonConnectorMetadata(catalog, new HashMap<>());
    }

    private FileStoreTable mockTable(String db, String tbl,
            List<String> partitionKeys, List<String> primaryKeys) throws Exception {
        FileStoreTable table = Mockito.mock(FileStoreTable.class);
        Mockito.when(table.name()).thenReturn(tbl);
        Mockito.when(table.fullName()).thenReturn(db + "." + tbl);
        Mockito.when(table.partitionKeys()).thenReturn(partitionKeys);
        Mockito.when(table.primaryKeys()).thenReturn(primaryKeys);
        Mockito.when(table.options()).thenReturn(new HashMap<>());
        Mockito.when(table.rowType()).thenReturn(new RowType(Arrays.asList(
                new DataField(0, "id", DataTypes.BIGINT()),
                new DataField(1, "name", DataTypes.STRING()))));
        Mockito.when(table.location()).thenReturn(new Path("file:///tmp/" + tbl));
        BranchManager bm = Mockito.mock(BranchManager.class);
        Mockito.when(bm.branches()).thenReturn(Collections.emptyList());
        Mockito.when(table.branchManager()).thenReturn(bm);
        Mockito.when(table.newBatchWriteBuilder()).thenReturn(Mockito.mock(BatchWriteBuilder.class));
        Mockito.when(catalog.getTable(Identifier.create(db, tbl))).thenReturn(table);
        return table;
    }

    private ConnectorTableHandle handleFor(String db, String tbl, FileStoreTable table) {
        PaimonTableHandle h = new PaimonTableHandle(db, tbl,
                table.partitionKeys() != null ? table.partitionKeys() : Collections.emptyList(),
                table.primaryKeys() != null ? table.primaryKeys() : Collections.emptyList());
        h.setPaimonTable(table);
        return h;
    }

    @Test
    void connectorAdvertisesPaimonCapabilities() {
        Map<ConnectorTxnCapability, Boolean> snapshot = new HashMap<>();
        for (ConnectorTxnCapability c : metadata.txnCapabilities()) {
            snapshot.put(c, true);
        }
        Assertions.assertTrue(snapshot.getOrDefault(ConnectorTxnCapability.MULTI_STATEMENT, false));
        Assertions.assertTrue(snapshot.getOrDefault(ConnectorTxnCapability.COMMIT_RETRY, false));
        Assertions.assertFalse(snapshot.getOrDefault(ConnectorTxnCapability.FAILOVER_SAFE, false),
                "Paimon must not advertise FAILOVER_SAFE in M3-09 (no portable txn-resume format yet)");
        Assertions.assertFalse(snapshot.getOrDefault(ConnectorTxnCapability.SAVEPOINT, false));
    }

    @Test
    void beginTransactionReturnsPaimonContext() throws Exception {
        FileStoreTable table = mockTable("db", "t", Collections.emptyList(), Collections.emptyList());
        ConnectorTransactionContext ctx = metadata.beginTransaction(
                null, handleFor("db", "t", table), WriteIntent.simple());
        Assertions.assertTrue(ctx instanceof PaimonTransactionContext);
        PaimonTransactionContext pc = (PaimonTransactionContext) ctx;
        Assertions.assertEquals("db", pc.getDbName());
        Assertions.assertEquals("t", pc.getTableName());
        Assertions.assertNotNull(pc.getWriteBuilder());
        Assertions.assertNotNull(pc.getTable());
    }

    @Test
    void txnIdStableAndPrefixed() throws Exception {
        FileStoreTable table = mockTable("db", "t", Collections.emptyList(), Collections.emptyList());
        PaimonTransactionContext ctx = (PaimonTransactionContext) metadata.beginTransaction(
                null, handleFor("db", "t", table), WriteIntent.simple());
        Assertions.assertTrue(ctx.txnId().startsWith("paimon-"));
        Assertions.assertEquals(ctx.txnId(), ctx.txnId());
    }

    @Test
    void contextCapabilitiesEqualConnectorCapabilities() throws Exception {
        FileStoreTable table = mockTable("db", "t", Collections.emptyList(), Collections.emptyList());
        ConnectorTransactionContext ctx = metadata.beginTransaction(
                null, handleFor("db", "t", table), WriteIntent.simple());
        Assertions.assertEquals(metadata.txnCapabilities(), ctx.txnCapabilities());
    }

    @Test
    void serializeIsDeferred() throws Exception {
        FileStoreTable table = mockTable("db", "t", Collections.emptyList(), Collections.emptyList());
        ConnectorTransactionContext ctx = metadata.beginTransaction(
                null, handleFor("db", "t", table), WriteIntent.simple());
        Assertions.assertFalse(ctx.supportsFailover());
        Assertions.assertThrows(DorisConnectorException.class, ctx::serialize);
    }

    @Test
    void labelEncodesOverwriteMode() throws Exception {
        FileStoreTable table = mockTable("db", "t",
                Collections.singletonList("dt"), Collections.emptyList());
        WriteIntent intent = WriteIntent.builder()
                .overwriteMode(WriteIntent.OverwriteMode.FULL_TABLE)
                .build();
        PaimonTransactionContext ctx = (PaimonTransactionContext) metadata.beginTransaction(
                null, handleFor("db", "t", table), intent);
        Assertions.assertTrue(ctx.label().contains("db.t"));
        Assertions.assertTrue(ctx.label().contains("FULL_TABLE"));
    }
}
