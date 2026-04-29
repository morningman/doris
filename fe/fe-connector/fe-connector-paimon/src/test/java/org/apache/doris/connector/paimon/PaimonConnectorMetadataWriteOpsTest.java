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
import org.apache.doris.connector.api.write.ConnectorWriteConfig;
import org.apache.doris.connector.api.write.ConnectorWriteType;
import org.apache.doris.connector.api.write.WriteIntent;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BranchManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class PaimonConnectorMetadataWriteOpsTest {

    private Catalog catalog;
    private PaimonConnectorMetadata metadata;

    @BeforeEach
    void setUp() {
        catalog = Mockito.mock(Catalog.class);
        metadata = new PaimonConnectorMetadata(catalog, new HashMap<>());
    }

    /** Build a mocked paimon FileStoreTable; partition keys / pk lists / branches are configurable. */
    private FileStoreTable mockTable(
            String db, String tbl,
            List<String> partitionKeys, List<String> primaryKeys,
            Map<String, String> options, List<String> branches) throws Exception {
        FileStoreTable table = Mockito.mock(FileStoreTable.class);
        Mockito.when(table.name()).thenReturn(tbl);
        Mockito.when(table.fullName()).thenReturn(db + "." + tbl);
        Mockito.when(table.partitionKeys()).thenReturn(partitionKeys);
        Mockito.when(table.primaryKeys()).thenReturn(primaryKeys);
        Mockito.when(table.options()).thenReturn(options);
        RowType rowType = new RowType(Arrays.asList(
                new DataField(0, "id", DataTypes.BIGINT()),
                new DataField(1, "name", DataTypes.STRING())));
        Mockito.when(table.rowType()).thenReturn(rowType);
        Mockito.when(table.location()).thenReturn(new Path("file:///tmp/" + tbl));

        BranchManager bm = Mockito.mock(BranchManager.class);
        Mockito.when(bm.branches()).thenReturn(branches);
        Mockito.when(table.branchManager()).thenReturn(bm);

        Mockito.when(catalog.getTable(Identifier.create(db, tbl))).thenReturn(table);
        return table;
    }

    private static byte[] serializeMessage() throws Exception {
        CommitMessage msg = new CommitMessageImpl(
                BinaryRow.EMPTY_ROW, 0, 1,
                DataIncrement.emptyIncrement(),
                new CompactIncrement(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));
        return new CommitMessageSerializer().serialize(msg);
    }

    private ConnectorTableHandle handleFor(String db, String tbl, FileStoreTable table) {
        PaimonTableHandle handle = new PaimonTableHandle(
                db, tbl,
                table.partitionKeys() != null ? table.partitionKeys() : Collections.emptyList(),
                table.primaryKeys() != null ? table.primaryKeys() : Collections.emptyList());
        handle.setPaimonTable(table);
        return handle;
    }

    // ========== capability + config ==========

    @Test
    void supportsInsertReturnsTrue() {
        Assertions.assertTrue(metadata.supportsInsert());
    }

    @Test
    void getWriteConfigSurfacesFileFormatPartitionAndPkOptions() throws Exception {
        Map<String, String> opts = new HashMap<>();
        opts.put(CoreOptions.FILE_FORMAT.key(), "orc");
        opts.put(CoreOptions.MERGE_ENGINE.key(), "deduplicate");
        opts.put(CoreOptions.BUCKET.key(), "4");
        FileStoreTable table = mockTable("db", "pk_t",
                Collections.singletonList("dt"),
                Collections.singletonList("id"),
                opts, Collections.emptyList());
        ConnectorWriteConfig cfg = metadata.getWriteConfig(
                null, handleFor("db", "pk_t", table), Collections.emptyList());

        Assertions.assertEquals(ConnectorWriteType.FILE_WRITE, cfg.getWriteType());
        Assertions.assertEquals("orc", cfg.getFileFormat());
        Assertions.assertEquals(Collections.singletonList("dt"), cfg.getPartitionColumns());
        Assertions.assertEquals("id", cfg.getProperties().get("primary-keys"));
        Assertions.assertEquals("deduplicate", cfg.getProperties().get("merge-engine"));
        Assertions.assertEquals("4", cfg.getProperties().get("bucket"));
        Assertions.assertEquals("file:/tmp/pk_t", cfg.getWriteLocation());
    }

    @Test
    void getWriteConfigForAppendOnlyOmitsPkProperties() throws Exception {
        FileStoreTable table = mockTable("db", "ap_t",
                Collections.emptyList(), Collections.emptyList(),
                new HashMap<>(), Collections.emptyList());
        ConnectorWriteConfig cfg = metadata.getWriteConfig(
                null, handleFor("db", "ap_t", table), Collections.emptyList());
        Assertions.assertNull(cfg.getProperties().get("primary-keys"));
        Assertions.assertNull(cfg.getProperties().get("merge-engine"));
    }

    // ========== beginInsert validation ==========

    @Test
    void beginInsertAppendOnAppendOnlyTableSucceeds() throws Exception {
        FileStoreTable table = mockTable("db", "ap_t",
                Collections.emptyList(), Collections.emptyList(),
                new HashMap<>(), Collections.emptyList());
        BatchWriteBuilder bwb = Mockito.mock(BatchWriteBuilder.class);
        Mockito.when(table.newBatchWriteBuilder()).thenReturn(bwb);

        PaimonInsertHandle h = (PaimonInsertHandle) metadata.beginInsert(
                null, handleFor("db", "ap_t", table), Collections.emptyList(), WriteIntent.simple());
        Assertions.assertSame(table, h.getTable());
        Assertions.assertSame(bwb, h.getWriteBuilder());
        Mockito.verify(bwb, Mockito.never()).withOverwrite(Mockito.anyMap());
    }

    @Test
    void beginInsertAppendOnPkTableSucceeds() throws Exception {
        FileStoreTable table = mockTable("db", "pk_t",
                Collections.emptyList(), Collections.singletonList("id"),
                new HashMap<>(), Collections.emptyList());
        Mockito.when(table.newBatchWriteBuilder()).thenReturn(Mockito.mock(BatchWriteBuilder.class));
        Assertions.assertNotNull(metadata.beginInsert(
                null, handleFor("db", "pk_t", table), Collections.emptyList(), WriteIntent.simple()));
    }

    @Test
    void beginInsertUpsertOnPkTableSucceeds() throws Exception {
        FileStoreTable table = mockTable("db", "pk_t",
                Collections.emptyList(), Collections.singletonList("id"),
                new HashMap<>(), Collections.emptyList());
        Mockito.when(table.newBatchWriteBuilder()).thenReturn(Mockito.mock(BatchWriteBuilder.class));
        WriteIntent intent = WriteIntent.builder().upsert(true).build();
        PaimonInsertHandle h = (PaimonInsertHandle) metadata.beginInsert(
                null, handleFor("db", "pk_t", table), Collections.emptyList(), intent);
        Assertions.assertTrue(h.getIntent().isUpsert());
    }

    @Test
    void beginInsertUpsertOnAppendOnlyTableThrows() throws Exception {
        FileStoreTable table = mockTable("db", "ap_t",
                Collections.emptyList(), Collections.emptyList(),
                new HashMap<>(), Collections.emptyList());
        WriteIntent intent = WriteIntent.builder().upsert(true).build();
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.beginInsert(null, handleFor("db", "ap_t", table), Collections.emptyList(), intent));
        Assertions.assertTrue(ex.getMessage().contains("UPSERT"),
                "expected UPSERT-not-supported message, got: " + ex.getMessage());
    }

    @Test
    void beginInsertFullTableOverwriteCallsWithOverwriteEmpty() throws Exception {
        FileStoreTable table = mockTable("db", "ow_full",
                Collections.singletonList("dt"), Collections.emptyList(),
                new HashMap<>(), Collections.emptyList());
        BatchWriteBuilder bwb = Mockito.mock(BatchWriteBuilder.class);
        Mockito.when(table.newBatchWriteBuilder()).thenReturn(bwb);

        WriteIntent intent = WriteIntent.builder()
                .overwriteMode(WriteIntent.OverwriteMode.FULL_TABLE).build();
        metadata.beginInsert(null, handleFor("db", "ow_full", table), Collections.emptyList(), intent);

        ArgumentCaptor<Map<String, String>> captor =
                ArgumentCaptor.forClass(Map.class);
        Mockito.verify(bwb).withOverwrite(captor.capture());
        Assertions.assertTrue(captor.getValue().isEmpty(), "FULL_TABLE must pass empty static map");
    }

    @Test
    void beginInsertStaticPartitionOverwriteCallsWithOverwriteMap() throws Exception {
        FileStoreTable table = mockTable("db", "ow_static",
                Collections.singletonList("region"), Collections.emptyList(),
                new HashMap<>(), Collections.emptyList());
        BatchWriteBuilder bwb = Mockito.mock(BatchWriteBuilder.class);
        Mockito.when(table.newBatchWriteBuilder()).thenReturn(bwb);

        Map<String, String> keys = new LinkedHashMap<>();
        keys.put("region", "us");
        WriteIntent intent = WriteIntent.builder()
                .overwriteMode(WriteIntent.OverwriteMode.STATIC_PARTITION)
                .staticPartitions(keys).build();
        metadata.beginInsert(null, handleFor("db", "ow_static", table), Collections.emptyList(), intent);

        ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(bwb).withOverwrite(captor.capture());
        Assertions.assertEquals("us", captor.getValue().get("region"));
    }

    @Test
    void beginInsertStaticPartitionUnknownKeyThrows() throws Exception {
        FileStoreTable table = mockTable("db", "ow_static_bad",
                Collections.singletonList("region"), Collections.emptyList(),
                new HashMap<>(), Collections.emptyList());
        Map<String, String> keys = new HashMap<>();
        keys.put("nope", "x");
        WriteIntent intent = WriteIntent.builder()
                .overwriteMode(WriteIntent.OverwriteMode.STATIC_PARTITION)
                .staticPartitions(keys).build();
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.beginInsert(
                        null, handleFor("db", "ow_static_bad", table), Collections.emptyList(), intent));
        Assertions.assertTrue(ex.getMessage().contains("Unknown partition column"));
    }

    @Test
    void beginInsertStaticPartitionOnUnpartitionedTableThrows() throws Exception {
        FileStoreTable table = mockTable("db", "no_part",
                Collections.emptyList(), Collections.emptyList(),
                new HashMap<>(), Collections.emptyList());
        Map<String, String> keys = new HashMap<>();
        keys.put("region", "x");
        WriteIntent intent = WriteIntent.builder()
                .overwriteMode(WriteIntent.OverwriteMode.STATIC_PARTITION)
                .staticPartitions(keys).build();
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.beginInsert(
                        null, handleFor("db", "no_part", table), Collections.emptyList(), intent));
    }

    @Test
    void beginInsertDynamicPartitionRejectedClearly() throws Exception {
        FileStoreTable table = mockTable("db", "dyn_t",
                Collections.singletonList("region"), Collections.emptyList(),
                new HashMap<>(), Collections.emptyList());
        WriteIntent intent = WriteIntent.builder()
                .overwriteMode(WriteIntent.OverwriteMode.DYNAMIC_PARTITION).build();
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.beginInsert(
                        null, handleFor("db", "dyn_t", table), Collections.emptyList(), intent));
        Assertions.assertTrue(ex.getMessage().contains("DYNAMIC_PARTITION"),
                "message must explain DYNAMIC_PARTITION limitation, got: " + ex.getMessage());
    }

    // ========== branch routing ==========

    @Test
    void beginInsertWithKnownBranchSwitchesTable() throws Exception {
        FileStoreTable mainTable = mockTable("db", "br_t",
                Collections.emptyList(), Collections.emptyList(),
                new HashMap<>(), Collections.singletonList("dev"));
        FileStoreTable branchTable = Mockito.mock(FileStoreTable.class);
        Mockito.when(mainTable.switchToBranch("dev")).thenReturn(branchTable);
        BatchWriteBuilder bwb = Mockito.mock(BatchWriteBuilder.class);
        Mockito.when(branchTable.newBatchWriteBuilder()).thenReturn(bwb);

        WriteIntent intent = WriteIntent.builder().branch("dev").build();
        PaimonInsertHandle h = (PaimonInsertHandle) metadata.beginInsert(
                null, handleFor("db", "br_t", mainTable), Collections.emptyList(), intent);

        Assertions.assertSame(branchTable, h.getTable());
        Mockito.verify(mainTable).switchToBranch("dev");
    }

    @Test
    void beginInsertWithUnknownBranchThrowsFastFail() throws Exception {
        FileStoreTable table = mockTable("db", "br_bad",
                Collections.emptyList(), Collections.emptyList(),
                new HashMap<>(), Collections.singletonList("main"));
        WriteIntent intent = WriteIntent.builder().branch("nope").build();
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.beginInsert(
                        null, handleFor("db", "br_bad", table), Collections.emptyList(), intent));
        Assertions.assertTrue(ex.getMessage().contains("does not exist"),
                "expected branch-not-found message, got: " + ex.getMessage());
    }

    // ========== finishInsert / abortInsert ==========

    @Test
    void finishInsertAppendInvokesCommitWithDecodedMessages() throws Exception {
        FileStoreTable table = mockTable("db", "fin_ap",
                Collections.emptyList(), Collections.emptyList(),
                new HashMap<>(), Collections.emptyList());
        BatchWriteBuilder bwb = Mockito.mock(BatchWriteBuilder.class);
        BatchTableCommit commit = Mockito.mock(BatchTableCommit.class);
        Mockito.when(bwb.newCommit()).thenReturn(commit);
        Mockito.when(table.newBatchWriteBuilder()).thenReturn(bwb);

        PaimonInsertHandle h = (PaimonInsertHandle) metadata.beginInsert(
                null, handleFor("db", "fin_ap", table), Collections.emptyList(), WriteIntent.simple());

        byte[] frag = serializeMessage();
        metadata.finishInsert(null, h, Arrays.asList(frag, frag));

        ArgumentCaptor<List<CommitMessage>> captor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(commit).commit(captor.capture());
        Assertions.assertEquals(2, captor.getValue().size());
        Mockito.verify(commit).close();
    }

    @Test
    void finishInsertUpsertOnPkTableInvokesCommit() throws Exception {
        FileStoreTable table = mockTable("db", "fin_pk",
                Collections.emptyList(), Collections.singletonList("id"),
                new HashMap<>(), Collections.emptyList());
        BatchWriteBuilder bwb = Mockito.mock(BatchWriteBuilder.class);
        BatchTableCommit commit = Mockito.mock(BatchTableCommit.class);
        Mockito.when(bwb.newCommit()).thenReturn(commit);
        Mockito.when(table.newBatchWriteBuilder()).thenReturn(bwb);

        WriteIntent intent = WriteIntent.builder().upsert(true).build();
        PaimonInsertHandle h = (PaimonInsertHandle) metadata.beginInsert(
                null, handleFor("db", "fin_pk", table), Collections.emptyList(), intent);

        // Two fragments — paimon collapses inserts vs delete row-ops via
        // CommitMessage payload; the converter does not need to inspect.
        metadata.finishInsert(null, h, Arrays.asList(serializeMessage(), serializeMessage()));
        Mockito.verify(commit).commit(Mockito.anyList());
    }

    @Test
    void finishInsertEmptyFragmentsRejectedLoudly() throws Exception {
        FileStoreTable table = mockTable("db", "fin_empty",
                Collections.emptyList(), Collections.emptyList(),
                new HashMap<>(), Collections.emptyList());
        Mockito.when(table.newBatchWriteBuilder()).thenReturn(Mockito.mock(BatchWriteBuilder.class));
        PaimonInsertHandle h = (PaimonInsertHandle) metadata.beginInsert(
                null, handleFor("db", "fin_empty", table), Collections.emptyList(), WriteIntent.simple());
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.finishInsert(null, h, Collections.emptyList()));
    }

    @Test
    void abortInsertIsNoOpAndDoesNotCommit() throws Exception {
        FileStoreTable table = mockTable("db", "abort_t",
                Collections.emptyList(), Collections.emptyList(),
                new HashMap<>(), Collections.emptyList());
        BatchWriteBuilder bwb = Mockito.mock(BatchWriteBuilder.class);
        Mockito.when(table.newBatchWriteBuilder()).thenReturn(bwb);
        PaimonInsertHandle h = (PaimonInsertHandle) metadata.beginInsert(
                null, handleFor("db", "abort_t", table), Collections.emptyList(), WriteIntent.simple());
        metadata.abortInsert(null, h);
        metadata.abortInsert(null, h);
        Mockito.verify(bwb, Mockito.never()).newCommit();
    }

    // ========== misc table-handle wiring ==========

    @Test
    void getWriteConfigUnknownTableThrows() throws Exception {
        Mockito.when(catalog.getTable(Identifier.create("db", "missing")))
                .thenThrow(new Catalog.TableNotExistException(Identifier.create("db", "missing")));
        PaimonTableHandle handle = new PaimonTableHandle("db", "missing",
                Collections.emptyList(), Collections.emptyList());
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.getWriteConfig(null, handle, Collections.emptyList()));
    }

    /**
     * Branch writes require a paimon FileStoreTable because the branch
     * machinery (switchToBranch / branchManager) is only defined on that
     * subtype; non-FileStoreTable inputs must fail loudly so the engine
     * doesn't silently route to the wrong snapshot lineage.
     */
    @Test
    void beginInsertWithBranchOnNonFileStoreTableThrows() throws Exception {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.fullName()).thenReturn("db.non_fs");
        Mockito.when(table.partitionKeys()).thenReturn(Collections.emptyList());
        Mockito.when(table.primaryKeys()).thenReturn(Collections.emptyList());
        Mockito.when(table.options()).thenReturn(Collections.emptyMap());
        Mockito.when(catalog.getTable(Identifier.create("db", "non_fs"))).thenReturn(table);

        PaimonTableHandle handle = new PaimonTableHandle("db", "non_fs",
                Collections.emptyList(), Collections.emptyList());
        WriteIntent intent = WriteIntent.builder().branch("dev").build();
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.beginInsert(null, handle, Collections.emptyList(), intent));
        Assertions.assertTrue(ex.getMessage().contains("FileStoreTable"));
    }
}
