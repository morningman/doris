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

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorInsertHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.write.ConnectorWriteConfig;
import org.apache.doris.connector.api.write.ConnectorWriteType;
import org.apache.doris.connector.api.write.WriteIntent;
import org.apache.doris.thrift.TIcebergCommitData;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

class IcebergConnectorMetadataWriteOpsTest {

    private InMemoryCatalog catalog;
    private IcebergConnectorMetadata metadata;

    @BeforeEach
    void setUp() {
        catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        ((SupportsNamespaces) catalog).createNamespace(Namespace.of("db"));
        metadata = new IcebergConnectorMetadata(catalog, new HashMap<>(), null, null, null);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
    }

    private Table createUnpartitioned(String name) {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()));
        return catalog.createTable(TableIdentifier.of("db", name), schema);
    }

    private Table createPartitioned(String name) {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.required(3, "region", Types.StringType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("region").build();
        return catalog.createTable(TableIdentifier.of("db", name), schema, spec);
    }

    private ConnectorTableHandle handleFor(String name) {
        return metadata.getTableHandle(null, "db", name).orElseThrow();
    }

    private byte[] makeFragment(String path, long rows, long size, List<String> partitionValues) throws Exception {
        TIcebergCommitData commit = new TIcebergCommitData();
        commit.setFilePath(path);
        commit.setRowCount(rows);
        commit.setFileSize(size);
        if (partitionValues != null) {
            commit.setPartitionValues(partitionValues);
        }
        return new TSerializer(new TBinaryProtocol.Factory()).serialize(commit);
    }

    private String fakeFile(Table table, String suffix) {
        return table.location() + "/data/" + suffix + "-" + UUID.randomUUID() + ".parquet";
    }

    @Test
    void supportsInsertReturnsTrue() {
        Assertions.assertTrue(metadata.supportsInsert());
    }

    @Test
    void getWriteConfigReturnsFileWriteWithFormatAndCompression() {
        Table t = createUnpartitioned("cfg_t");
        ConnectorWriteConfig cfg = metadata.getWriteConfig(null, handleFor("cfg_t"), Collections.emptyList());
        Assertions.assertEquals(ConnectorWriteType.FILE_WRITE, cfg.getWriteType());
        Assertions.assertEquals("parquet", cfg.getFileFormat());
        Assertions.assertNotNull(cfg.getCompression());
        Assertions.assertEquals(t.location(), cfg.getWriteLocation());
        Assertions.assertTrue(cfg.getPartitionColumns().isEmpty());
        Assertions.assertEquals("parquet", cfg.getProperties().get("file-format"));
    }

    @Test
    void getWriteConfigExposesPartitionColumns() {
        createPartitioned("cfg_pt");
        ConnectorWriteConfig cfg = metadata.getWriteConfig(null, handleFor("cfg_pt"), Collections.emptyList());
        Assertions.assertEquals(Collections.singletonList("region"), cfg.getPartitionColumns());
    }

    @Test
    void beginInsertWithAppendIntentBuildsHandle() {
        createUnpartitioned("ins_t");
        ConnectorInsertHandle h = metadata.beginInsert(null, handleFor("ins_t"), Collections.emptyList(), WriteIntent.simple());
        Assertions.assertTrue(h instanceof IcebergInsertHandle);
        IcebergInsertHandle ih = (IcebergInsertHandle) h;
        Assertions.assertEquals(WriteIntent.OverwriteMode.NONE, ih.getIntent().overwriteMode());
        Assertions.assertNotNull(ih.getTransaction());
    }

    @Test
    void beginInsertWithFullTableIntent() {
        createUnpartitioned("ow_full");
        WriteIntent intent = WriteIntent.builder().overwriteMode(WriteIntent.OverwriteMode.FULL_TABLE).build();
        IcebergInsertHandle ih = (IcebergInsertHandle) metadata.beginInsert(
                null, handleFor("ow_full"), Collections.emptyList(), intent);
        Assertions.assertEquals(WriteIntent.OverwriteMode.FULL_TABLE, ih.getIntent().overwriteMode());
    }

    @Test
    void beginInsertWithStaticPartitionIntentCarriesKeys() {
        createPartitioned("ow_static");
        Map<String, String> partKeys = new HashMap<>();
        partKeys.put("region", "us-east");
        WriteIntent intent = WriteIntent.builder()
                .overwriteMode(WriteIntent.OverwriteMode.STATIC_PARTITION)
                .staticPartitions(partKeys)
                .build();
        IcebergInsertHandle ih = (IcebergInsertHandle) metadata.beginInsert(
                null, handleFor("ow_static"), Collections.emptyList(), intent);
        Assertions.assertEquals("us-east", ih.getIntent().staticPartitions().get("region"));
    }

    @Test
    void beginInsertWithDynamicPartitionIntent() {
        createPartitioned("ow_dyn");
        WriteIntent intent = WriteIntent.builder()
                .overwriteMode(WriteIntent.OverwriteMode.DYNAMIC_PARTITION)
                .build();
        IcebergInsertHandle ih = (IcebergInsertHandle) metadata.beginInsert(
                null, handleFor("ow_dyn"), Collections.emptyList(), intent);
        Assertions.assertEquals(WriteIntent.OverwriteMode.DYNAMIC_PARTITION, ih.getIntent().overwriteMode());
    }

    @Test
    void beginInsertRejectsBranchUntilM304() {
        createUnpartitioned("br_tbl");
        WriteIntent intent = WriteIntent.builder().branch("feature").build();
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.beginInsert(null, handleFor("br_tbl"), Collections.emptyList(), intent));
        Assertions.assertTrue(ex.getMessage().contains("M3-04"));
    }

    @Test
    void beginInsertRejectsPartitionOverwriteOnUnpartitionedTable() {
        createUnpartitioned("not_part");
        WriteIntent intent = WriteIntent.builder()
                .overwriteMode(WriteIntent.OverwriteMode.DYNAMIC_PARTITION)
                .build();
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.beginInsert(null, handleFor("not_part"), Collections.emptyList(), intent));
    }

    @Test
    void appendIntentCommitsAppendSnapshot() throws Exception {
        Table t = createUnpartitioned("e2e_app");
        // Stage a real DataFile through iceberg's writer-style APIs would require
        // touching disk; instead, we synthesize a TIcebergCommitData fragment that
        // points at a path we know iceberg won't load (commit doesn't read it).
        DataFile staged = stageDataFile(t, "append-1", 5, null);
        byte[] frag = makeFragment(staged.path().toString(), staged.recordCount(), staged.fileSizeInBytes(), null);
        runInsert(t, WriteIntent.simple(), Collections.singletonList(frag));

        Table reloaded = catalog.loadTable(TableIdentifier.of("db", "e2e_app"));
        Snapshot snap = reloaded.currentSnapshot();
        Assertions.assertNotNull(snap);
        Assertions.assertEquals("append", snap.operation());
        Assertions.assertEquals(1, countDataFiles(reloaded));
    }

    @Test
    void fullTableOverwriteReplacesAllFiles() throws Exception {
        Table t = createUnpartitioned("e2e_full");
        // Seed table with two appended files
        appendStaged(t, Arrays.asList(stageDataFile(t, "old-1", 2, null), stageDataFile(t, "old-2", 3, null)));
        Table snap1 = catalog.loadTable(TableIdentifier.of("db", "e2e_full"));
        Assertions.assertEquals(2, countDataFiles(snap1));

        DataFile newFile = stageDataFile(t, "new-1", 10, null);
        byte[] frag = makeFragment(newFile.path().toString(), newFile.recordCount(), newFile.fileSizeInBytes(), null);
        WriteIntent intent = WriteIntent.builder()
                .overwriteMode(WriteIntent.OverwriteMode.FULL_TABLE)
                .build();
        runInsert(t, intent, Collections.singletonList(frag));

        Table after = catalog.loadTable(TableIdentifier.of("db", "e2e_full"));
        Assertions.assertEquals("overwrite", after.currentSnapshot().operation());
        // The previous two files are dropped, only the new one remains.
        Assertions.assertEquals(1, countDataFiles(after));
    }

    @Test
    void staticPartitionOverwriteReplacesOnlyMatchingPartition() throws Exception {
        Table t = createPartitioned("e2e_static");
        appendStaged(t, Arrays.asList(
                stageDataFile(t, "us-1", 2, Collections.singletonList("us")),
                stageDataFile(t, "eu-1", 3, Collections.singletonList("eu"))));
        Assertions.assertEquals(2, countDataFiles(catalog.loadTable(TableIdentifier.of("db", "e2e_static"))));

        DataFile usReplacement = stageDataFile(t, "us-2", 7, Collections.singletonList("us"));
        byte[] frag = makeFragment(
                usReplacement.path().toString(), usReplacement.recordCount(), usReplacement.fileSizeInBytes(),
                Collections.singletonList("us"));
        Map<String, String> keys = new HashMap<>();
        keys.put("region", "us");
        WriteIntent intent = WriteIntent.builder()
                .overwriteMode(WriteIntent.OverwriteMode.STATIC_PARTITION)
                .staticPartitions(keys)
                .build();
        runInsert(t, intent, Collections.singletonList(frag));

        Table after = catalog.loadTable(TableIdentifier.of("db", "e2e_static"));
        Assertions.assertEquals("overwrite", after.currentSnapshot().operation());
        // Old us-1 is replaced; eu-1 remains; us-2 added → 2 files total.
        List<DataFile> remaining = listDataFiles(after);
        Assertions.assertEquals(2, remaining.size());
        boolean haveEu = remaining.stream().anyMatch(f -> f.path().toString().contains("eu-1"));
        boolean haveUs2 = remaining.stream().anyMatch(f -> f.path().toString().contains("us-2"));
        boolean haveUs1 = remaining.stream().anyMatch(f -> f.path().toString().contains("us-1"));
        Assertions.assertTrue(haveEu, "eu partition should survive");
        Assertions.assertTrue(haveUs2, "new us file should be present");
        Assertions.assertFalse(haveUs1, "old us file should be replaced");
    }

    @Test
    void dynamicPartitionOverwriteReplacesTouchedPartitions() throws Exception {
        Table t = createPartitioned("e2e_dyn");
        appendStaged(t, Arrays.asList(
                stageDataFile(t, "us-1", 2, Collections.singletonList("us")),
                stageDataFile(t, "eu-1", 3, Collections.singletonList("eu"))));

        DataFile newUs = stageDataFile(t, "us-3", 9, Collections.singletonList("us"));
        byte[] frag = makeFragment(
                newUs.path().toString(), newUs.recordCount(), newUs.fileSizeInBytes(),
                Collections.singletonList("us"));
        WriteIntent intent = WriteIntent.builder()
                .overwriteMode(WriteIntent.OverwriteMode.DYNAMIC_PARTITION)
                .build();
        runInsert(t, intent, Collections.singletonList(frag));

        Table after = catalog.loadTable(TableIdentifier.of("db", "e2e_dyn"));
        // ReplacePartitions emits the snapshot operation "overwrite".
        Assertions.assertEquals("overwrite", after.currentSnapshot().operation());
        List<DataFile> remaining = listDataFiles(after);
        Assertions.assertEquals(2, remaining.size());
        Assertions.assertTrue(remaining.stream().anyMatch(f -> f.path().toString().contains("eu-1")));
        Assertions.assertTrue(remaining.stream().anyMatch(f -> f.path().toString().contains("us-3")));
        Assertions.assertFalse(remaining.stream().anyMatch(f -> f.path().toString().contains("us-1")));
    }

    @Test
    void abortInsertIsIdempotentAndDoesNotCommit() {
        createUnpartitioned("e2e_abort");
        IcebergInsertHandle h = (IcebergInsertHandle) metadata.beginInsert(
                null, handleFor("e2e_abort"), Collections.emptyList(), WriteIntent.simple());
        metadata.abortInsert(null, h);
        metadata.abortInsert(null, h);
        Table reloaded = catalog.loadTable(TableIdentifier.of("db", "e2e_abort"));
        Assertions.assertNull(reloaded.currentSnapshot(), "no snapshot should be produced by aborted insert");
    }

    private void runInsert(Table table, WriteIntent intent, List<byte[]> fragments) {
        ConnectorTableHandle handle = handleFor(stripDbPrefix(table.name()));
        IcebergInsertHandle ih = (IcebergInsertHandle) metadata.beginInsert(
                null, handle, Collections.emptyList(), intent);
        metadata.finishInsert(null, ih, fragments);
    }

    private static String stripDbPrefix(String fqn) {
        int dot = fqn.lastIndexOf('.');
        return dot < 0 ? fqn : fqn.substring(dot + 1);
    }

    private DataFile stageDataFile(Table t, String tag, long rows, List<String> partitionValues) throws Exception {
        // Build a DataFile referencing a synthetic path. iceberg's commit logic
        // does not read the file; only manifests need consistent metadata.
        org.apache.iceberg.DataFiles.Builder b = org.apache.iceberg.DataFiles.builder(t.spec())
                .withPath(t.location() + "/data/" + tag + "-" + UUID.randomUUID() + ".parquet")
                .withFileSizeInBytes(1024L)
                .withRecordCount(rows)
                .withFormat(FileFormat.PARQUET);
        if (t.spec().isPartitioned() && partitionValues != null) {
            org.apache.iceberg.PartitionData pd = new org.apache.iceberg.PartitionData(t.spec().partitionType());
            for (int i = 0; i < partitionValues.size(); i++) {
                pd.set(i, partitionValues.get(i));
            }
            b.withPartition(pd);
        }
        return b.build();
    }

    private void appendStaged(Table t, List<DataFile> files) {
        org.apache.iceberg.AppendFiles app = t.newAppend();
        for (DataFile f : files) {
            app.appendFile(f);
        }
        app.commit();
    }

    private List<DataFile> listDataFiles(Table t) {
        List<DataFile> out = new ArrayList<>();
        try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks = t.newScan().planFiles()) {
            tasks.forEach(task -> out.add(task.file()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return out;
    }

    private int countDataFiles(Table t) {
        return listDataFiles(t).size();
    }
}
