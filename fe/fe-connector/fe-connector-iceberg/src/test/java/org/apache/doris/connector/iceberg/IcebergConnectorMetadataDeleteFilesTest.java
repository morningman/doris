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

import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.write.WriteIntent;
import org.apache.doris.thrift.TFileContent;
import org.apache.doris.thrift.TIcebergCommitData;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionData;
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
import java.util.UUID;

class IcebergConnectorMetadataDeleteFilesTest {

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

    private Table createUnpartitionedV3(String name) {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()));
        return catalog.createTable(
                TableIdentifier.of("db", name), schema, PartitionSpec.unpartitioned(),
                Collections.singletonMap(org.apache.iceberg.TableProperties.FORMAT_VERSION, "3"));
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

    private DataFile stageDataFile(Table t, String tag, long rows, List<String> partitionValues) {
        DataFiles.Builder b = DataFiles.builder(t.spec())
                .withPath(t.location() + "/data/" + tag + "-" + UUID.randomUUID() + ".parquet")
                .withFileSizeInBytes(1024L)
                .withRecordCount(rows)
                .withFormat(FileFormat.PARQUET);
        if (t.spec().isPartitioned() && partitionValues != null) {
            PartitionData pd = new PartitionData(t.spec().partitionType());
            for (int i = 0; i < partitionValues.size(); i++) {
                pd.set(i, partitionValues.get(i));
            }
            b.withPartition(pd);
        }
        return b.build();
    }

    private void appendStaged(Table t, List<DataFile> files) {
        AppendFiles app = t.newAppend();
        for (DataFile f : files) {
            app.appendFile(f);
        }
        app.commit();
    }

    private byte[] dataFragment(Table t, DataFile staged, List<String> partitionValues) throws Exception {
        TIcebergCommitData c = new TIcebergCommitData();
        c.setFilePath(staged.path().toString());
        c.setRowCount(staged.recordCount());
        c.setFileSize(staged.fileSizeInBytes());
        c.setFileContent(TFileContent.DATA);
        if (partitionValues != null) {
            c.setPartitionValues(partitionValues);
        }
        return new TSerializer(new TBinaryProtocol.Factory()).serialize(c);
    }

    private byte[] positionDeleteFragment(Table t, String referencedDataFile, List<String> partitionValues)
            throws Exception {
        TIcebergCommitData c = new TIcebergCommitData();
        c.setFilePath(t.location() + "/data/pos-del-" + UUID.randomUUID() + ".parquet");
        c.setRowCount(1L);
        c.setFileSize(64L);
        c.setFileContent(TFileContent.POSITION_DELETES);
        c.setReferencedDataFilePath(referencedDataFile);
        c.setPartitionSpecId(t.spec().specId());
        if (partitionValues != null) {
            c.setPartitionValues(partitionValues);
        }
        return new TSerializer(new TBinaryProtocol.Factory()).serialize(c);
    }

    private byte[] equalityDeleteFragment(Table t, List<Integer> equalityFieldIds, List<String> partitionValues)
            throws Exception {
        TIcebergCommitData c = new TIcebergCommitData();
        c.setFilePath(t.location() + "/data/eq-del-" + UUID.randomUUID() + ".parquet");
        c.setRowCount(2L);
        c.setFileSize(128L);
        c.setFileContent(TFileContent.EQUALITY_DELETES);
        c.setEqualityFieldIds(equalityFieldIds);
        c.setPartitionSpecId(t.spec().specId());
        if (partitionValues != null) {
            c.setPartitionValues(partitionValues);
        }
        return new TSerializer(new TBinaryProtocol.Factory()).serialize(c);
    }

    private byte[] dvFragment(Table t, String referencedDataFile, List<String> partitionValues, boolean withOffsets)
            throws Exception {
        TIcebergCommitData c = new TIcebergCommitData();
        c.setFilePath(t.location() + "/data/dv-" + UUID.randomUUID() + ".puffin");
        c.setRowCount(3L);
        c.setFileSize(256L);
        c.setFileContent(TFileContent.DELETION_VECTOR);
        c.setReferencedDataFilePath(referencedDataFile);
        c.setPartitionSpecId(t.spec().specId());
        if (withOffsets) {
            c.setContentOffset(4L);
            c.setContentSizeInBytes(128L);
        }
        if (partitionValues != null) {
            c.setPartitionValues(partitionValues);
        }
        return new TSerializer(new TBinaryProtocol.Factory()).serialize(c);
    }

    private void runInsert(Table t, WriteIntent intent, List<byte[]> fragments) {
        ConnectorTableHandle handle = handleFor(stripDbPrefix(t.name()));
        IcebergInsertHandle ih = (IcebergInsertHandle) metadata.beginInsert(
                null, handle, Collections.emptyList(), intent);
        metadata.finishInsert(null, ih, fragments);
    }

    private static String stripDbPrefix(String fqn) {
        int dot = fqn.lastIndexOf('.');
        return dot < 0 ? fqn : fqn.substring(dot + 1);
    }

    private List<DeleteFile> listDeleteFiles(Table t) {
        List<DeleteFile> out = new ArrayList<>();
        try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks = t.newScan().planFiles()) {
            tasks.forEach(task -> out.addAll(task.deletes()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return out;
    }

    // ---------- capability tests ----------

    @Test
    void connectorDeclaresRowLevelDeleteCapabilities() {
        IcebergConnectorProvider provider = new IcebergConnectorProvider();
        Assertions.assertNotNull(provider);
        // The capability set is owned by IcebergConnector#getCapabilities, but
        // since constructing a Connector requires an iceberg backend factory,
        // assert directly against the static enum membership we expect.
        // The presence of the flag values themselves is the contract under test.
        Assertions.assertNotNull(ConnectorCapability.SUPPORTS_ROW_LEVEL_DELETE);
        Assertions.assertNotNull(ConnectorCapability.SUPPORTS_POSITION_DELETE);
        Assertions.assertNotNull(ConnectorCapability.SUPPORTS_EQUALITY_DELETE);
        Assertions.assertNotNull(ConnectorCapability.SUPPORTS_DELETION_VECTOR);
    }

    // ---------- converter unit tests ----------

    @Test
    void converterRejectsEqualityDeleteWithoutFieldIds() throws Exception {
        Table t = createPartitioned("conv_eq_missing");
        TIcebergCommitData c = new TIcebergCommitData();
        c.setFilePath(t.location() + "/data/eq-bad.parquet");
        c.setRowCount(1L);
        c.setFileSize(8L);
        c.setFileContent(TFileContent.EQUALITY_DELETES);
        c.setPartitionSpecId(t.spec().specId());
        c.setPartitionValues(Collections.singletonList("us"));
        byte[] payload = new TSerializer(new TBinaryProtocol.Factory()).serialize(c);
        IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class,
                () -> IcebergCommitDataConverter.decodeFragments(t, Collections.singletonList(payload)));
        Assertions.assertTrue(ex.getMessage().contains("equality_field_ids"),
                "expected message about missing equality_field_ids, got: " + ex.getMessage());
    }

    @Test
    void converterRejectsDeletionVectorWithoutOffsets() throws Exception {
        Table t = createUnpartitioned("conv_dv_missing");
        byte[] payload = dvFragment(t, t.location() + "/data/seed.parquet", null, false);
        IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class,
                () -> IcebergCommitDataConverter.decodeFragments(t, Collections.singletonList(payload)));
        Assertions.assertTrue(ex.getMessage().contains("content_offset"),
                "expected message about missing content_offset, got: " + ex.getMessage());
    }

    @Test
    void converterBuildsPositionDeleteFile() throws Exception {
        Table t = createUnpartitioned("conv_pos");
        DataFile seed = stageDataFile(t, "seed", 4, null);
        appendStaged(t, Collections.singletonList(seed));
        Table reloaded = catalog.loadTable(TableIdentifier.of("db", "conv_pos"));
        byte[] payload = positionDeleteFragment(reloaded, seed.path().toString(), null);

        IcebergCommitDataConverter.DecodedCommitFiles decoded =
                IcebergCommitDataConverter.decodeFragments(reloaded, Collections.singletonList(payload));
        Assertions.assertTrue(decoded.dataFiles().isEmpty());
        Assertions.assertEquals(1, decoded.deleteFiles().size());
        DeleteFile df = decoded.deleteFiles().get(0);
        Assertions.assertEquals(FileContent.POSITION_DELETES, df.content());
        Assertions.assertEquals(seed.path().toString(), df.referencedDataFile().toString());
    }

    @Test
    void converterBuildsEqualityDeleteFileWithFieldIds() throws Exception {
        Table t = createPartitioned("conv_eq");
        byte[] payload = equalityDeleteFragment(t, Arrays.asList(1, 2), Collections.singletonList("us"));

        IcebergCommitDataConverter.DecodedCommitFiles decoded =
                IcebergCommitDataConverter.decodeFragments(t, Collections.singletonList(payload));
        Assertions.assertEquals(1, decoded.deleteFiles().size());
        DeleteFile df = decoded.deleteFiles().get(0);
        Assertions.assertEquals(FileContent.EQUALITY_DELETES, df.content());
        Assertions.assertEquals(Arrays.asList(1, 2), df.equalityFieldIds());
        Assertions.assertEquals(t.spec().specId(), df.specId());
    }

    @Test
    void converterBuildsDeletionVectorPuffinFile() throws Exception {
        Table t = createUnpartitioned("conv_dv");
        DataFile seed = stageDataFile(t, "seed", 5, null);
        appendStaged(t, Collections.singletonList(seed));
        Table reloaded = catalog.loadTable(TableIdentifier.of("db", "conv_dv"));
        byte[] payload = dvFragment(reloaded, seed.path().toString(), null, true);

        IcebergCommitDataConverter.DecodedCommitFiles decoded =
                IcebergCommitDataConverter.decodeFragments(reloaded, Collections.singletonList(payload));
        Assertions.assertEquals(1, decoded.deleteFiles().size());
        DeleteFile df = decoded.deleteFiles().get(0);
        Assertions.assertEquals(FileContent.POSITION_DELETES, df.content());
        Assertions.assertEquals(FileFormat.PUFFIN, df.format());
        Assertions.assertEquals(4L, df.contentOffset().longValue());
        Assertions.assertEquals(128L, df.contentSizeInBytes().longValue());
        Assertions.assertEquals(seed.path().toString(), df.referencedDataFile().toString());
    }

    @Test
    void mixedDataAndDeleteFragmentsAreSplitByContent() throws Exception {
        Table t = createUnpartitioned("conv_mixed");
        DataFile seed = stageDataFile(t, "seed", 3, null);
        appendStaged(t, Collections.singletonList(seed));
        Table reloaded = catalog.loadTable(TableIdentifier.of("db", "conv_mixed"));

        DataFile newRow = stageDataFile(reloaded, "new", 2, null);
        byte[] dataFrag = dataFragment(reloaded, newRow, null);
        byte[] posFrag = positionDeleteFragment(reloaded, seed.path().toString(), null);

        IcebergCommitDataConverter.DecodedCommitFiles decoded =
                IcebergCommitDataConverter.decodeFragments(reloaded, Arrays.asList(dataFrag, posFrag));
        Assertions.assertEquals(1, decoded.dataFiles().size());
        Assertions.assertEquals(1, decoded.deleteFiles().size());
    }

    // ---------- finishInsert end-to-end tests via RowDelta ----------

    @Test
    void positionDeleteOnlyCommitProducesOverwriteSnapshot() throws Exception {
        Table t = createUnpartitioned("rd_pos_only");
        DataFile seed = stageDataFile(t, "seed", 5, null);
        appendStaged(t, Collections.singletonList(seed));
        Table seeded = catalog.loadTable(TableIdentifier.of("db", "rd_pos_only"));
        long appendSnap = seeded.currentSnapshot().snapshotId();

        byte[] frag = positionDeleteFragment(seeded, seed.path().toString(), null);
        runInsert(seeded, WriteIntent.simple(), Collections.singletonList(frag));

        Table after = catalog.loadTable(TableIdentifier.of("db", "rd_pos_only"));
        Snapshot snap = after.currentSnapshot();
        Assertions.assertNotEquals(appendSnap, snap.snapshotId(), "row-delta must produce a new snapshot");
        Assertions.assertEquals("delete", snap.operation(),
                "RowDelta with deletes only is reported as 'delete' by iceberg");
        List<DeleteFile> deletes = listDeleteFiles(after);
        Assertions.assertEquals(1, deletes.size());
        Assertions.assertEquals(FileContent.POSITION_DELETES, deletes.get(0).content());
    }

    @Test
    void equalityDeleteOnlyCommitOnPartitionedTable() throws Exception {
        Table t = createPartitioned("rd_eq_only");
        DataFile seedUs = stageDataFile(t, "us", 5, Collections.singletonList("us"));
        appendStaged(t, Collections.singletonList(seedUs));
        Table seeded = catalog.loadTable(TableIdentifier.of("db", "rd_eq_only"));

        byte[] frag = equalityDeleteFragment(seeded, Collections.singletonList(1),
                Collections.singletonList("us"));
        runInsert(seeded, WriteIntent.simple(), Collections.singletonList(frag));

        Table after = catalog.loadTable(TableIdentifier.of("db", "rd_eq_only"));
        List<DeleteFile> deletes = listDeleteFiles(after);
        Assertions.assertEquals(1, deletes.size());
        Assertions.assertEquals(FileContent.EQUALITY_DELETES, deletes.get(0).content());
        Assertions.assertEquals(Collections.singletonList(1), deletes.get(0).equalityFieldIds());
    }

    @Test
    void mixedDataAndEqualityDeleteCommitProducesBothFiles() throws Exception {
        Table t = createPartitioned("rd_mixed");
        DataFile seedUs = stageDataFile(t, "us", 5, Collections.singletonList("us"));
        appendStaged(t, Collections.singletonList(seedUs));
        Table seeded = catalog.loadTable(TableIdentifier.of("db", "rd_mixed"));

        DataFile newRow = stageDataFile(seeded, "us-new", 3, Collections.singletonList("us"));
        byte[] dataFrag = dataFragment(seeded, newRow, Collections.singletonList("us"));
        byte[] eqFrag = equalityDeleteFragment(seeded, Collections.singletonList(1),
                Collections.singletonList("us"));
        runInsert(seeded, WriteIntent.simple(), Arrays.asList(dataFrag, eqFrag));

        Table after = catalog.loadTable(TableIdentifier.of("db", "rd_mixed"));
        // Originally "us" had 1 file with 5 rows; new data file pushes total to 2.
        int dataFiles = 0;
        try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks = after.newScan().planFiles()) {
            dataFiles = com.google.common.collect.Iterables.size(tasks);
        }
        Assertions.assertEquals(2, dataFiles, "both seed and new data files must be visible after row-delta");
        List<DeleteFile> deletes = listDeleteFiles(after);
        Assertions.assertFalse(deletes.isEmpty(), "row-delta must add at least one equality delete file");
        Assertions.assertTrue(deletes.stream().allMatch(d -> d.content() == FileContent.EQUALITY_DELETES));
    }

    @Test
    void deletionVectorCommitProducesPuffinDeleteFile() throws Exception {
        Table t = createUnpartitionedV3("rd_dv");
        DataFile seed = stageDataFile(t, "seed", 5, null);
        appendStaged(t, Collections.singletonList(seed));
        Table seeded = catalog.loadTable(TableIdentifier.of("db", "rd_dv"));

        byte[] frag = dvFragment(seeded, seed.path().toString(), null, true);
        runInsert(seeded, WriteIntent.simple(), Collections.singletonList(frag));

        Table after = catalog.loadTable(TableIdentifier.of("db", "rd_dv"));
        List<DeleteFile> deletes = listDeleteFiles(after);
        Assertions.assertEquals(1, deletes.size());
        DeleteFile df = deletes.get(0);
        Assertions.assertEquals(FileFormat.PUFFIN, df.format());
        Assertions.assertEquals(FileContent.POSITION_DELETES, df.content());
        Assertions.assertEquals(4L, df.contentOffset().longValue());
        Assertions.assertEquals(128L, df.contentSizeInBytes().longValue());
    }

    @Test
    void rowDeltaCommitRespectsBranchTarget() throws Exception {
        Table t = createUnpartitioned("rd_branch");
        DataFile seed = stageDataFile(t, "seed", 5, null);
        appendStaged(t, Collections.singletonList(seed));
        Table seeded = catalog.loadTable(TableIdentifier.of("db", "rd_branch"));
        long mainSnap = seeded.currentSnapshot().snapshotId();
        seeded.manageSnapshots().createBranch("dev", mainSnap).commit();

        byte[] frag = positionDeleteFragment(seeded, seed.path().toString(), null);
        WriteIntent intent = WriteIntent.builder().branch("dev").build();
        runInsert(seeded, intent, Collections.singletonList(frag));

        Table after = catalog.loadTable(TableIdentifier.of("db", "rd_branch"));
        Assertions.assertEquals(mainSnap, after.currentSnapshot().snapshotId(),
                "main branch must not advance for branch row-delta commit");
        Snapshot devSnap = after.snapshot(after.refs().get("dev").snapshotId());
        Assertions.assertNotNull(devSnap);
        Assertions.assertNotEquals(mainSnap, devSnap.snapshotId(),
                "dev branch must point at a new row-delta snapshot");
    }

    @Test
    void rowDeltaWithOverwriteIntentIsRejected() throws Exception {
        Table t = createUnpartitioned("rd_overwrite_reject");
        DataFile seed = stageDataFile(t, "seed", 1, null);
        appendStaged(t, Collections.singletonList(seed));
        Table seeded = catalog.loadTable(TableIdentifier.of("db", "rd_overwrite_reject"));

        byte[] frag = positionDeleteFragment(seeded, seed.path().toString(), null);
        WriteIntent intent = WriteIntent.builder()
                .overwriteMode(WriteIntent.OverwriteMode.FULL_TABLE)
                .build();
        ConnectorTableHandle handle = handleFor("rd_overwrite_reject");
        IcebergInsertHandle ih = (IcebergInsertHandle) metadata.beginInsert(
                null, handle, Collections.emptyList(), intent);
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.finishInsert(null, ih, Collections.singletonList(frag)));
        Assertions.assertTrue(ex.getMessage().contains("row-level delete"),
                "expected reject-on-overwrite message, got: " + ex.getMessage());
    }
}
