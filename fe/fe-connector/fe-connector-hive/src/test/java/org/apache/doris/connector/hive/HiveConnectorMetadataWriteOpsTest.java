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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.write.ConnectorWriteConfig;
import org.apache.doris.connector.api.write.ConnectorWriteType;
import org.apache.doris.connector.api.write.WriteIntent;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsClientException;
import org.apache.doris.connector.hms.HmsPartitionSpec;
import org.apache.doris.connector.hms.HmsTableInfo;
import org.apache.doris.connector.hms.HmsWriteOps;
import org.apache.doris.thrift.THiveLocationParams;
import org.apache.doris.thrift.THivePartitionUpdate;
import org.apache.doris.thrift.TUpdateMode;

import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class HiveConnectorMetadataWriteOpsTest {

    private HmsClient hmsClient;
    private HmsWriteOps writeOps;
    private HiveConnectorMetadata metadata;

    @BeforeEach
    void setUp() {
        hmsClient = Mockito.mock(HmsClient.class);
        writeOps = Mockito.mock(HmsWriteOps.class);
        metadata = new HiveConnectorMetadata(hmsClient, writeOps, null,
                HiveConnectorMetadata.DEFAULT_HEARTBEAT_PERIOD_MS, new HashMap<>(), "hive_test");
    }

    private static byte[] serialize(THivePartitionUpdate update) throws Exception {
        return new TSerializer(new TBinaryProtocol.Factory()).serialize(update);
    }

    private static THivePartitionUpdate update(String name, TUpdateMode mode, String targetPath) {
        THivePartitionUpdate u = new THivePartitionUpdate();
        u.setName(name == null ? "" : name);
        u.setUpdateMode(mode);
        u.setRowCount(1);
        u.setFileSize(100);
        u.setFileNames(new ArrayList<>(Arrays.asList("000000_0")));
        THiveLocationParams loc = new THiveLocationParams();
        loc.setWritePath(targetPath);
        loc.setTargetPath(targetPath);
        u.setLocation(loc);
        return u;
    }

    // ========== capability + config ==========

    @Test
    void supportsInsertReturnsTrue() {
        Assertions.assertTrue(metadata.supportsInsert());
    }

    @Test
    void supportsDeleteAndMergeRequireWriteOps() {
        Assertions.assertTrue(metadata.supportsDelete());
        Assertions.assertTrue(metadata.supportsMerge());
        HiveConnectorMetadata noWrite = new HiveConnectorMetadata(
                hmsClient, null, null, 30_000L, new HashMap<>(), "hive_test");
        Assertions.assertFalse(noWrite.supportsDelete());
        Assertions.assertFalse(noWrite.supportsMerge());
    }

    @Test
    void getWriteConfigReportsLocationAndPartitionsAndAcidFlag() {
        HmsTableInfo info = HiveWriteOpsFixtures.acidTable("db", "t", Collections.singletonList("dt"));
        Mockito.when(hmsClient.getTable("db", "t")).thenReturn(info);
        ConnectorWriteConfig cfg = metadata.getWriteConfig(null,
                HiveWriteOpsFixtures.handleFor(info), Collections.emptyList());
        Assertions.assertEquals(ConnectorWriteType.FILE_WRITE, cfg.getWriteType());
        Assertions.assertEquals("file:///wh/db/t", cfg.getWriteLocation());
        Assertions.assertEquals(Collections.singletonList("dt"), cfg.getPartitionColumns());
        Assertions.assertEquals("true", cfg.getProperties().get("acid"));
    }

    // ========== non-ACID INSERT INTO partitioned ==========

    @Test
    void nonAcidInsertNewPartitionTriggersAddPartitions() throws Exception {
        HmsTableInfo info = HiveWriteOpsFixtures.nonAcidTable("db", "t", Collections.singletonList("dt"));
        Mockito.when(hmsClient.getTable("db", "t")).thenReturn(info);
        Mockito.when(hmsClient.getPartition("db", "t", Collections.singletonList("2024-01-01")))
                .thenThrow(new HmsClientException("not found"));

        HiveInsertHandle handle = (HiveInsertHandle) metadata.beginInsert(
                null, HiveWriteOpsFixtures.handleFor(info), Collections.emptyList(), WriteIntent.simple());
        metadata.finishInsert(null, handle, Collections.singletonList(serialize(
                update("dt=2024-01-01", TUpdateMode.NEW, "file:///wh/db/t/dt=2024-01-01"))));

        ArgumentCaptor<List<HmsPartitionSpec>> captor = newListCaptor();
        Mockito.verify(writeOps).addPartitions(Mockito.eq("db"), Mockito.eq("t"), captor.capture());
        List<HmsPartitionSpec> added = captor.getValue();
        Assertions.assertEquals(1, added.size());
        Assertions.assertEquals(Collections.singletonList("2024-01-01"), added.get(0).getValues());
        Assertions.assertEquals("file:///wh/db/t/dt=2024-01-01", added.get(0).getLocation());
    }

    @Test
    void nonAcidInsertAppendOnExistingPartitionDoesNotMutateMetastore() throws Exception {
        HmsTableInfo info = HiveWriteOpsFixtures.nonAcidTable("db", "t", Collections.singletonList("dt"));
        Mockito.when(hmsClient.getTable("db", "t")).thenReturn(info);

        HiveInsertHandle handle = (HiveInsertHandle) metadata.beginInsert(
                null, HiveWriteOpsFixtures.handleFor(info), Collections.emptyList(), WriteIntent.simple());
        metadata.finishInsert(null, handle, Collections.singletonList(serialize(
                update("dt=2024-01-01", TUpdateMode.APPEND, "file:///wh/db/t/dt=2024-01-01"))));

        Mockito.verify(writeOps, Mockito.never()).addPartitions(Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.verify(writeOps, Mockito.never()).dropPartition(
                Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyBoolean());
    }

    // ========== non-ACID INSERT INTO unpartitioned ==========

    @Test
    void nonAcidInsertUnpartitionedAppendIsNoOp() throws Exception {
        HmsTableInfo info = HiveWriteOpsFixtures.nonAcidTable("db", "t", Collections.emptyList());
        Mockito.when(hmsClient.getTable("db", "t")).thenReturn(info);

        HiveInsertHandle handle = (HiveInsertHandle) metadata.beginInsert(
                null, HiveWriteOpsFixtures.handleFor(info), Collections.emptyList(), WriteIntent.simple());
        metadata.finishInsert(null, handle, Collections.singletonList(serialize(
                update("", TUpdateMode.APPEND, "file:///wh/db/t"))));

        Mockito.verifyNoInteractions(writeOps);
    }

    // ========== non-ACID INSERT OVERWRITE FULL_TABLE ==========

    @Test
    void nonAcidOverwriteFullTableUnpartitionedDoesNotMutateMetastore() throws Exception {
        HmsTableInfo info = HiveWriteOpsFixtures.nonAcidTable("db", "t", Collections.emptyList());
        Mockito.when(hmsClient.getTable("db", "t")).thenReturn(info);
        WriteIntent intent = new WriteIntent.Builder()
                .overwriteMode(WriteIntent.OverwriteMode.FULL_TABLE).build();

        HiveInsertHandle handle = (HiveInsertHandle) metadata.beginInsert(
                null, HiveWriteOpsFixtures.handleFor(info), Collections.emptyList(), intent);
        metadata.finishInsert(null, handle, Collections.singletonList(serialize(
                update("", TUpdateMode.OVERWRITE, "file:///wh/db/t"))));

        // No add/drop required — file replacement is BE/file-system level.
        Mockito.verify(writeOps, Mockito.never()).addPartitions(Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.verify(writeOps, Mockito.never()).dropPartition(
                Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyBoolean());
    }

    // ========== non-ACID INSERT OVERWRITE STATIC_PARTITION ==========

    @Test
    void nonAcidOverwriteStaticPartitionDropsThenAddsPartition() throws Exception {
        HmsTableInfo info = HiveWriteOpsFixtures.nonAcidTable("db", "t", Collections.singletonList("dt"));
        Mockito.when(hmsClient.getTable("db", "t")).thenReturn(info);
        Mockito.when(hmsClient.getPartition("db", "t", Collections.singletonList("2024-01-01")))
                .thenReturn(HiveWriteOpsFixtures.partition(
                        Collections.singletonList("2024-01-01"), "file:///wh/db/t/dt=2024-01-01"));
        Map<String, String> sp = new LinkedHashMap<>();
        sp.put("dt", "2024-01-01");
        WriteIntent intent = new WriteIntent.Builder()
                .overwriteMode(WriteIntent.OverwriteMode.STATIC_PARTITION)
                .staticPartitions(sp).build();

        HiveInsertHandle handle = (HiveInsertHandle) metadata.beginInsert(
                null, HiveWriteOpsFixtures.handleFor(info), Collections.emptyList(), intent);
        metadata.finishInsert(null, handle, Collections.singletonList(serialize(
                update("dt=2024-01-01", TUpdateMode.OVERWRITE, "file:///wh/db/t/dt=2024-01-01"))));

        Mockito.verify(writeOps).dropPartition("db", "t",
                Collections.singletonList("2024-01-01"), false);
        Mockito.verify(writeOps).addPartitions(Mockito.eq("db"), Mockito.eq("t"), Mockito.anyList());
    }

    // ========== non-ACID INSERT OVERWRITE DYNAMIC_PARTITION ==========

    @Test
    void nonAcidOverwriteDynamicPartitionPerPartitionUpdate() throws Exception {
        HmsTableInfo info = HiveWriteOpsFixtures.nonAcidTable("db", "t", Collections.singletonList("dt"));
        Mockito.when(hmsClient.getTable("db", "t")).thenReturn(info);
        Mockito.when(hmsClient.getPartition("db", "t", Collections.singletonList("2024-01-01")))
                .thenReturn(HiveWriteOpsFixtures.partition(
                        Collections.singletonList("2024-01-01"), "file:///wh/db/t/dt=2024-01-01"));
        Mockito.when(hmsClient.getPartition("db", "t", Collections.singletonList("2024-01-02")))
                .thenThrow(new HmsClientException("not found"));
        WriteIntent intent = new WriteIntent.Builder()
                .overwriteMode(WriteIntent.OverwriteMode.DYNAMIC_PARTITION).build();

        HiveInsertHandle handle = (HiveInsertHandle) metadata.beginInsert(
                null, HiveWriteOpsFixtures.handleFor(info), Collections.emptyList(), intent);
        metadata.finishInsert(null, handle, Arrays.asList(
                serialize(update("dt=2024-01-01", TUpdateMode.OVERWRITE, "file:///wh/db/t/dt=2024-01-01")),
                serialize(update("dt=2024-01-02", TUpdateMode.OVERWRITE, "file:///wh/db/t/dt=2024-01-02"))));

        Mockito.verify(writeOps).dropPartition("db", "t",
                Collections.singletonList("2024-01-01"), false);
        Mockito.verify(writeOps, Mockito.times(2)).addPartitions(
                Mockito.eq("db"), Mockito.eq("t"), Mockito.anyList());
        Mockito.verify(writeOps, Mockito.times(1)).dropPartition(
                Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyBoolean());
    }

    @Test
    void staticPartitionOverwriteOnUnpartitionedTableRejected() {
        HmsTableInfo info = HiveWriteOpsFixtures.nonAcidTable("db", "t", Collections.emptyList());
        Mockito.when(hmsClient.getTable("db", "t")).thenReturn(info);
        Map<String, String> sp = new LinkedHashMap<>();
        sp.put("dt", "x");
        WriteIntent intent = new WriteIntent.Builder()
                .overwriteMode(WriteIntent.OverwriteMode.STATIC_PARTITION)
                .staticPartitions(sp).build();
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.beginInsert(null, HiveWriteOpsFixtures.handleFor(info),
                        Collections.emptyList(), intent));
    }

    @Test
    void unknownStaticPartitionColumnRejected() {
        HmsTableInfo info = HiveWriteOpsFixtures.nonAcidTable("db", "t", Collections.singletonList("dt"));
        Mockito.when(hmsClient.getTable("db", "t")).thenReturn(info);
        Map<String, String> sp = new LinkedHashMap<>();
        sp.put("region", "us");
        WriteIntent intent = new WriteIntent.Builder()
                .overwriteMode(WriteIntent.OverwriteMode.STATIC_PARTITION)
                .staticPartitions(sp).build();
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.beginInsert(null, HiveWriteOpsFixtures.handleFor(info),
                        Collections.emptyList(), intent));
    }

    @Test
    void abortInsertCancelsHeartbeatWithoutFailure() {
        HmsTableInfo info = HiveWriteOpsFixtures.nonAcidTable("db", "t", Collections.emptyList());
        Mockito.when(hmsClient.getTable("db", "t")).thenReturn(info);
        HiveInsertHandle handle = (HiveInsertHandle) metadata.beginInsert(
                null, HiveWriteOpsFixtures.handleFor(info), Collections.emptyList(), WriteIntent.simple());
        // No exception expected; nothing to abort metastore-side for non-ACID.
        metadata.abortInsert(null, handle);
        Mockito.verifyNoInteractions(writeOps);
    }

    @SuppressWarnings("unchecked")
    private static <T> ArgumentCaptor<List<T>> newListCaptor() {
        return (ArgumentCaptor<List<T>>) (Object) ArgumentCaptor.forClass(List.class);
    }
}
