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
import org.apache.doris.connector.api.write.WriteIntent;
import org.apache.doris.connector.hms.HmsAcidOperation;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsClientException;
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
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

class HiveAcidWriteOpsTest {

    private HmsClient hmsClient;
    private HmsWriteOps writeOps;
    private ScheduledExecutorService heartbeatExecutor;
    private HiveConnectorMetadata metadata;

    @BeforeEach
    void setUp() {
        hmsClient = Mockito.mock(HmsClient.class);
        writeOps = Mockito.mock(HmsWriteOps.class);
        heartbeatExecutor = Mockito.mock(ScheduledExecutorService.class);
        Mockito.when(heartbeatExecutor.scheduleAtFixedRate(
                        Mockito.any(Runnable.class),
                        Mockito.anyLong(), Mockito.anyLong(), Mockito.any(TimeUnit.class)))
                .thenReturn(Mockito.mock(ScheduledFuture.class));
        Mockito.when(writeOps.openTxn(Mockito.anyString())).thenReturn(7L);
        Mockito.when(writeOps.allocateWriteId(Mockito.eq(7L), Mockito.any(), Mockito.any())).thenReturn(11L);
        Mockito.when(writeOps.acquireLock(Mockito.eq(7L), Mockito.any(), Mockito.any(),
                Mockito.any(), Mockito.any(), Mockito.anyBoolean())).thenReturn(99L);
        metadata = new HiveConnectorMetadata(hmsClient, writeOps, heartbeatExecutor,
                30_000L, new HashMap<>(), "hive_test");
    }

    private static byte[] serialize(THivePartitionUpdate update) throws Exception {
        return new TSerializer(new TBinaryProtocol.Factory()).serialize(update);
    }

    private static THivePartitionUpdate update(String name, TUpdateMode mode) {
        THivePartitionUpdate u = new THivePartitionUpdate();
        u.setName(name == null ? "" : name);
        u.setUpdateMode(mode);
        u.setRowCount(1);
        u.setFileSize(10);
        u.setFileNames(new ArrayList<>(Arrays.asList("000000_0")));
        THiveLocationParams loc = new THiveLocationParams();
        loc.setWritePath("file:///wh/db/t/" + (name.isEmpty() ? "" : name));
        loc.setTargetPath(loc.getWritePath());
        u.setLocation(loc);
        return u;
    }

    // ========== INSERT on ACID table ==========

    @Test
    void acidInsertOpensTxnAllocatesWriteIdAcquiresSharedWriteLockAndCommits() throws Exception {
        HmsTableInfo info = HiveWriteOpsFixtures.acidTable("db", "t", Collections.singletonList("dt"));
        Mockito.when(hmsClient.getTable("db", "t")).thenReturn(info);

        HiveInsertHandle handle = (HiveInsertHandle) metadata.beginInsert(
                null, HiveWriteOpsFixtures.handleFor(info), Collections.emptyList(), WriteIntent.simple());

        Assertions.assertTrue(handle.getContext().isAcid());
        Assertions.assertEquals(7L, handle.getContext().getTxnId());
        Assertions.assertEquals(11L, handle.getContext().getWriteId());
        Assertions.assertEquals(99L, handle.getContext().getLockId());
        Assertions.assertEquals(HmsAcidOperation.INSERT, handle.getContext().getAcidOperation());

        InOrder order = Mockito.inOrder(writeOps);
        order.verify(writeOps).openTxn(Mockito.anyString());
        order.verify(writeOps).allocateWriteId(7L, "db", "t");
        order.verify(writeOps).acquireLock(Mockito.eq(7L), Mockito.any(), Mockito.any(),
                Mockito.eq("db"), Mockito.eq("t"), Mockito.eq(true));
        Mockito.verify(heartbeatExecutor).scheduleAtFixedRate(
                Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.anyLong(), Mockito.any(TimeUnit.class));

        metadata.finishInsert(null, handle, Collections.singletonList(serialize(
                update("dt=2024-01-01", TUpdateMode.NEW))));

        Mockito.verify(writeOps).addDynamicPartitions(7L, 11L, "db", "t",
                Collections.singletonList("dt=2024-01-01"), HmsAcidOperation.INSERT);
        Mockito.verify(writeOps).commitTxn(7L);
    }

    @Test
    void acidInsertOverwriteIsRejected() {
        HmsTableInfo info = HiveWriteOpsFixtures.acidTable("db", "t", Collections.singletonList("dt"));
        Mockito.when(hmsClient.getTable("db", "t")).thenReturn(info);
        WriteIntent intent = new WriteIntent.Builder()
                .overwriteMode(WriteIntent.OverwriteMode.FULL_TABLE).build();
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.beginInsert(null, HiveWriteOpsFixtures.handleFor(info),
                        Collections.emptyList(), intent));
        Mockito.verifyNoInteractions(writeOps);
    }

    // ========== DELETE on ACID table ==========

    @Test
    void deleteOnAcidTableRequestsSharedWriteLockAndCommits() throws Exception {
        HmsTableInfo info = HiveWriteOpsFixtures.acidTable("db", "t", Collections.singletonList("dt"));
        Mockito.when(hmsClient.getTable("db", "t")).thenReturn(info);

        HiveDeleteHandle handle = (HiveDeleteHandle) metadata.beginDelete(
                null, HiveWriteOpsFixtures.handleFor(info));

        Assertions.assertEquals(HmsAcidOperation.DELETE, handle.getContext().getAcidOperation());
        Mockito.verify(writeOps).acquireLock(Mockito.eq(7L), Mockito.any(), Mockito.any(),
                Mockito.eq("db"), Mockito.eq("t"), Mockito.eq(true));

        metadata.finishDelete(null, handle, Collections.singletonList(serialize(
                update("dt=2024-01-01", TUpdateMode.APPEND))));
        Mockito.verify(writeOps).addDynamicPartitions(7L, 11L, "db", "t",
                Collections.singletonList("dt=2024-01-01"), HmsAcidOperation.DELETE);
        Mockito.verify(writeOps).commitTxn(7L);
    }

    @Test
    void deleteOnNonAcidTableRejected() {
        HmsTableInfo info = HiveWriteOpsFixtures.nonAcidTable("db", "t", Collections.emptyList());
        Mockito.when(hmsClient.getTable("db", "t")).thenReturn(info);
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.beginDelete(null, HiveWriteOpsFixtures.handleFor(info)));
    }

    // ========== UPDATE / MERGE on ACID table ==========

    @Test
    void mergeOnAcidTableRegistersUpdateOperationOnCommit() throws Exception {
        HmsTableInfo info = HiveWriteOpsFixtures.acidTable("db", "t", Collections.singletonList("dt"));
        Mockito.when(hmsClient.getTable("db", "t")).thenReturn(info);

        HiveMergeHandle handle = (HiveMergeHandle) metadata.beginMerge(
                null, HiveWriteOpsFixtures.handleFor(info));
        Assertions.assertEquals(HmsAcidOperation.UPDATE, handle.getContext().getAcidOperation());

        metadata.finishMerge(null, handle, Collections.singletonList(serialize(
                update("dt=2024-01-01", TUpdateMode.APPEND))));
        Mockito.verify(writeOps).addDynamicPartitions(7L, 11L, "db", "t",
                Collections.singletonList("dt=2024-01-01"), HmsAcidOperation.UPDATE);
        Mockito.verify(writeOps).commitTxn(7L);
    }

    @Test
    void mergeOnNonAcidTableRejected() {
        HmsTableInfo info = HiveWriteOpsFixtures.nonAcidTable("db", "t", Collections.emptyList());
        Mockito.when(hmsClient.getTable("db", "t")).thenReturn(info);
        Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.beginMerge(null, HiveWriteOpsFixtures.handleFor(info)));
    }

    // ========== abort + heartbeat lifecycle ==========

    @Test
    void abortInsertOnAcidTableInvokesAbortTxnAndCancelsHeartbeat() {
        HmsTableInfo info = HiveWriteOpsFixtures.acidTable("db", "t", Collections.emptyList());
        Mockito.when(hmsClient.getTable("db", "t")).thenReturn(info);
        ScheduledFuture<?> future = Mockito.mock(ScheduledFuture.class);
        Mockito.when(heartbeatExecutor.scheduleAtFixedRate(
                Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.anyLong(), Mockito.any(TimeUnit.class)))
                .thenReturn((ScheduledFuture) future);

        HiveInsertHandle handle = (HiveInsertHandle) metadata.beginInsert(
                null, HiveWriteOpsFixtures.handleFor(info), Collections.emptyList(), WriteIntent.simple());
        metadata.abortInsert(null, handle);

        Mockito.verify(writeOps).abortTxn(7L);
        Mockito.verify(future).cancel(false);
    }

    @Test
    void heartbeatCancelOnSuccessfulCommit() throws Exception {
        HmsTableInfo info = HiveWriteOpsFixtures.acidTable("db", "t", Collections.emptyList());
        Mockito.when(hmsClient.getTable("db", "t")).thenReturn(info);
        ScheduledFuture<?> future = Mockito.mock(ScheduledFuture.class);
        Mockito.when(heartbeatExecutor.scheduleAtFixedRate(
                Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.anyLong(), Mockito.any(TimeUnit.class)))
                .thenReturn((ScheduledFuture) future);

        HiveInsertHandle handle = (HiveInsertHandle) metadata.beginInsert(
                null, HiveWriteOpsFixtures.handleFor(info), Collections.emptyList(), WriteIntent.simple());
        metadata.finishInsert(null, handle, Collections.singletonList(serialize(
                update("", TUpdateMode.APPEND))));

        Mockito.verify(future).cancel(false);
    }

    @Test
    void lockFailureBubblesUpAndAbortsTxn() {
        HmsTableInfo info = HiveWriteOpsFixtures.acidTable("db", "t", Collections.emptyList());
        Mockito.when(hmsClient.getTable("db", "t")).thenReturn(info);
        Mockito.when(writeOps.acquireLock(Mockito.eq(7L), Mockito.any(), Mockito.any(),
                Mockito.eq("db"), Mockito.eq("t"), Mockito.anyBoolean()))
                .thenThrow(new HmsClientException("lock denied"));

        HmsClientException thrown = Assertions.assertThrows(HmsClientException.class,
                () -> metadata.beginInsert(null, HiveWriteOpsFixtures.handleFor(info),
                        Collections.emptyList(), WriteIntent.simple()));
        Assertions.assertTrue(thrown.getMessage().contains("lock denied"));
        Mockito.verify(writeOps).abortTxn(7L);
    }

    @Test
    void heartbeatExecutesAgainstWriteOps() throws Exception {
        // Use a real single-thread executor to verify the scheduled task
        // actually invokes HmsWriteOps#heartbeat.
        ScheduledExecutorService realExec = Executors.newSingleThreadScheduledExecutor();
        try {
            HiveConnectorMetadata local = new HiveConnectorMetadata(hmsClient, writeOps, realExec,
                    20L, new HashMap<>(), "hive_test");
            HmsTableInfo info = HiveWriteOpsFixtures.acidTable("db", "t", Collections.emptyList());
            Mockito.when(hmsClient.getTable("db", "t")).thenReturn(info);

            HiveInsertHandle handle = (HiveInsertHandle) local.beginInsert(
                    null, HiveWriteOpsFixtures.handleFor(info), Collections.emptyList(), WriteIntent.simple());
            // Wait for at least one heartbeat to fire.
            Thread.sleep(80L);
            local.abortInsert(null, handle);
            Mockito.verify(writeOps, Mockito.atLeastOnce()).heartbeat(7L, 99L);
        } finally {
            realExec.shutdownNow();
        }
    }
}
