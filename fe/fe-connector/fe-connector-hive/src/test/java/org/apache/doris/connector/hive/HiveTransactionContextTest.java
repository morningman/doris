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
import org.apache.doris.connector.api.write.ConnectorTransactionContext;
import org.apache.doris.connector.api.write.ConnectorTxnCapability;
import org.apache.doris.connector.api.write.WriteIntent;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsTableInfo;
import org.apache.doris.connector.hms.HmsWriteOps;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

class HiveTransactionContextTest {

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
        Mockito.when(writeOps.allocateWriteId(Mockito.eq(7L), Mockito.any(), Mockito.any()))
                .thenReturn(11L);
        Mockito.when(writeOps.acquireLock(Mockito.eq(7L), Mockito.any(), Mockito.any(),
                Mockito.any(), Mockito.any(), Mockito.anyBoolean())).thenReturn(99L);
        metadata = new HiveConnectorMetadata(hmsClient, writeOps, heartbeatExecutor,
                30_000L, new HashMap<>(), "hive_test");
    }

    @Test
    void connectorAdvertisesUnionOfPossibleCapabilities() {
        Assertions.assertTrue(metadata.txnCapabilities().contains(ConnectorTxnCapability.MULTI_STATEMENT));
        Assertions.assertTrue(metadata.txnCapabilities().contains(ConnectorTxnCapability.FAILOVER_SAFE));
        Assertions.assertFalse(metadata.txnCapabilities().contains(ConnectorTxnCapability.COMMIT_RETRY),
                "HMS rejects duplicate commit_txn on the same txnId; COMMIT_RETRY must not be advertised");
        Assertions.assertFalse(metadata.txnCapabilities().contains(ConnectorTxnCapability.SAVEPOINT));
    }

    @Test
    void acidTableContextHasMultiStatementAndFailoverCaps() {
        HmsTableInfo info = HiveWriteOpsFixtures.acidTable("db", "t", Collections.singletonList("dt"));
        Mockito.when(hmsClient.getTable("db", "t")).thenReturn(info);

        ConnectorTransactionContext ctx = metadata.beginTransaction(
                null, HiveWriteOpsFixtures.handleFor(info), WriteIntent.simple());
        Assertions.assertTrue(ctx instanceof HiveTransactionContext);
        HiveTransactionContext hctx = (HiveTransactionContext) ctx;

        Assertions.assertTrue(hctx.isAcid());
        Assertions.assertTrue(hctx.txnCapabilities().contains(ConnectorTxnCapability.MULTI_STATEMENT));
        Assertions.assertTrue(hctx.txnCapabilities().contains(ConnectorTxnCapability.FAILOVER_SAFE));
        Assertions.assertFalse(hctx.txnCapabilities().contains(ConnectorTxnCapability.COMMIT_RETRY));
    }

    @Test
    void acidTxnIdEqualsHmsTxnId() {
        HmsTableInfo info = HiveWriteOpsFixtures.acidTable("db", "t", Collections.emptyList());
        Mockito.when(hmsClient.getTable("db", "t")).thenReturn(info);

        HiveTransactionContext ctx = (HiveTransactionContext) metadata.beginTransaction(
                null, HiveWriteOpsFixtures.handleFor(info), WriteIntent.simple());
        Assertions.assertEquals("hive-acid-7", ctx.txnId());
        Assertions.assertEquals(7L, ctx.getAcidContext().getTxnId());
        Assertions.assertEquals(11L, ctx.getAcidContext().getWriteId());
        Assertions.assertEquals(99L, ctx.getAcidContext().getLockId());
    }

    @Test
    void nonAcidTableContextHasEmptyCapabilities() {
        HmsTableInfo info = HiveWriteOpsFixtures.nonAcidTable("db", "n", Collections.emptyList());
        Mockito.when(hmsClient.getTable("db", "n")).thenReturn(info);

        HiveTransactionContext ctx = (HiveTransactionContext) metadata.beginTransaction(
                null, HiveWriteOpsFixtures.handleFor(info), WriteIntent.simple());
        Assertions.assertFalse(ctx.isAcid());
        Assertions.assertTrue(ctx.txnCapabilities().isEmpty(),
                "non-ACID hive writes have no atomic FS commit and no HMS-allocated txn handle");
    }

    @Test
    void nonAcidTxnIdIsPluginGeneratedAndStable() {
        HmsTableInfo info = HiveWriteOpsFixtures.nonAcidTable("db", "n", Collections.emptyList());
        Mockito.when(hmsClient.getTable("db", "n")).thenReturn(info);

        HiveTransactionContext ctx = (HiveTransactionContext) metadata.beginTransaction(
                null, HiveWriteOpsFixtures.handleFor(info), WriteIntent.simple());
        Assertions.assertTrue(ctx.txnId().startsWith("hive-fs-db.n-"));
        Assertions.assertEquals(ctx.txnId(), ctx.txnId());
    }

    @Test
    void labelEncodesAcidnessAndOverwriteMode() {
        HmsTableInfo info = HiveWriteOpsFixtures.acidTable("db", "t", Collections.emptyList());
        Mockito.when(hmsClient.getTable("db", "t")).thenReturn(info);

        HiveTransactionContext ctx = (HiveTransactionContext) metadata.beginTransaction(
                null, HiveWriteOpsFixtures.handleFor(info), WriteIntent.simple());
        Assertions.assertTrue(ctx.label().contains("db.t"));
        Assertions.assertTrue(ctx.label().contains("NONE"));
        Assertions.assertTrue(ctx.label().endsWith(":acid"));
    }

    @Test
    void serializeIsDeferredEvenForAcid() {
        HmsTableInfo info = HiveWriteOpsFixtures.acidTable("db", "t", Collections.emptyList());
        Mockito.when(hmsClient.getTable("db", "t")).thenReturn(info);

        HiveTransactionContext ctx = (HiveTransactionContext) metadata.beginTransaction(
                null, HiveWriteOpsFixtures.handleFor(info), WriteIntent.simple());
        Assertions.assertFalse(ctx.supportsFailover(),
                "FAILOVER_SAFE capability is advertised but the wire format lands in M3-15");
        Assertions.assertThrows(DorisConnectorException.class, ctx::serialize);
    }
}
