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

package org.apache.doris.connector.audit;

import org.apache.doris.connector.api.audit.ConnectorAuditEvent;
import org.apache.doris.connector.api.audit.ConnectorAuditOps.AuditEventKind;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.plugin.AuditEvent.EventType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class PluginAuditEventBridgeTest {

    private AtomicReference<AuditEvent> last;
    private AtomicInteger sinkCalls;

    @BeforeEach
    public void setUp() {
        last = new AtomicReference<>();
        sinkCalls = new AtomicInteger();
        PluginAuditEventBridge.setSink(e -> {
            sinkCalls.incrementAndGet();
            last.set(e);
        });
    }

    @AfterEach
    public void tearDown() {
        PluginAuditEventBridge.setSink(null);
    }

    @Test
    public void kindOfPlanCompleted() {
        ConnectorAuditEvent e = new ConnectorAuditEvent.PlanCompletedAuditEvent(
                "ctl", 1L, Optional.of("q"), "db", "t", 5L, 10L);
        Assertions.assertEquals(AuditEventKind.PLAN_COMPLETED, PluginAuditEventBridge.kindOf(e));
    }

    @Test
    public void kindOfCommitCompleted() {
        ConnectorAuditEvent e = new ConnectorAuditEvent.CommitCompletedAuditEvent(
                "ctl", 1L, Optional.empty(), "db", "t", "c1", 7L);
        Assertions.assertEquals(AuditEventKind.COMMIT_COMPLETED, PluginAuditEventBridge.kindOf(e));
    }

    @Test
    public void kindOfMtmvRefresh() {
        ConnectorAuditEvent e = new ConnectorAuditEvent.MtmvRefreshAuditEvent(
                "ctl", 1L, Optional.empty(), "db", "t", Optional.of("p1"), 99L);
        Assertions.assertEquals(AuditEventKind.MTMV_REFRESH, PluginAuditEventBridge.kindOf(e));
    }

    @Test
    public void kindOfPolicyEval() {
        ConnectorAuditEvent e = new ConnectorAuditEvent.PolicyEvalAuditEvent(
                "ctl", 1L, Optional.empty(), "db", "t", "u", "RLS", true);
        Assertions.assertEquals(AuditEventKind.POLICY_EVAL, PluginAuditEventBridge.kindOf(e));
    }

    @Test
    public void bridgePlanCompletedMapsAllFields() {
        ConnectorAuditEvent e = new ConnectorAuditEvent.PlanCompletedAuditEvent(
                "myctl", 12345L, Optional.of("q-7"), "mydb", "mytbl", 88L, 4L);
        PluginAuditEventBridge.bridge(e);
        AuditEvent out = last.get();
        Assertions.assertNotNull(out);
        Assertions.assertEquals(EventType.PLUGIN_PLAN_COMPLETED, out.type);
        Assertions.assertEquals(12345L, out.timestamp);
        Assertions.assertEquals("myctl", out.ctl);
        Assertions.assertEquals("mydb", out.db);
        Assertions.assertEquals("q-7", out.queryId);
        Assertions.assertEquals("mydb.mytbl", out.queriedTablesAndViews);
        Assertions.assertEquals("88", out.planTimesMs);
        Assertions.assertEquals(4L, out.scanRows);
    }

    @Test
    public void bridgeCommitCompletedMapsAllFields() {
        ConnectorAuditEvent e = new ConnectorAuditEvent.CommitCompletedAuditEvent(
                "ctl", 222L, Optional.empty(), "db", "t", "snap-1", 1000L);
        PluginAuditEventBridge.bridge(e);
        AuditEvent out = last.get();
        Assertions.assertNotNull(out);
        Assertions.assertEquals(EventType.PLUGIN_COMMIT_COMPLETED, out.type);
        Assertions.assertEquals("commitId=snap-1", out.stmt);
        Assertions.assertEquals(1000L, out.returnRows);
        Assertions.assertEquals("", out.queryId);
    }

    @Test
    public void bridgeMtmvRefreshIncludesPartitionAndSnapshot() {
        ConnectorAuditEvent e = new ConnectorAuditEvent.MtmvRefreshAuditEvent(
                "ctl", 1L, Optional.empty(), "db", "t", Optional.of("p=1"), 42L);
        PluginAuditEventBridge.bridge(e);
        AuditEvent out = last.get();
        Assertions.assertEquals(EventType.PLUGIN_MTMV_REFRESH, out.type);
        Assertions.assertTrue(out.stmt.contains("partition=p=1"));
        Assertions.assertTrue(out.stmt.contains("snapshot=42"));
    }

    @Test
    public void bridgeMtmvRefreshHandlesEmptyPartition() {
        ConnectorAuditEvent e = new ConnectorAuditEvent.MtmvRefreshAuditEvent(
                "ctl", 1L, Optional.empty(), "db", "t", Optional.empty(), 0L);
        PluginAuditEventBridge.bridge(e);
        AuditEvent out = last.get();
        Assertions.assertTrue(out.stmt.contains("partition=*"));
    }

    @Test
    public void bridgePolicyEvalSetsUserAndPolicy() {
        ConnectorAuditEvent e = new ConnectorAuditEvent.PolicyEvalAuditEvent(
                "ctl", 1L, Optional.empty(), "db", "t", "alice", "MASK", false);
        PluginAuditEventBridge.bridge(e);
        AuditEvent out = last.get();
        Assertions.assertEquals(EventType.PLUGIN_POLICY_EVAL, out.type);
        Assertions.assertEquals("alice", out.user);
        Assertions.assertEquals("policy=MASK;applied=false", out.stmt);
    }

    @Test
    public void bridgeSwallowsSinkException() {
        PluginAuditEventBridge.setSink(e -> {
            throw new RuntimeException("downstream dead");
        });
        ConnectorAuditEvent e = new ConnectorAuditEvent.PlanCompletedAuditEvent(
                "ctl", 1L, Optional.empty(), "db", "t", 0L, 0L);
        // Must not propagate
        PluginAuditEventBridge.bridge(e);
    }

    @Test
    public void bridgeNullEventThrows() {
        Assertions.assertThrows(NullPointerException.class,
                () -> PluginAuditEventBridge.bridge(null));
    }

    @Test
    public void setSinkNullRestoresDefault() {
        // Just exercise the setter null path; default sink will then be
        // looked up against Env (which is null in unit tests, dropped silently).
        PluginAuditEventBridge.setSink(null);
        ConnectorAuditEvent e = new ConnectorAuditEvent.PlanCompletedAuditEvent(
                "ctl", 1L, Optional.empty(), "db", "t", 0L, 0L);
        PluginAuditEventBridge.bridge(e);
        // Re-install recording sink for tearDown clean-up; nothing else to assert.
        Consumer<AuditEvent> ignored = ae -> { };
        PluginAuditEventBridge.setSink(ignored);
    }
}
