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

package org.apache.doris.connector;

import org.apache.doris.connector.api.audit.ConnectorAuditEvent;
import org.apache.doris.connector.api.audit.ConnectorAuditOps.AuditEventKind;
import org.apache.doris.connector.api.event.ConnectorMetaChangeEvent;
import org.apache.doris.connector.audit.AuditEventChainListener;
import org.apache.doris.connector.audit.PluginAuditEventBridge;
import org.apache.doris.plugin.AuditEvent;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class DefaultConnectorContextAuditPublishTest {

    private static final String CTL = "ctl";

    private DefaultConnectorContext.EngineHooks savedHooks;
    private AtomicInteger sinkCalls;
    private AtomicReference<AuditEvent> sinkLast;

    @BeforeEach
    public void setUp() {
        savedHooks = DefaultConnectorContext.getEngineHooks();
        sinkCalls = new AtomicInteger();
        sinkLast = new AtomicReference<>();
        PluginAuditEventBridge.setSink(ae -> {
            sinkCalls.incrementAndGet();
            sinkLast.set(ae);
        });
    }

    @AfterEach
    public void tearDown() {
        DefaultConnectorContext.setEngineHooks(savedHooks);
        PluginAuditEventBridge.setSink(null);
    }

    private static DefaultConnectorContext newCtx() {
        return new DefaultConnectorContext(CTL, 1L);
    }

    private static ConnectorAuditEvent planEvent(String catalog) {
        return new ConnectorAuditEvent.PlanCompletedAuditEvent(
                catalog, 1L, Optional.of("q"), "db", "tbl", 1L, 1L);
    }

    private static void installHooks(DefaultConnectorContext.AuditDeclaration decl) {
        DefaultConnectorContext.setEngineHooks(new DefaultConnectorContext.EngineHooks() {
            @Override
            public DefaultConnectorContext.AuditDeclaration probeAudit(String catalogName) {
                return decl;
            }

            @Override
            public void dispatchExternal(String catalogName, ConnectorMetaChangeEvent event) {
                throw new UnsupportedOperationException();
            }
        });
    }

    @Test
    public void forwardsWhenCapabilityAndKindMatch() {
        installHooks(new DefaultConnectorContext.AuditDeclaration(true, Set.of(AuditEventKind.PLAN_COMPLETED)));
        newCtx().publishAuditEvent(planEvent(CTL));
        Assertions.assertEquals(1, sinkCalls.get());
        Assertions.assertNotNull(sinkLast.get());
    }

    @Test
    public void droppedWhenCapabilityMissing() {
        installHooks(new DefaultConnectorContext.AuditDeclaration(false, Set.of(AuditEventKind.PLAN_COMPLETED)));
        newCtx().publishAuditEvent(planEvent(CTL));
        Assertions.assertEquals(0, sinkCalls.get());
    }

    @Test
    public void droppedWhenKindNotInDeclaredSet() {
        installHooks(new DefaultConnectorContext.AuditDeclaration(true, Set.of(AuditEventKind.COMMIT_COMPLETED)));
        newCtx().publishAuditEvent(planEvent(CTL));
        Assertions.assertEquals(0, sinkCalls.get());
    }

    @Test
    public void droppedWhenForeignCatalog() {
        installHooks(new DefaultConnectorContext.AuditDeclaration(true, Set.of(AuditEventKind.PLAN_COMPLETED)));
        newCtx().publishAuditEvent(planEvent("OTHER"));
        Assertions.assertEquals(0, sinkCalls.get());
    }

    @Test
    public void droppedWhenEmptyDeclaredSet() {
        installHooks(new DefaultConnectorContext.AuditDeclaration(true, Set.of()));
        newCtx().publishAuditEvent(planEvent(CTL));
        Assertions.assertEquals(0, sinkCalls.get());
    }

    @Test
    public void listenerFiredBeforeBridge() {
        installHooks(new DefaultConnectorContext.AuditDeclaration(true, Set.of(AuditEventKind.PLAN_COMPLETED)));
        AtomicInteger order = new AtomicInteger();
        AtomicInteger listenerSlot = new AtomicInteger(-1);
        AtomicInteger sinkSlot = new AtomicInteger(-1);
        Consumer<ConnectorAuditEvent> listener = e -> listenerSlot.set(order.getAndIncrement());
        AuditEventChainListener.INSTANCE.register(listener);
        try {
            PluginAuditEventBridge.setSink(ae -> {
                sinkSlot.set(order.getAndIncrement());
                sinkCalls.incrementAndGet();
            });
            newCtx().publishAuditEvent(planEvent(CTL));
            Assertions.assertEquals(0, listenerSlot.get(), "listener fires first");
            Assertions.assertEquals(1, sinkSlot.get(), "sink fires second");
        } finally {
            AuditEventChainListener.INSTANCE.unregister(listener);
        }
    }

    @Test
    public void exceptionInProbeIsIsolated() {
        DefaultConnectorContext.setEngineHooks(new DefaultConnectorContext.EngineHooks() {
            @Override
            public DefaultConnectorContext.AuditDeclaration probeAudit(String catalogName) {
                throw new RuntimeException("probe broken");
            }

            @Override
            public void dispatchExternal(String catalogName, ConnectorMetaChangeEvent event) {
                throw new UnsupportedOperationException();
            }
        });
        // Must not propagate.
        newCtx().publishAuditEvent(planEvent(CTL));
        Assertions.assertEquals(0, sinkCalls.get());
    }

    @Test
    public void nullEventThrows() {
        installHooks(new DefaultConnectorContext.AuditDeclaration(true, Set.of(AuditEventKind.PLAN_COMPLETED)));
        Assertions.assertThrows(NullPointerException.class,
                () -> newCtx().publishAuditEvent(null));
    }
}
