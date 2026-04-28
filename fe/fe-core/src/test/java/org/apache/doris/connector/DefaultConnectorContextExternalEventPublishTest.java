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

import org.apache.doris.connector.api.event.ConnectorMetaChangeEvent;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class DefaultConnectorContextExternalEventPublishTest {

    private static final String CTL = "ctl";

    private DefaultConnectorContext.EngineHooks saved;

    @BeforeEach
    public void setUp() {
        saved = DefaultConnectorContext.getEngineHooks();
    }

    @AfterEach
    public void tearDown() {
        DefaultConnectorContext.setEngineHooks(saved);
    }

    private static ConnectorMetaChangeEvent dbCreated() {
        return new ConnectorMetaChangeEvent.DatabaseCreated(
                1L, Instant.ofEpochMilli(0L), CTL, "db", "test");
    }

    @Test
    public void forwardsToDispatcher() {
        AtomicInteger calls = new AtomicInteger();
        AtomicReference<String> seenName = new AtomicReference<>();
        DefaultConnectorContext.setEngineHooks(new DefaultConnectorContext.EngineHooks() {
            @Override
            public DefaultConnectorContext.AuditDeclaration probeAudit(String c) {
                return DefaultConnectorContext.AuditDeclaration.empty();
            }

            @Override
            public void dispatchExternal(String c, ConnectorMetaChangeEvent e) {
                calls.incrementAndGet();
                seenName.set(c);
            }
        });
        new DefaultConnectorContext(CTL, 1L).publishExternalEvent(dbCreated());
        Assertions.assertEquals(1, calls.get());
        Assertions.assertEquals(CTL, seenName.get());
    }

    @Test
    public void exceptionInDispatcherSwallowed() {
        DefaultConnectorContext.setEngineHooks(new DefaultConnectorContext.EngineHooks() {
            @Override
            public DefaultConnectorContext.AuditDeclaration probeAudit(String c) {
                return new DefaultConnectorContext.AuditDeclaration(false, Set.of());
            }

            @Override
            public void dispatchExternal(String c, ConnectorMetaChangeEvent e) {
                throw new RuntimeException("dispatcher broken");
            }
        });
        // Must not propagate
        new DefaultConnectorContext(CTL, 1L).publishExternalEvent(dbCreated());
    }

    @Test
    public void nullEventThrows() {
        DefaultConnectorContext.setEngineHooks(new DefaultConnectorContext.EngineHooks() {
            @Override
            public DefaultConnectorContext.AuditDeclaration probeAudit(String c) {
                return DefaultConnectorContext.AuditDeclaration.empty();
            }

            @Override
            public void dispatchExternal(String c, ConnectorMetaChangeEvent e) {
                // ignored
            }
        });
        Assertions.assertThrows(NullPointerException.class,
                () -> new DefaultConnectorContext(CTL, 1L).publishExternalEvent(null));
    }

    @Test
    public void hooksReceivesCatalogName() {
        AtomicReference<String> seen = new AtomicReference<>();
        DefaultConnectorContext.setEngineHooks(new DefaultConnectorContext.EngineHooks() {
            @Override
            public DefaultConnectorContext.AuditDeclaration probeAudit(String c) {
                return DefaultConnectorContext.AuditDeclaration.empty();
            }

            @Override
            public void dispatchExternal(String c, ConnectorMetaChangeEvent e) {
                seen.set(c);
            }
        });
        new DefaultConnectorContext("alpha", 7L).publishExternalEvent(dbCreated());
        Assertions.assertEquals("alpha", seen.get());
    }

    @Test
    public void defaultHooksIsNonNull() {
        // Sanity: when the static holder is reset to default, it must not be null.
        DefaultConnectorContext.setEngineHooks(null);
        Assertions.assertNotNull(DefaultConnectorContext.getEngineHooks());
    }
}
