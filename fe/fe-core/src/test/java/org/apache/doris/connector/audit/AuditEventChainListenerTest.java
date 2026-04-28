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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class AuditEventChainListenerTest {

    private static ConnectorAuditEvent sampleEvent() {
        return new ConnectorAuditEvent.PlanCompletedAuditEvent(
                "ctl", 1L, Optional.empty(), "db", "tbl", 0L, 0L);
    }

    @Test
    public void emptyListenerListIsNoop() {
        AuditEventChainListener l = new AuditEventChainListener();
        Assertions.assertEquals(0, l.listenerCount());
        // must not throw
        l.fireBeforeForward(sampleEvent());
    }

    @Test
    public void registerAddsListener() {
        AuditEventChainListener l = new AuditEventChainListener();
        AtomicInteger seen = new AtomicInteger();
        l.register(e -> seen.incrementAndGet());
        Assertions.assertEquals(1, l.listenerCount());
        l.fireBeforeForward(sampleEvent());
        Assertions.assertEquals(1, seen.get());
    }

    @Test
    public void registerSameListenerOnceOnly() {
        AuditEventChainListener l = new AuditEventChainListener();
        Consumer<ConnectorAuditEvent> c = e -> { };
        l.register(c);
        l.register(c);
        Assertions.assertEquals(1, l.listenerCount());
    }

    @Test
    public void unregisterRemovesListener() {
        AuditEventChainListener l = new AuditEventChainListener();
        Consumer<ConnectorAuditEvent> c = e -> { };
        l.register(c);
        l.unregister(c);
        Assertions.assertEquals(0, l.listenerCount());
    }

    @Test
    public void multipleListenersAllReceiveEvent() {
        AuditEventChainListener l = new AuditEventChainListener();
        AtomicInteger a = new AtomicInteger();
        AtomicInteger b = new AtomicInteger();
        l.register(e -> a.incrementAndGet());
        l.register(e -> b.incrementAndGet());
        l.fireBeforeForward(sampleEvent());
        Assertions.assertEquals(1, a.get());
        Assertions.assertEquals(1, b.get());
    }

    @Test
    public void listenerExceptionIsIsolated() {
        AuditEventChainListener l = new AuditEventChainListener();
        AtomicInteger b = new AtomicInteger();
        l.register(e -> {
            throw new RuntimeException("boom");
        });
        l.register(e -> b.incrementAndGet());
        // Must not propagate; sibling still runs.
        l.fireBeforeForward(sampleEvent());
        Assertions.assertEquals(1, b.get());
    }

    @Test
    public void registerNullThrows() {
        AuditEventChainListener l = new AuditEventChainListener();
        Assertions.assertThrows(NullPointerException.class, () -> l.register(null));
    }

    @Test
    public void fireNullEventThrows() {
        AuditEventChainListener l = new AuditEventChainListener();
        Assertions.assertThrows(NullPointerException.class, () -> l.fireBeforeForward(null));
    }

    @Test
    public void singletonInstanceAccessible() {
        Assertions.assertNotNull(AuditEventChainListener.INSTANCE);
        // singleton reachable; do not register on it from tests so other tests
        // are not polluted.
    }
}
