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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * D8 — interception point fired immediately before a {@link ConnectorAuditEvent}
 * is forwarded to {@link PluginAuditEventBridge}.
 *
 * <p>The current SPI design only allows plugins to <i>publish</i> audit events
 * via {@code ConnectorContext.publishAuditEvent}; multi-plugin chaining
 * (D8 §11.3 — plugin A inspects plugin B's audit events) is future work.
 * This listener provides the registration surface so future plugins can
 * subscribe without re-engineering the publish path.</p>
 *
 * <p>Listener invocation is exception-isolated: a misbehaving listener
 * cannot prevent the event from reaching the bridge, nor can it break
 * sibling listeners. The listener registry is process-wide
 * ({@link #INSTANCE}); registrations across catalogs are intentionally
 * shared so a plugin can attach a single listener that observes every
 * catalog's audit traffic.</p>
 */
public final class AuditEventChainListener {

    private static final Logger LOG = LogManager.getLogger(AuditEventChainListener.class);

    public static final AuditEventChainListener INSTANCE = new AuditEventChainListener();

    private final CopyOnWriteArrayList<Consumer<ConnectorAuditEvent>> listeners
            = new CopyOnWriteArrayList<>();

    AuditEventChainListener() {
        // package-private; tests may instantiate fresh listeners to avoid
        // touching the global INSTANCE.
    }

    /** Register a listener. Same listener instance may be registered at
     *  most once; duplicate registration is a no-op. */
    public void register(Consumer<ConnectorAuditEvent> listener) {
        Objects.requireNonNull(listener, "listener");
        listeners.addIfAbsent(listener);
    }

    /** Unregister a previously registered listener. No-op if not present. */
    public void unregister(Consumer<ConnectorAuditEvent> listener) {
        Objects.requireNonNull(listener, "listener");
        listeners.remove(listener);
    }

    /** Number of currently registered listeners. */
    public int listenerCount() {
        return listeners.size();
    }

    /**
     * Invoke every registered listener in registration order. Any listener
     * that throws is logged and skipped; sibling listeners and the
     * downstream bridge are unaffected.
     */
    public void fireBeforeForward(ConnectorAuditEvent event) {
        Objects.requireNonNull(event, "event");
        for (Consumer<ConnectorAuditEvent> l : listeners) {
            try {
                l.accept(event);
            } catch (Throwable t) {
                LOG.warn("audit chain listener {} threw on event {}; isolated",
                        l.getClass().getName(), event.getClass().getSimpleName(), t);
            }
        }
    }
}
