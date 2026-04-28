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

package org.apache.doris.connector.event;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.event.ConnectorMetaChangeEvent;
import org.apache.doris.connector.api.event.EventBatch;
import org.apache.doris.connector.api.event.EventCursor;
import org.apache.doris.connector.api.event.EventFilter;
import org.apache.doris.connector.api.event.EventSourceOps;
import org.apache.doris.connector.api.event.SelfManagedEventSource;
import org.apache.doris.datasource.MetaIdMappingsLog;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectorEventDispatcherSelfManagedWireTest {

    private static final class FakeSelfManagedOps implements EventSourceOps, SelfManagedEventSource {
        final AtomicInteger ticks = new AtomicInteger();

        @Override
        public boolean isSelfManaged() {
            return true;
        }

        @Override
        public Optional<EventCursor> initialCursor() {
            return Optional.empty();
        }

        @Override
        public EventBatch poll(EventCursor cursor, int max, Duration timeout, EventFilter filter) {
            throw new AssertionError("must not poll self-managed");
        }

        @Override
        public Runnable getSelfManagedTask() {
            return ticks::incrementAndGet;
        }

        @Override
        public EventCursor parseCursor(byte[] persisted) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] serializeCursor(EventCursor cursor) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class PlainOps implements EventSourceOps {
        @Override
        public Optional<EventCursor> initialCursor() {
            return Optional.empty();
        }

        @Override
        public EventBatch poll(EventCursor cursor, int max, Duration timeout, EventFilter filter) {
            throw new AssertionError("not invoked in this test");
        }

        @Override
        public EventCursor parseCursor(byte[] persisted) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] serializeCursor(EventCursor cursor) {
            throw new UnsupportedOperationException();
        }
    }

    private static ConnectorMetadata metadataWith(EventSourceOps ops) {
        return new ConnectorMetadata() {
            @Override
            public List<String> listDatabaseNames(ConnectorSession s) {
                return List.of();
            }

            @Override
            public EventSourceOps getEventSourceOps() {
                return ops;
            }
        };
    }

    private static Connector connectorWith(EventSourceOps ops) {
        return new Connector() {
            @Override
            public ConnectorMetadata getMetadata(ConnectorSession session) {
                return metadataWith(ops);
            }

            @Override
            public void close() {
            }
        };
    }

    private static PluginDrivenExternalCatalog mockCatalog(long id, String name,
                                                           Connector conn) {
        PluginDrivenExternalCatalog c = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(c.getId()).thenReturn(id);
        Mockito.when(c.getName()).thenReturn(name);
        Mockito.when(c.getType()).thenReturn("hudi");
        Mockito.when(c.getConnector()).thenReturn(conn);
        Mockito.when(c.buildConnectorSession()).thenReturn(null);
        return c;
    }

    private static ConnectorEventDispatcher dispatcherWith(
            ConnectorEventDispatcher.CatalogProvider provider) {
        return new ConnectorEventDispatcher(null, () -> true, provider,
                (ConnectorEventDispatcher.EditLogSink) (MetaIdMappingsLog log) -> { },
                60_000L);
    }

    @Test
    public void attachSchedulesSelfManagedTask() throws Exception {
        FakeSelfManagedOps ops = new FakeSelfManagedOps();
        PluginDrivenExternalCatalog cat = mockCatalog(1L, "h1", connectorWith(ops));
        ConnectorEventDispatcher d = dispatcherWith(() -> List.of(cat));
        try {
            d.attachSelfManaged(cat);
            // schedule ran with min 1s period; wait briefly.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (System.nanoTime() < deadline && ops.ticks.get() == 0) {
                Thread.sleep(50);
            }
            Assertions.assertTrue(ops.ticks.get() >= 1, "task should have ticked at least once");
        } finally {
            d.detachSelfManaged(1L);
            d.stop();
        }
    }

    @Test
    public void detachCancelsTask() {
        FakeSelfManagedOps ops = new FakeSelfManagedOps();
        PluginDrivenExternalCatalog cat = mockCatalog(2L, "h2", connectorWith(ops));
        ConnectorEventDispatcher d = dispatcherWith(() -> List.of(cat));
        try {
            d.attachSelfManaged(cat);
            Assertions.assertTrue(d.detachSelfManaged(2L));
            // Second detach is a no-op.
            Assertions.assertFalse(d.detachSelfManaged(2L));
        } finally {
            d.stop();
        }
    }

    @Test
    public void plainOpsNotScheduled() {
        PluginDrivenExternalCatalog cat = mockCatalog(3L, "h3", connectorWith(new PlainOps()));
        ConnectorEventDispatcher d = dispatcherWith(() -> List.of(cat));
        try {
            d.attachSelfManaged(cat);
            // No task should have been registered.
            Assertions.assertFalse(d.detachSelfManaged(3L));
        } finally {
            d.stop();
        }
    }

    @Test
    public void startSchedulesAllSelfManagedFromProvider() throws Exception {
        FakeSelfManagedOps ops = new FakeSelfManagedOps();
        PluginDrivenExternalCatalog cat = mockCatalog(4L, "h4", connectorWith(ops));
        ConnectorEventDispatcher d = new ConnectorEventDispatcher(null, () -> true,
                () -> List.of(cat),
                (ConnectorEventDispatcher.EditLogSink) (MetaIdMappingsLog log) -> { },
                1_000L);
        try {
            d.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (System.nanoTime() < deadline && ops.ticks.get() == 0) {
                Thread.sleep(50);
            }
            Assertions.assertTrue(ops.ticks.get() >= 1);
        } finally {
            d.stop();
        }
    }

    @Test
    public void dispatchExternalRoutesToCatalogByName() {
        PlainOps ops = new PlainOps();
        PluginDrivenExternalCatalog cat = mockCatalog(5L, "named", connectorWith(ops));
        Mockito.when(cat.getConnectorContext()).thenReturn(null);
        ConnectorEventDispatcher d = dispatcherWith(() -> List.of(cat));
        ConnectorMetaChangeEvent e = new ConnectorMetaChangeEvent.DatabaseCreated(
                42L, java.time.Instant.now(), "named", "db", "test");
        d.dispatchExternal("named", e);
        Assertions.assertEquals(42L, d.getLastEventId(5L));
    }

    @Test
    public void dispatchExternalDropsOnFollower() {
        PlainOps ops = new PlainOps();
        PluginDrivenExternalCatalog cat = mockCatalog(6L, "f", connectorWith(ops));
        ConnectorEventDispatcher d = new ConnectorEventDispatcher(null, () -> false,
                () -> List.of(cat),
                (ConnectorEventDispatcher.EditLogSink) (MetaIdMappingsLog log) -> { },
                60_000L);
        ConnectorMetaChangeEvent e = new ConnectorMetaChangeEvent.DatabaseCreated(
                7L, java.time.Instant.now(), "f", "db", "test");
        d.dispatchExternal("f", e);
        // Last event id should not have been advanced.
        Assertions.assertEquals(-1L, d.getLastEventId(6L));
    }

    @Test
    public void dispatchExternalUnknownCatalogIsLoggedAndDropped() {
        ConnectorEventDispatcher d = dispatcherWith(List::of);
        ConnectorMetaChangeEvent e = new ConnectorMetaChangeEvent.DatabaseCreated(
                7L, java.time.Instant.now(), "missing", "db", "test");
        // Must not throw.
        d.dispatchExternal("missing", e);
    }
}
