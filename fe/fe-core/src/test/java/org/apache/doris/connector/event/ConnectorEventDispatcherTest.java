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

import org.apache.doris.connector.DefaultConnectorContext;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.cache.CacheLoader;
import org.apache.doris.connector.api.cache.ConnectorCacheSpec;
import org.apache.doris.connector.api.cache.ConnectorMetaCacheBinding;
import org.apache.doris.connector.api.cache.ConnectorMetaCacheInvalidation;
import org.apache.doris.connector.api.cache.InvalidateRequest;
import org.apache.doris.connector.api.cache.InvalidateScope;
import org.apache.doris.connector.api.cache.MetaCacheHandle;
import org.apache.doris.connector.api.event.ConnectorMetaChangeEvent;
import org.apache.doris.connector.api.event.EventBatch;
import org.apache.doris.connector.api.event.EventCursor;
import org.apache.doris.connector.api.event.EventFilter;
import org.apache.doris.connector.api.event.EventSourceException;
import org.apache.doris.connector.api.event.EventSourceOps;
import org.apache.doris.connector.api.event.PartitionSpec;
import org.apache.doris.datasource.MetaIdMappingsLog;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectorEventDispatcherTest {

    private static final String CTL = "ctl";

    /** Recording invalidation strategy that captures every InvalidateRequest. */
    private static final class RecordingInv implements ConnectorMetaCacheInvalidation {
        final List<InvalidateRequest> seen = new CopyOnWriteArrayList<>();

        @Override
        public boolean appliesTo(InvalidateRequest req) {
            seen.add(req);
            // Always claim applicability so the registry will issue invalidateAll().
            return true;
        }
    }

    private static ConnectorMetaCacheBinding<String, String> testBinding(RecordingInv inv) {
        ConnectorCacheSpec spec = ConnectorCacheSpec.builder()
                .maxSize(16L)
                .ttl(Duration.ofMinutes(1))
                .build();
        CacheLoader<String, String> loader = k -> k;
        return ConnectorMetaCacheBinding.builder("test.entry", String.class, String.class, loader)
                .invalidationStrategy(inv)
                .defaultSpec(spec)
                .build();
    }

    private static DefaultConnectorContext ctxWith(RecordingInv inv) {
        DefaultConnectorContext ctx = new DefaultConnectorContext(CTL, 1L);
        MetaCacheHandle<String, String> h = ctx.getCacheRegistry().bind(testBinding(inv));
        Assertions.assertNotNull(h);
        return ctx;
    }

    private static PluginDrivenExternalCatalog mockCatalog(long id, String name, String type) {
        PluginDrivenExternalCatalog c = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(c.getId()).thenReturn(id);
        Mockito.when(c.getName()).thenReturn(name);
        Mockito.when(c.getType()).thenReturn(type);
        return c;
    }

    private static ConnectorEventDispatcher.EditLogSink recordingSink(List<MetaIdMappingsLog> sink) {
        return log -> sink.add(log);
    }

    private static ConnectorEventDispatcher dispatcherWith(
            ConnectorEventDispatcher.CatalogProvider provider,
            ConnectorEventDispatcher.EditLogSink sink) {
        return new ConnectorEventDispatcher(null, () -> true, provider, sink, 60_000L);
    }

    // ---------- toInvalidateRequest ----------

    @Test
    public void databaseEventToDatabaseScope() {
        ConnectorMetaChangeEvent ev = new ConnectorMetaChangeEvent.DatabaseCreated(
                1L, Instant.now(), CTL, "db", "test");
        InvalidateRequest req = ConnectorEventDispatcher.toInvalidateRequest(ev);
        Assertions.assertEquals(InvalidateScope.DATABASE, req.getScope());
        Assertions.assertEquals(Optional.of("db"), req.getDatabase());
    }

    @Test
    public void tableEventToTableScope() {
        ConnectorMetaChangeEvent ev = new ConnectorMetaChangeEvent.TableCreated(
                2L, Instant.now(), CTL, "db", "tbl", "test");
        InvalidateRequest req = ConnectorEventDispatcher.toInvalidateRequest(ev);
        Assertions.assertEquals(InvalidateScope.TABLE, req.getScope());
        Assertions.assertEquals(Optional.of("db"), req.getDatabase());
        Assertions.assertEquals(Optional.of("tbl"), req.getTable());
    }

    @Test
    public void partitionEventToPartitionScope() {
        LinkedHashMap<String, String> kv = new LinkedHashMap<>();
        kv.put("p", "1");
        kv.put("q", "2");
        PartitionSpec ps = new PartitionSpec(kv);
        ConnectorMetaChangeEvent ev = new ConnectorMetaChangeEvent.PartitionAdded(
                3L, Instant.now(), CTL, "db", "tbl", ps, "test");
        InvalidateRequest req = ConnectorEventDispatcher.toInvalidateRequest(ev);
        Assertions.assertEquals(InvalidateScope.PARTITIONS, req.getScope());
        Assertions.assertEquals(List.of("1", "2"), req.getPartitionKeys());
    }

    @Test
    public void vendorEventWithoutCoordinatesToCatalogScope() {
        ConnectorMetaChangeEvent ev = new ConnectorMetaChangeEvent.VendorEvent(
                4L, Instant.now(), CTL, Optional.empty(), Optional.empty(),
                "vendor", java.util.Map.of(), "test");
        InvalidateRequest req = ConnectorEventDispatcher.toInvalidateRequest(ev);
        Assertions.assertEquals(InvalidateScope.CATALOG, req.getScope());
    }

    @Test
    public void dataChangedToTableScope() {
        ConnectorMetaChangeEvent ev = new ConnectorMetaChangeEvent.DataChanged(
                5L, Instant.now(), CTL, "db", "tbl",
                Optional.empty(), Optional.empty(), "test");
        InvalidateRequest req = ConnectorEventDispatcher.toInvalidateRequest(ev);
        Assertions.assertEquals(InvalidateScope.TABLE, req.getScope());
    }

    // ---------- dispatchEvent ----------

    @Test
    public void dispatchInvokesRegistryAndRecordsLog() {
        RecordingInv inv = new RecordingInv();
        DefaultConnectorContext ctx = ctxWith(inv);
        List<MetaIdMappingsLog> sink = new ArrayList<>();
        PluginDrivenExternalCatalog cat = mockCatalog(7L, "ic", "iceberg");
        ConnectorEventDispatcher d = dispatcherWith(List::of, recordingSink(sink));

        ConnectorMetaChangeEvent ev = new ConnectorMetaChangeEvent.TableAltered(
                42L, Instant.now(), CTL, "db", "tbl", "test");
        d.dispatchEvent(cat, ctx, ev);

        Assertions.assertEquals(1, inv.seen.size());
        Assertions.assertEquals(InvalidateScope.TABLE, inv.seen.get(0).getScope());
        Assertions.assertEquals(1, sink.size());
        MetaIdMappingsLog log = sink.get(0);
        Assertions.assertEquals(7L, log.getCatalogId());
        Assertions.assertEquals(42L, log.getLastSyncedEventId());
        Assertions.assertEquals("iceberg", log.getConnectorType());
        Assertions.assertEquals(42L, d.getLastEventId(7L));
    }

    @Test
    public void dispatchToleratesNullContext() {
        List<MetaIdMappingsLog> sink = new ArrayList<>();
        PluginDrivenExternalCatalog cat = mockCatalog(8L, "n", "paimon");
        ConnectorEventDispatcher d = dispatcherWith(List::of, recordingSink(sink));
        ConnectorMetaChangeEvent ev = new ConnectorMetaChangeEvent.DatabaseDropped(
                9L, Instant.now(), CTL, "db", "test");
        // Must not throw even with no context.
        d.dispatchEvent(cat, null, ev);
        Assertions.assertEquals(1, sink.size());
        Assertions.assertEquals("paimon", sink.get(0).getConnectorType());
    }

    @Test
    public void dispatchSwallowsEditLogFailures() {
        RecordingInv inv = new RecordingInv();
        DefaultConnectorContext ctx = ctxWith(inv);
        PluginDrivenExternalCatalog cat = mockCatalog(10L, "x", "iceberg");
        ConnectorEventDispatcher d = dispatcherWith(List::of, log -> {
            throw new RuntimeException("edit log down");
        });
        ConnectorMetaChangeEvent ev = new ConnectorMetaChangeEvent.TableDropped(
                1L, Instant.now(), CTL, "db", "tbl", "test");
        // Invalidation must still fire even when the edit log throws.
        Assertions.assertDoesNotThrow(() -> d.dispatchEvent(cat, ctx, ev));
        Assertions.assertEquals(1, inv.seen.size());
    }

    // ---------- pollOnce ----------

    /** Minimal in-memory cursor. */
    private record Cur(long pos) implements EventCursor {
        @Override
        public String describe() {
            return "Cur(" + pos + ")";
        }

        @Override
        public int compareTo(EventCursor o) {
            return Long.compare(this.pos, ((Cur) o).pos);
        }
    }

    private static final class FakeOps implements EventSourceOps {
        final List<ConnectorMetaChangeEvent> events;
        final AtomicInteger pollCalls = new AtomicInteger();

        FakeOps(List<ConnectorMetaChangeEvent> events) {
            this.events = events;
        }

        @Override
        public Optional<EventCursor> initialCursor() {
            return Optional.of(new Cur(0));
        }

        @Override
        public EventBatch poll(EventCursor cursor, int max, Duration timeout, EventFilter filter)
                throws EventSourceException {
            pollCalls.incrementAndGet();
            return new EventBatch(events, new Cur(((Cur) cursor).pos + events.size()),
                    false, Optional.empty());
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

    @Test
    public void pollOnceSkipsCatalogsWithNoneOps() {
        RecordingInv inv = new RecordingInv();
        DefaultConnectorContext ctx = ctxWith(inv);
        PluginDrivenExternalCatalog cat = mockCatalog(11L, "n", "iceberg");
        Mockito.when(cat.getConnector()).thenReturn(connectorWith(EventSourceOps.NONE));
        Mockito.when(cat.getConnectorContext()).thenReturn(ctx);
        Mockito.when(cat.buildConnectorSession()).thenReturn(null);

        List<MetaIdMappingsLog> sink = new ArrayList<>();
        ConnectorEventDispatcher d = dispatcherWith(() -> List.of(cat), recordingSink(sink));
        d.pollOnce();
        Assertions.assertTrue(inv.seen.isEmpty());
        Assertions.assertTrue(sink.isEmpty());
    }

    @Test
    public void pollOnceDispatchesPolledEvents() {
        RecordingInv inv = new RecordingInv();
        DefaultConnectorContext ctx = ctxWith(inv);
        FakeOps ops = new FakeOps(List.of(
                new ConnectorMetaChangeEvent.DatabaseCreated(1L, Instant.now(), CTL, "db", "c"),
                new ConnectorMetaChangeEvent.TableCreated(2L, Instant.now(), CTL, "db", "tbl", "c"),
                new ConnectorMetaChangeEvent.PartitionAdded(3L, Instant.now(), CTL, "db", "tbl",
                        new PartitionSpec(new LinkedHashMap<>(java.util.Map.of("p", "v"))), "c")));
        PluginDrivenExternalCatalog cat = mockCatalog(12L, "ic", "iceberg");
        Mockito.when(cat.getConnector()).thenReturn(connectorWith(ops));
        Mockito.when(cat.getConnectorContext()).thenReturn(ctx);
        Mockito.when(cat.buildConnectorSession()).thenReturn(null);

        List<MetaIdMappingsLog> sink = new ArrayList<>();
        ConnectorEventDispatcher d = dispatcherWith(() -> List.of(cat), recordingSink(sink));
        d.pollOnce();

        Assertions.assertEquals(1, ops.pollCalls.get());
        Assertions.assertEquals(3, inv.seen.size());
        Assertions.assertEquals(InvalidateScope.DATABASE, inv.seen.get(0).getScope());
        Assertions.assertEquals(InvalidateScope.TABLE, inv.seen.get(1).getScope());
        Assertions.assertEquals(InvalidateScope.PARTITIONS, inv.seen.get(2).getScope());
        Assertions.assertEquals(3, sink.size());
        Assertions.assertEquals(3L, d.getLastEventId(12L));
        // Cursor advanced to position 3
        Assertions.assertEquals(3L, ((Cur) d.getCursor(12L)).pos());
    }

    @Test
    public void pollOnceSkipsSelfManagedSources() {
        EventSourceOps selfMgr = new EventSourceOps() {
            @Override
            public Optional<EventCursor> initialCursor() {
                return Optional.empty();
            }

            @Override
            public EventBatch poll(EventCursor cursor, int max, Duration timeout, EventFilter filter) {
                throw new AssertionError("self-managed source must not be polled");
            }

            @Override
            public EventCursor parseCursor(byte[] persisted) {
                return null;
            }

            @Override
            public byte[] serializeCursor(EventCursor cursor) {
                return new byte[0];
            }

            @Override
            public boolean isSelfManaged() {
                return true;
            }
        };
        RecordingInv inv = new RecordingInv();
        DefaultConnectorContext ctx = ctxWith(inv);
        PluginDrivenExternalCatalog cat = mockCatalog(13L, "self", "hudi");
        Mockito.when(cat.getConnector()).thenReturn(connectorWith(selfMgr));
        Mockito.when(cat.getConnectorContext()).thenReturn(ctx);
        Mockito.when(cat.buildConnectorSession()).thenReturn(null);

        ConnectorEventDispatcher d = dispatcherWith(() -> List.of(cat), log -> { });
        Assertions.assertDoesNotThrow(d::pollOnce);
        Assertions.assertTrue(inv.seen.isEmpty());
    }

    @Test
    public void pollOnceContinuesPastFailingCatalog() {
        // First catalog throws; second still gets polled.
        PluginDrivenExternalCatalog bad = mockCatalog(14L, "bad", "iceberg");
        Mockito.when(bad.getConnector()).thenThrow(new RuntimeException("init failed"));

        RecordingInv inv = new RecordingInv();
        DefaultConnectorContext ctx = ctxWith(inv);
        FakeOps ops = new FakeOps(List.of(
                new ConnectorMetaChangeEvent.TableDropped(7L, Instant.now(), CTL, "db", "tbl", "c")));
        PluginDrivenExternalCatalog ok = mockCatalog(15L, "ok", "iceberg");
        Mockito.when(ok.getConnector()).thenReturn(connectorWith(ops));
        Mockito.when(ok.getConnectorContext()).thenReturn(ctx);
        Mockito.when(ok.buildConnectorSession()).thenReturn(null);

        ConnectorEventDispatcher d = dispatcherWith(() -> List.of(bad, ok), log -> { });
        d.pollOnce();
        Assertions.assertEquals(1, inv.seen.size());
    }

    @Test
    public void startAndStopAreIdempotent() {
        ConnectorEventDispatcher d = dispatcherWith(List::of, log -> { });
        Assertions.assertFalse(d.isStarted());
        d.start();
        d.start();
        Assertions.assertTrue(d.isStarted());
        d.stop();
        d.stop();
        Assertions.assertFalse(d.isStarted());
    }

    @Test
    public void rejectsBadPollInterval() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new ConnectorEventDispatcher(null, () -> true, List::of, log -> { }, 0L));
    }
}
