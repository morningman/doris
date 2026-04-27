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

package org.apache.doris.connector.hive.event;

import org.apache.doris.connector.api.event.ConnectorMetaChangeEvent;
import org.apache.doris.connector.api.event.EventBatch;
import org.apache.doris.connector.api.event.EventCursor;
import org.apache.doris.connector.api.event.EventFilter;
import org.apache.doris.connector.api.event.EventSourceException;
import org.apache.doris.connector.api.event.EventSourceOps;
import org.apache.doris.connector.api.event.TableIdentifier;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsClientException;
import org.apache.doris.connector.hms.HmsNotificationEvent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * {@link EventSourceOps} implementation for Hive (HMS) catalogs.
 *
 * <p>Pulls notification events from HMS via {@link HmsClient} and
 * translates each one into the SPI-level
 * {@link ConnectorMetaChangeEvent} hierarchy through
 * {@link HiveEventTranslator}. The engine-side
 * {@code ConnectorEventDispatcher} drives this class on a master-only
 * scheduler; this class never spins up a thread or schedule of its own.
 *
 * <p>Cursors are simple monotonic {@code long} eventIds wrapped in
 * {@link HiveEventCursor}; persistence is the engine's responsibility.</p>
 */
public final class HiveEventSourceOps implements EventSourceOps {

    private static final Logger LOG = LogManager.getLogger(HiveEventSourceOps.class);

    /** Hard upper bound on the per-poll batch size, matching legacy HMS behavior. */
    private static final int MAX_BATCH_SIZE = 10_000;

    private final HmsClient hmsClient;
    private final HiveEventTranslator translator;
    private final String catalogName;

    public HiveEventSourceOps(HmsClient hmsClient, String catalogName) {
        this(hmsClient, catalogName, new HiveEventTranslator(catalogName));
    }

    HiveEventSourceOps(HmsClient hmsClient, String catalogName, HiveEventTranslator translator) {
        this.hmsClient = Objects.requireNonNull(hmsClient, "hmsClient");
        this.catalogName = Objects.requireNonNull(catalogName, "catalogName");
        this.translator = Objects.requireNonNull(translator, "translator");
    }

    @Override
    public Optional<EventCursor> initialCursor() {
        try {
            long current = hmsClient.getCurrentNotificationEventId();
            return Optional.of(new HiveEventCursor(current));
        } catch (HmsClientException e) {
            LOG.warn("HiveEventSourceOps.initialCursor failed for catalog {}: {}",
                    catalogName, e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public EventBatch poll(EventCursor cursor, int maxEvents, Duration timeout, EventFilter filter)
            throws EventSourceException {
        Objects.requireNonNull(cursor, "cursor");
        Objects.requireNonNull(filter, "filter");
        if (!(cursor instanceof HiveEventCursor)) {
            throw new EventSourceException(
                    "expected HiveEventCursor, got " + cursor.getClass().getName());
        }
        HiveEventCursor hiveCursor = (HiveEventCursor) cursor;
        int cap = maxEvents <= 0 ? 1 : Math.min(maxEvents, MAX_BATCH_SIZE);

        List<HmsNotificationEvent> raw;
        try {
            raw = hmsClient.getNextNotification(hiveCursor.getEventId(), cap);
        } catch (HmsClientException e) {
            throw new EventSourceException(
                    "HMS getNextNotification failed for catalog " + catalogName, e);
        }
        if (raw == null || raw.isEmpty()) {
            return EventBatch.empty(hiveCursor);
        }

        List<ConnectorMetaChangeEvent> translated = new ArrayList<>(raw.size());
        long lastEventId = hiveCursor.getEventId();
        for (HmsNotificationEvent ne : raw) {
            lastEventId = Math.max(lastEventId, ne.getEventId());
            if (!matchesFilter(filter, ne)) {
                continue;
            }
            translated.addAll(translator.translate(ne));
        }

        EventCursor next = new HiveEventCursor(lastEventId);
        boolean hasMore = raw.size() >= cap;
        if (translated.isEmpty()) {
            return new EventBatch(List.of(), next, hasMore, Optional.empty());
        }
        EventBatch.Builder b = EventBatch.builder().nextCursor(next).hasMore(hasMore);
        for (ConnectorMetaChangeEvent e : translated) {
            b.add(e);
        }
        return b.build();
    }

    private static boolean matchesFilter(EventFilter filter, HmsNotificationEvent event) {
        if (filter.catchAll()) {
            return true;
        }
        String db = event.getDbName();
        if (db != null && filter.databases().contains(db)) {
            return true;
        }
        String tbl = event.getTableName();
        if (db != null && tbl != null) {
            for (TableIdentifier ti : filter.tables()) {
                if (db.equalsIgnoreCase(ti.database()) && tbl.equalsIgnoreCase(ti.table())) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public EventCursor parseCursor(byte[] persisted) {
        Objects.requireNonNull(persisted, "persisted");
        if (persisted.length != Long.BYTES) {
            throw new IllegalArgumentException(
                    "HiveEventCursor blob must be " + Long.BYTES + " bytes, got " + persisted.length);
        }
        return new HiveEventCursor(ByteBuffer.wrap(persisted).getLong());
    }

    @Override
    public byte[] serializeCursor(EventCursor cursor) {
        Objects.requireNonNull(cursor, "cursor");
        if (!(cursor instanceof HiveEventCursor)) {
            throw new IllegalArgumentException(
                    "expected HiveEventCursor, got " + cursor.getClass().getName());
        }
        return ByteBuffer.allocate(Long.BYTES).putLong(((HiveEventCursor) cursor).getEventId()).array();
    }

    @Override
    public boolean isSelfManaged() {
        return false;
    }
}
