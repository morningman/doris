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

package org.apache.doris.connector.hudi.event;

import org.apache.doris.connector.api.event.ConnectorMetaChangeEvent;
import org.apache.doris.connector.api.event.EventBatch;
import org.apache.doris.connector.api.event.EventCursor;
import org.apache.doris.connector.api.event.EventFilter;
import org.apache.doris.connector.api.event.EventSourceException;
import org.apache.doris.connector.api.event.EventSourceOps;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Consumer;

/**
 * Self-managed {@link EventSourceOps} for Hudi catalogs.
 *
 * <p>Hudi has no notification log and no per-table snapshot id that
 * cheaply summarises a commit. Instead, every commit creates a file
 * under the table's {@code .hoodie/} directory (an "instant"). This
 * source therefore runs in <b>self-managed</b> mode
 * ({@link #isSelfManaged()} returns {@code true}): a
 * {@link HudiTimelineWatcher} {@link Runnable} diffs the timeline of
 * every known table on every tick and publishes
 * {@link ConnectorMetaChangeEvent}s through a publisher
 * (typically {@code ConnectorContext::publishExternalEvent}).
 *
 * <p>The dispatcher does not call {@link #poll(EventCursor, int,
 * Duration, EventFilter)} for self-managed sources; this implementation
 * still returns a safe {@link EventBatch#empty(EventCursor)} carrying
 * the latest cursor so any accidental call cannot corrupt state.
 *
 * <p><b>M2-12 wiring</b>: this class deliberately does <i>not</i>
 * create any {@code Thread} / {@code ScheduledExecutorService} of its
 * own. The watcher Runnable is exposed via {@link #getSelfManagedTask()}
 * so M2-12 can attach it to
 * {@link org.apache.doris.connector.api.event.MasterOnlyScheduler}
 * inside the dispatcher; until then the watcher is dormant.
 *
 * <p>Cursor blob magic: {@code 0xCE 0x01}.
 */
public final class HudiEventSourceOps implements EventSourceOps {

    private static final byte CURSOR_MAGIC = (byte) 0xCE;
    private static final byte CURSOR_VERSION = 1;

    private final String catalogName;
    private final HudiTimelineWatcher watcher;

    public HudiEventSourceOps(String catalogName,
                              HudiTableLister tableLister,
                              HudiTimelineLister timelineLister,
                              Consumer<ConnectorMetaChangeEvent> publisher) {
        this.catalogName = Objects.requireNonNull(catalogName, "catalogName");
        this.watcher = new HudiTimelineWatcher(catalogName,
                Objects.requireNonNull(tableLister, "tableLister"),
                Objects.requireNonNull(timelineLister, "timelineLister"),
                Objects.requireNonNull(publisher, "publisher"));
    }

    /**
     * Returns the watcher {@link Runnable}. <b>M2-12</b> will register
     * this with the engine's {@code MasterOnlyScheduler} at a
     * configurable interval. No production code currently invokes it.
     */
    public Runnable getSelfManagedTask() {
        return watcher;
    }

    /** Catalog name (for logs / introspection). */
    public String catalogName() {
        return catalogName;
    }

    @Override
    public boolean isSelfManaged() {
        return true;
    }

    @Override
    public Optional<EventCursor> initialCursor() {
        return Optional.of(watcher.currentCursor());
    }

    /**
     * Self-managed sources are never polled by the dispatcher; this
     * implementation always returns an empty batch carrying the
     * watcher's current cursor so accidental calls are harmless.
     */
    @Override
    public EventBatch poll(EventCursor cursor, int maxEvents, Duration timeout, EventFilter filter)
            throws EventSourceException {
        Objects.requireNonNull(filter, "filter");
        EventCursor next = cursor == null ? watcher.currentCursor() : cursor;
        if (!(next instanceof HudiEventCursor)) {
            throw new EventSourceException(
                    "expected HudiEventCursor, got " + next.getClass().getName());
        }
        return EventBatch.empty(next);
    }

    @Override
    public EventCursor parseCursor(byte[] persisted) {
        Objects.requireNonNull(persisted, "persisted");
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(persisted))) {
            byte magic = in.readByte();
            byte version = in.readByte();
            if (magic != CURSOR_MAGIC || version != CURSOR_VERSION) {
                throw new IllegalArgumentException(
                        "bad HudiEventCursor blob magic/version: " + magic + "/" + version);
            }
            long tickId = in.readLong();
            int n = in.readInt();
            if (n < 0) {
                throw new IllegalArgumentException("negative table count in cursor blob: " + n);
            }
            Map<String, String> tables = new TreeMap<>();
            for (int i = 0; i < n; i++) {
                String key = in.readUTF();
                String lastInstant = in.readUTF();
                tables.put(key, lastInstant);
            }
            return new HudiEventCursor(tickId, tables);
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to parse HudiEventCursor blob", e);
        }
    }

    @Override
    public byte[] serializeCursor(EventCursor cursor) {
        Objects.requireNonNull(cursor, "cursor");
        if (!(cursor instanceof HudiEventCursor)) {
            throw new IllegalArgumentException(
                    "expected HudiEventCursor, got " + cursor.getClass().getName());
        }
        HudiEventCursor c = (HudiEventCursor) cursor;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeByte(CURSOR_MAGIC);
            out.writeByte(CURSOR_VERSION);
            out.writeLong(c.tickId());
            out.writeInt(c.lastInstantPerTable().size());
            for (Map.Entry<String, String> e : c.lastInstantPerTable().entrySet()) {
                out.writeUTF(e.getKey());
                out.writeUTF(e.getValue());
            }
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("failed to serialize HudiEventCursor", e);
        }
    }

    @Override
    public void shutdownSelfManaged(Duration grace) {
        // Self-managed scheduler lives on the dispatcher side (M2-12);
        // this source owns no executor of its own and therefore has
        // nothing to wind down here.
    }
}
