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

package org.apache.doris.connector.iceberg.event;

import org.apache.doris.connector.api.event.ConnectorMetaChangeEvent;
import org.apache.doris.connector.api.event.EventBatch;
import org.apache.doris.connector.api.event.EventCursor;
import org.apache.doris.connector.api.event.EventFilter;
import org.apache.doris.connector.api.event.EventSourceException;
import org.apache.doris.connector.api.event.EventSourceOps;
import org.apache.doris.connector.api.event.TableIdentifier;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * Snapshot-polling {@link EventSourceOps} for Iceberg catalogs.
 *
 * <p>Iceberg has no global notification log; instead, this implementation
 * keeps a {@link IcebergEventCursor} that records {@code (db, table) →
 * (snapshotId, schemaId)} for every table the dispatcher has ever seen.
 * Each {@link #poll(EventCursor, int, Duration, EventFilter)} call:
 * <ol>
 *   <li>bumps {@code tickId} (so cursors stay strictly monotonic, even
 *       when no events are produced — required by the dispatcher);</li>
 *   <li>re-loads each known table via {@link Catalog#loadTable} and
 *       diffs the result against the previous {@code TableState}; the
 *       {@link IcebergEventTranslator} produces SPI events;</li>
 *   <li>every {@code namespaceListInterval} ticks, also re-lists
 *       namespaces and tables to detect newly-created or removed
 *       databases / tables.</li>
 * </ol>
 *
 * <p>The dispatcher's {@code MasterOnlyScheduledExecutor} is the sole
 * driver — this class never starts its own thread or scheduler. All
 * source-side errors are wrapped in {@link EventSourceException} per
 * the contract documented in
 * {@code fe-connector-api/.../EventSourceOps}.</p>
 *
 * <p><b>Filter semantics:</b> when {@code EventFilter} excludes a
 * table, its {@code (snapshotId, schemaId)} is still updated in the
 * next cursor — the change is "consumed" silently rather than
 * re-emitted on every tick. This mirrors hive's
 * {@code HiveEventSourceOps} which advances {@code lastEventId} past
 * filtered events for the same reason.</p>
 */
public final class IcebergEventSourceOps implements EventSourceOps {

    private static final Logger LOG = LogManager.getLogger(IcebergEventSourceOps.class);

    /** Default soft cap on events per poll if the engine passes 0. */
    static final int DEFAULT_MAX_BATCH = 256;

    /** Hard upper bound; matches the legacy hms cap. */
    private static final int MAX_BATCH_SIZE = 10_000;

    /** Stride between full namespace/table re-discovery passes. */
    static final int DEFAULT_NAMESPACE_LIST_INTERVAL = 10;

    /** eventId stride per tick. Multiple events from the same tick fit
     * comfortably under this cap and remain ordered with later ticks. */
    private static final long EVENT_ID_STRIDE_PER_TICK = 1_000_000L;

    private static final byte CURSOR_MAGIC = (byte) 0xCC;
    private static final byte CURSOR_VERSION = 1;

    private final Catalog catalog;
    private final String catalogName;
    private final IcebergEventTranslator translator;
    private final int defaultMaxBatch;
    private final int namespaceListInterval;

    public IcebergEventSourceOps(Catalog catalog, String catalogName) {
        this(catalog, catalogName, DEFAULT_MAX_BATCH, DEFAULT_NAMESPACE_LIST_INTERVAL);
    }

    public IcebergEventSourceOps(Catalog catalog, String catalogName,
                                 int defaultMaxBatch, int namespaceListInterval) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.catalogName = Objects.requireNonNull(catalogName, "catalogName");
        this.translator = new IcebergEventTranslator(catalogName);
        if (defaultMaxBatch <= 0) {
            throw new IllegalArgumentException("defaultMaxBatch must be > 0");
        }
        if (namespaceListInterval <= 0) {
            throw new IllegalArgumentException("namespaceListInterval must be > 0");
        }
        this.defaultMaxBatch = defaultMaxBatch;
        this.namespaceListInterval = namespaceListInterval;
    }

    @Override
    public Optional<EventCursor> initialCursor() {
        try {
            Map<String, IcebergEventCursor.TableState> seed = scanAll();
            return Optional.of(new IcebergEventCursor(0L, seed));
        } catch (RuntimeException e) {
            LOG.warn("IcebergEventSourceOps.initialCursor failed for catalog {}: {}",
                    catalogName, e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public EventBatch poll(EventCursor cursor, int maxEvents, Duration timeout, EventFilter filter)
            throws EventSourceException {
        Objects.requireNonNull(cursor, "cursor");
        Objects.requireNonNull(filter, "filter");
        if (!(cursor instanceof IcebergEventCursor)) {
            throw new EventSourceException(
                    "expected IcebergEventCursor, got " + cursor.getClass().getName());
        }
        IcebergEventCursor previous = (IcebergEventCursor) cursor;
        long nextTick = previous.tickId() + 1;
        long baseEventId = nextTick * EVENT_ID_STRIDE_PER_TICK;
        Instant now = Instant.now();
        int cap = maxEvents <= 0 ? defaultMaxBatch : Math.min(maxEvents, MAX_BATCH_SIZE);

        List<ConnectorMetaChangeEvent> events = new ArrayList<>();
        Map<String, IcebergEventCursor.TableState> nextStates = new LinkedHashMap<>(previous.tables());

        // tableIndex tracks "slot within the same tick" so eventId stays
        // ascending across the batch.  Multiple events for one table
        // share the same slot (allowed: EventBatch invariant is < not <=).
        int tableIndex = 0;

        boolean fullPass = (nextTick % namespaceListInterval) == 0 || nextStates.isEmpty();
        Set<String> currentDbs = null;
        Map<String, IcebergEventCursor.TableState> discovered = null;
        if (fullPass) {
            try {
                discovered = scanAll();
                currentDbs = discoverDbs();
            } catch (RuntimeException e) {
                throw new EventSourceException(
                        "iceberg full discovery failed for catalog " + catalogName, e);
            }

            // namespace-level diffs first
            Set<String> previousDbs = new HashSet<>();
            for (String k : nextStates.keySet()) {
                previousDbs.add(k.substring(0, k.indexOf('\u0001')));
            }
            // CREATE_DB
            for (String db : currentDbs) {
                if (!previousDbs.contains(db)) {
                    long id = baseEventId + tableIndex++;
                    if (!shouldEmit(id, events, cap)) {
                        break;
                    }
                    if (matchesDb(filter, db)) {
                        events.add(translator.dbCreated(id, now, Namespace.of(db)));
                    }
                }
            }
            // DROP_DB (purge table entries for missing dbs)
            for (String db : previousDbs) {
                if (!currentDbs.contains(db)) {
                    long id = baseEventId + tableIndex++;
                    if (!shouldEmit(id, events, cap)) {
                        break;
                    }
                    if (matchesDb(filter, db)) {
                        events.add(translator.dbDropped(id, now, db));
                    }
                    nextStates.keySet().removeIf(k -> k.startsWith(db + "\u0001"));
                }
            }
        }

        // Per-table diffs.  Walk union of "previous tables" + "discovered tables".
        Set<String> tableKeys = new java.util.TreeSet<>(nextStates.keySet());
        if (discovered != null) {
            tableKeys.addAll(discovered.keySet());
        }
        for (String key : tableKeys) {
            if (events.size() >= cap) {
                break;
            }
            String[] parts = key.split("\u0001", 2);
            if (parts.length != 2) {
                continue;
            }
            String db = parts[0];
            String tbl = parts[1];
            org.apache.iceberg.catalog.TableIdentifier id =
                    org.apache.iceberg.catalog.TableIdentifier.of(Namespace.of(db), tbl);
            IcebergEventCursor.TableState prev = nextStates.get(key);
            Table loaded;
            try {
                loaded = catalog.loadTable(id);
            } catch (NoSuchTableException e) {
                loaded = null;
            } catch (RuntimeException e) {
                long evtId = baseEventId + tableIndex++;
                if (matchesTable(filter, db, tbl)) {
                    events.add(translator.loadFailed(evtId, now, id, e));
                }
                continue;
            }

            List<ConnectorMetaChangeEvent> diff = translator.diffTable(
                    baseEventId + tableIndex, now, id, prev, loaded);
            if (!diff.isEmpty()) {
                tableIndex++;
            }

            if (loaded == null) {
                nextStates.remove(key);
            } else {
                Snapshot snap = loaded.currentSnapshot();
                int schemaId = loaded.schema() == null ? 0 : loaded.schema().schemaId();
                long snapId = snap == null ? 0L : snap.snapshotId();
                nextStates.put(key, new IcebergEventCursor.TableState(snapId, schemaId));
            }

            if (!matchesTable(filter, db, tbl)) {
                continue;
            }
            for (ConnectorMetaChangeEvent e : diff) {
                if (events.size() >= cap) {
                    break;
                }
                events.add(e);
            }
        }

        EventCursor next = new IcebergEventCursor(nextTick, nextStates);
        if (events.isEmpty()) {
            return EventBatch.empty(next);
        }
        return new EventBatch(events, next, events.size() >= cap, Optional.empty());
    }

    /** Returns true if there's still space in the batch for one more event. */
    private static boolean shouldEmit(long evtId, List<ConnectorMetaChangeEvent> events, int cap) {
        return events.size() < cap;
    }

    private static boolean matchesDb(EventFilter filter, String db) {
        if (filter.catchAll()) {
            return true;
        }
        if (filter.databases().contains(db)) {
            return true;
        }
        for (TableIdentifier ti : filter.tables()) {
            if (ti.database().equalsIgnoreCase(db)) {
                return true;
            }
        }
        return false;
    }

    private static boolean matchesTable(EventFilter filter, String db, String table) {
        if (filter.catchAll()) {
            return true;
        }
        if (filter.databases().contains(db)) {
            return true;
        }
        for (TableIdentifier ti : filter.tables()) {
            if (ti.database().equalsIgnoreCase(db) && ti.table().equalsIgnoreCase(table)) {
                return true;
            }
        }
        return false;
    }

    private Map<String, IcebergEventCursor.TableState> scanAll() {
        Map<String, IcebergEventCursor.TableState> out = new TreeMap<>();
        Set<String> dbs = discoverDbs();
        for (String db : dbs) {
            List<org.apache.iceberg.catalog.TableIdentifier> ids;
            try {
                ids = catalog.listTables(Namespace.of(db));
            } catch (RuntimeException e) {
                LOG.debug("listTables({}) failed for catalog {}", db, catalogName, e);
                continue;
            }
            if (ids == null) {
                continue;
            }
            for (org.apache.iceberg.catalog.TableIdentifier id : ids) {
                try {
                    Table t = catalog.loadTable(id);
                    Snapshot snap = t.currentSnapshot();
                    int schemaId = t.schema() == null ? 0 : t.schema().schemaId();
                    long snapId = snap == null ? 0L : snap.snapshotId();
                    out.put(IcebergEventCursor.key(db, id.name().toLowerCase(Locale.ROOT)),
                            new IcebergEventCursor.TableState(snapId, schemaId));
                } catch (RuntimeException e) {
                    LOG.debug("loadTable({}) failed during seed for catalog {}", id, catalogName, e);
                }
            }
        }
        return out;
    }

    private Set<String> discoverDbs() {
        if (!(catalog instanceof SupportsNamespaces)) {
            return Collections.emptySet();
        }
        Set<String> dbs = new java.util.TreeSet<>();
        SupportsNamespaces ns = (SupportsNamespaces) catalog;
        for (Namespace n : ns.listNamespaces(Namespace.empty())) {
            dbs.add(n.toString().toLowerCase(Locale.ROOT));
        }
        return dbs;
    }

    @Override
    public EventCursor parseCursor(byte[] persisted) {
        Objects.requireNonNull(persisted, "persisted");
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(persisted))) {
            byte magic = in.readByte();
            byte version = in.readByte();
            if (magic != CURSOR_MAGIC || version != CURSOR_VERSION) {
                throw new IllegalArgumentException(
                        "bad IcebergEventCursor blob magic/version: " + magic + "/" + version);
            }
            long tickId = in.readLong();
            int n = in.readInt();
            if (n < 0) {
                throw new IllegalArgumentException("negative table count in cursor blob: " + n);
            }
            Map<String, IcebergEventCursor.TableState> tables = new TreeMap<>();
            for (int i = 0; i < n; i++) {
                String key = in.readUTF();
                long snapshotId = in.readLong();
                int schemaId = in.readInt();
                tables.put(key, new IcebergEventCursor.TableState(snapshotId, schemaId));
            }
            return new IcebergEventCursor(tickId, tables);
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to parse IcebergEventCursor blob", e);
        }
    }

    @Override
    public byte[] serializeCursor(EventCursor cursor) {
        Objects.requireNonNull(cursor, "cursor");
        if (!(cursor instanceof IcebergEventCursor)) {
            throw new IllegalArgumentException(
                    "expected IcebergEventCursor, got " + cursor.getClass().getName());
        }
        IcebergEventCursor c = (IcebergEventCursor) cursor;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeByte(CURSOR_MAGIC);
            out.writeByte(CURSOR_VERSION);
            out.writeLong(c.tickId());
            out.writeInt(c.tables().size());
            for (Map.Entry<String, IcebergEventCursor.TableState> e : c.tables().entrySet()) {
                out.writeUTF(e.getKey());
                out.writeLong(e.getValue().snapshotId());
                out.writeInt(e.getValue().schemaId());
            }
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("failed to serialize IcebergEventCursor", e);
        }
    }

    @Override
    public boolean isSelfManaged() {
        return false;
    }
}
