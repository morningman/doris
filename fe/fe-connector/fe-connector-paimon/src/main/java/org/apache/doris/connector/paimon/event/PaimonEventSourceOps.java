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

package org.apache.doris.connector.paimon.event;

import org.apache.doris.connector.api.event.ConnectorMetaChangeEvent;
import org.apache.doris.connector.api.event.EventBatch;
import org.apache.doris.connector.api.event.EventCursor;
import org.apache.doris.connector.api.event.EventFilter;
import org.apache.doris.connector.api.event.EventSourceException;
import org.apache.doris.connector.api.event.EventSourceOps;
import org.apache.doris.connector.api.event.TableIdentifier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Snapshot-polling {@link EventSourceOps} for Paimon catalogs.
 *
 * <p>Paimon emits a monotonically increasing {@code snapshotId} per
 * table on every commit. There is no global notification log, so this
 * implementation keeps a {@link PaimonEventCursor} of every
 * {@code (db, table)} the dispatcher has ever seen and re-loads each
 * table via {@link Catalog#getTable(Identifier)} on every poll.
 *
 * <p>Each {@link #poll(EventCursor, int, Duration, EventFilter)} call:
 * <ol>
 *   <li>bumps {@code tickId} (cursors stay strictly monotonic across
 *       polls — required by the dispatcher's
 *       {@code IcebergEventCursor#compareTo} contract documented in
 *       M2-03);</li>
 *   <li>re-loads each known table via
 *       {@link Catalog#getTable(Identifier)} and diffs the result
 *       against the previous {@link PaimonEventCursor.TableState}; the
 *       {@link PaimonEventTranslator} produces SPI events;</li>
 *   <li>every {@code namespaceListInterval} ticks, also re-lists
 *       databases / tables to detect newly-created or removed
 *       databases / tables.</li>
 * </ol>
 *
 * <p>The dispatcher's {@code MasterOnlyScheduledExecutor} is the sole
 * driver — this class never starts its own thread or scheduler.
 *
 * <p><b>Filter semantics:</b> when {@code EventFilter} excludes a
 * table, its {@code (snapshotId, schemaId)} is still updated in the
 * next cursor — the change is "consumed" silently rather than
 * re-emitted on every tick. This mirrors hive / iceberg behaviour.
 *
 * <p><b>Error semantics:</b> catastrophic discovery failures (full
 * {@link Catalog#listDatabases()} / {@link Catalog#listTables(String)})
 * are wrapped in {@link EventSourceException}; per-table failures
 * downgrade to a {@code VendorEvent} so the cursor still advances and
 * one bad table cannot block the entire batch.
 */
public final class PaimonEventSourceOps implements EventSourceOps {

    private static final Logger LOG = LogManager.getLogger(PaimonEventSourceOps.class);

    /** Default soft cap on events per poll if the engine passes 0. */
    static final int DEFAULT_MAX_BATCH = 256;

    /** Hard upper bound; matches the legacy hms cap. */
    private static final int MAX_BATCH_SIZE = 10_000;

    /** Stride between full namespace/table re-discovery passes. */
    static final int DEFAULT_NAMESPACE_LIST_INTERVAL = 10;

    /** eventId stride per tick. Multiple events from the same tick fit
     * comfortably under this cap and remain ordered with later ticks. */
    private static final long EVENT_ID_STRIDE_PER_TICK = 1_000_000L;

    private static final byte CURSOR_MAGIC = (byte) 0xCD;
    private static final byte CURSOR_VERSION = 1;

    private final Catalog catalog;
    private final String catalogName;
    private final PaimonEventTranslator translator;
    private final int defaultMaxBatch;
    private final int namespaceListInterval;

    public PaimonEventSourceOps(Catalog catalog, String catalogName) {
        this(catalog, catalogName, DEFAULT_MAX_BATCH, DEFAULT_NAMESPACE_LIST_INTERVAL);
    }

    public PaimonEventSourceOps(Catalog catalog, String catalogName,
                                int defaultMaxBatch, int namespaceListInterval) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.catalogName = Objects.requireNonNull(catalogName, "catalogName");
        this.translator = new PaimonEventTranslator(catalogName);
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
            Map<String, PaimonEventCursor.TableState> seed = scanAll();
            return Optional.of(new PaimonEventCursor(0L, seed));
        } catch (RuntimeException e) {
            LOG.warn("PaimonEventSourceOps.initialCursor failed for catalog {}: {}",
                    catalogName, e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public EventBatch poll(EventCursor cursor, int maxEvents, Duration timeout, EventFilter filter)
            throws EventSourceException {
        Objects.requireNonNull(cursor, "cursor");
        Objects.requireNonNull(filter, "filter");
        if (!(cursor instanceof PaimonEventCursor)) {
            throw new EventSourceException(
                    "expected PaimonEventCursor, got " + cursor.getClass().getName());
        }
        PaimonEventCursor previous = (PaimonEventCursor) cursor;
        long nextTick = previous.tickId() + 1;
        long baseEventId = nextTick * EVENT_ID_STRIDE_PER_TICK;
        Instant now = Instant.now();
        int cap = maxEvents <= 0 ? defaultMaxBatch : Math.min(maxEvents, MAX_BATCH_SIZE);

        List<ConnectorMetaChangeEvent> events = new ArrayList<>();
        Map<String, PaimonEventCursor.TableState> nextStates =
                new LinkedHashMap<>(previous.tables());

        // tableIndex tracks the slot within the current tick so eventId
        // stays ascending across the batch. Multiple events for the same
        // table share the same slot (allowed: EventBatch invariant is <).
        int tableIndex = 0;

        boolean fullPass = (nextTick % namespaceListInterval) == 0 || nextStates.isEmpty();
        Set<String> currentDbs = null;
        Map<String, PaimonEventCursor.TableState> discovered = null;
        if (fullPass) {
            try {
                discovered = scanAll();
                currentDbs = discoverDbs();
            } catch (RuntimeException e) {
                throw new EventSourceException(
                        "paimon full discovery failed for catalog " + catalogName, e);
            }

            Set<String> previousDbs = new HashSet<>();
            for (String k : nextStates.keySet()) {
                previousDbs.add(k.substring(0, k.indexOf('\u0001')));
            }
            // CREATE_DB
            for (String db : currentDbs) {
                if (!previousDbs.contains(db)) {
                    long id = baseEventId + tableIndex++;
                    if (events.size() >= cap) {
                        break;
                    }
                    if (matchesDb(filter, db)) {
                        events.add(translator.dbCreated(id, now, db));
                    }
                }
            }
            // DROP_DB (purge table entries belonging to dropped dbs)
            for (String db : previousDbs) {
                if (!currentDbs.contains(db)) {
                    long id = baseEventId + tableIndex++;
                    if (events.size() >= cap) {
                        break;
                    }
                    if (matchesDb(filter, db)) {
                        events.add(translator.dbDropped(id, now, db));
                    }
                    nextStates.keySet().removeIf(k -> k.startsWith(db + "\u0001"));
                }
            }
        }

        // Per-table diffs. Walk union of "previously seen" + "newly discovered".
        Set<String> tableKeys = new TreeSet<>(nextStates.keySet());
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
            Identifier id = Identifier.create(db, tbl);
            PaimonEventCursor.TableState prev = nextStates.get(key);
            Table loaded;
            try {
                loaded = catalog.getTable(id);
            } catch (Catalog.TableNotExistException e) {
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
                Optional<Snapshot> latest = loaded.latestSnapshot();
                long snapId = latest.map(Snapshot::id).orElse(0L);
                long schemaId = latest.map(Snapshot::schemaId).orElse(0L);
                nextStates.put(key, new PaimonEventCursor.TableState(snapId, schemaId));
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

        EventCursor next = new PaimonEventCursor(nextTick, nextStates);
        if (events.isEmpty()) {
            return EventBatch.empty(next);
        }
        return new EventBatch(events, next, events.size() >= cap, Optional.empty());
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

    private Map<String, PaimonEventCursor.TableState> scanAll() {
        Map<String, PaimonEventCursor.TableState> out = new TreeMap<>();
        Set<String> dbs = discoverDbs();
        for (String db : dbs) {
            List<String> tables;
            try {
                tables = catalog.listTables(db);
            } catch (Catalog.DatabaseNotExistException e) {
                continue;
            } catch (RuntimeException e) {
                LOG.debug("listTables({}) failed for catalog {}", db, catalogName, e);
                continue;
            }
            if (tables == null) {
                continue;
            }
            for (String t : tables) {
                Identifier id = Identifier.create(db, t);
                try {
                    Table table = catalog.getTable(id);
                    Optional<Snapshot> latest = table.latestSnapshot();
                    long snapId = latest.map(Snapshot::id).orElse(0L);
                    long schemaId = latest.map(Snapshot::schemaId).orElse(0L);
                    out.put(PaimonEventCursor.key(db, t.toLowerCase(Locale.ROOT)),
                            new PaimonEventCursor.TableState(snapId, schemaId));
                } catch (Catalog.TableNotExistException e) {
                    // race: table dropped between list and getTable; skip.
                } catch (RuntimeException e) {
                    LOG.debug("getTable({}) failed during seed for catalog {}", id, catalogName, e);
                }
            }
        }
        return out;
    }

    private Set<String> discoverDbs() {
        Set<String> dbs = new TreeSet<>();
        for (String d : catalog.listDatabases()) {
            dbs.add(d.toLowerCase(Locale.ROOT));
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
                        "bad PaimonEventCursor blob magic/version: " + magic + "/" + version);
            }
            long tickId = in.readLong();
            int n = in.readInt();
            if (n < 0) {
                throw new IllegalArgumentException("negative table count in cursor blob: " + n);
            }
            Map<String, PaimonEventCursor.TableState> tables = new TreeMap<>();
            for (int i = 0; i < n; i++) {
                String key = in.readUTF();
                long snapshotId = in.readLong();
                long schemaId = in.readLong();
                tables.put(key, new PaimonEventCursor.TableState(snapshotId, schemaId));
            }
            return new PaimonEventCursor(tickId, tables);
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to parse PaimonEventCursor blob", e);
        }
    }

    @Override
    public byte[] serializeCursor(EventCursor cursor) {
        Objects.requireNonNull(cursor, "cursor");
        if (!(cursor instanceof PaimonEventCursor)) {
            throw new IllegalArgumentException(
                    "expected PaimonEventCursor, got " + cursor.getClass().getName());
        }
        PaimonEventCursor c = (PaimonEventCursor) cursor;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeByte(CURSOR_MAGIC);
            out.writeByte(CURSOR_VERSION);
            out.writeLong(c.tickId());
            out.writeInt(c.tables().size());
            for (Map.Entry<String, PaimonEventCursor.TableState> e : c.tables().entrySet()) {
                out.writeUTF(e.getKey());
                out.writeLong(e.getValue().snapshotId());
                out.writeLong(e.getValue().schemaId());
            }
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("failed to serialize PaimonEventCursor", e);
        }
    }

    @Override
    public boolean isSelfManaged() {
        return false;
    }
}
