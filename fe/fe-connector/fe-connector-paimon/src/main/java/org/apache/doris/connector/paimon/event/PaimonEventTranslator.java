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

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Diffs Paimon catalog state against a previously-seen
 * {@link PaimonEventCursor.TableState} and produces SPI
 * {@link ConnectorMetaChangeEvent}s.
 *
 * <p>Mapping rules:
 * <ul>
 *   <li>Snapshot id changed →
 *       {@link ConnectorMetaChangeEvent.DataChanged} carrying the new
 *       snapshot id; the paimon {@link Snapshot.CommitKind}
 *       {@code APPEND / OVERWRITE} are recognised data-change kinds.
 *       {@code COMPACT / ANALYZE} also flow as {@code DataChanged}
 *       (the engine treats them as table-scope invalidations) but
 *       additionally emit a sibling
 *       {@link ConnectorMetaChangeEvent.VendorEvent} so the
 *       compaction / analyze semantic is not silently dropped.
 *       An unknown / {@code null} commit kind degrades to
 *       {@code DataChanged + VendorEvent} as well.</li>
 *   <li>Schema id changed →
 *       {@link ConnectorMetaChangeEvent.TableAltered}.</li>
 *   <li>Table no longer present (Catalog.TableNotExistException) →
 *       {@link ConnectorMetaChangeEvent.TableDropped}.</li>
 *   <li>New table observed →
 *       {@link ConnectorMetaChangeEvent.TableCreated}.</li>
 *   <li>Database appeared / disappeared →
 *       {@link ConnectorMetaChangeEvent.DatabaseCreated} /
 *       {@link ConnectorMetaChangeEvent.DatabaseDropped}.</li>
 * </ul>
 *
 * <p>Per-partition events are not emitted in this version: paimon
 * partition info lives in the manifest layer and walking it on every
 * poll is too expensive; all data-change events therefore use
 * {@code Optional.empty()} for the partition spec which makes the
 * dispatcher invalidate at table scope.
 */
final class PaimonEventTranslator {

    private final String catalogName;

    PaimonEventTranslator(String catalogName) {
        this.catalogName = Objects.requireNonNull(catalogName, "catalogName");
    }

    /**
     * Compute the events emitted for one table whose previous state was
     * {@code previous} (may be {@code null} for "first time observed")
     * and whose current loaded value is {@code current} (may be
     * {@code null} when the table no longer exists in the catalog).
     */
    List<ConnectorMetaChangeEvent> diffTable(long eventId,
                                             Instant ts,
                                             Identifier id,
                                             PaimonEventCursor.TableState previous,
                                             Table current) {
        Objects.requireNonNull(id, "id");
        String db = id.getDatabaseName().toLowerCase(Locale.ROOT);
        String tbl = id.getObjectName().toLowerCase(Locale.ROOT);

        if (current == null) {
            if (previous == null) {
                return Collections.emptyList();
            }
            return Collections.singletonList(new ConnectorMetaChangeEvent.TableDropped(
                    eventId, ts, catalogName, db, tbl, "paimon.table.dropped"));
        }

        Optional<Snapshot> latest = current.latestSnapshot();
        long snapshotId = latest.map(Snapshot::id).orElse(0L);
        long schemaId = latest.map(Snapshot::schemaId).orElse(0L);
        Snapshot.CommitKind kind = latest.map(Snapshot::commitKind).orElse(null);

        if (previous == null) {
            return Collections.singletonList(new ConnectorMetaChangeEvent.TableCreated(
                    eventId, ts, catalogName, db, tbl, "paimon.table.created"));
        }

        if (previous.snapshotId() == snapshotId && previous.schemaId() == schemaId) {
            return Collections.emptyList();
        }

        List<ConnectorMetaChangeEvent> out = new ArrayList<>(2);
        if (previous.schemaId() != schemaId) {
            out.add(new ConnectorMetaChangeEvent.TableAltered(
                    eventId, ts, catalogName, db, tbl,
                    "paimon.schema.changed:" + previous.schemaId() + "->" + schemaId));
        }
        if (previous.snapshotId() != snapshotId) {
            String kindStr = kind == null ? "unknown" : kind.name().toLowerCase(Locale.ROOT);
            String cause = "paimon.snapshot:" + previous.snapshotId() + "->" + snapshotId
                    + "/kind=" + kindStr;
            out.add(new ConnectorMetaChangeEvent.DataChanged(
                    eventId, ts, catalogName, db, tbl,
                    latest.isPresent() ? Optional.of(snapshotId) : Optional.empty(),
                    Optional.empty(),
                    cause));
            if (needsVendorSibling(kind)) {
                Map<String, String> attrs = new HashMap<>();
                attrs.put("kind", kindStr);
                attrs.put("snapshotId", Long.toString(snapshotId));
                out.add(new ConnectorMetaChangeEvent.VendorEvent(
                        eventId, ts, catalogName,
                        Optional.of(db), Optional.of(tbl),
                        "paimon", attrs,
                        kind == null ? "paimon.unknown.kind" : "paimon.vendor.kind"));
            }
        }
        return out;
    }

    /**
     * APPEND / OVERWRITE are plain data-change semantics; COMPACT / ANALYZE
     * still invalidate caches but carry vendor-specific meaning the
     * engine should be able to observe; an absent / unknown commit kind
     * is treated as vendor-side surprise.
     */
    private static boolean needsVendorSibling(Snapshot.CommitKind kind) {
        if (kind == null) {
            return true;
        }
        return kind == Snapshot.CommitKind.COMPACT || kind == Snapshot.CommitKind.ANALYZE;
    }

    /** Database appeared. */
    ConnectorMetaChangeEvent dbCreated(long eventId, Instant ts, String db) {
        Objects.requireNonNull(db, "db");
        return new ConnectorMetaChangeEvent.DatabaseCreated(
                eventId, ts, catalogName, db.toLowerCase(Locale.ROOT), "paimon.database.created");
    }

    /** Database disappeared. */
    ConnectorMetaChangeEvent dbDropped(long eventId, Instant ts, String db) {
        Objects.requireNonNull(db, "db");
        return new ConnectorMetaChangeEvent.DatabaseDropped(
                eventId, ts, catalogName, db.toLowerCase(Locale.ROOT), "paimon.database.dropped");
    }

    /**
     * Build a vendor fallback event when {@link org.apache.paimon.catalog.Catalog#getTable}
     * fails for a reason other than {@code TableNotExistException} (e.g.
     * transient network failure); keeps the cursor advancing so the
     * table is re-examined on the next tick instead of replaying.
     */
    ConnectorMetaChangeEvent loadFailed(long eventId, Instant ts, Identifier id, Throwable cause) {
        Map<String, String> attrs = new HashMap<>();
        attrs.put("error", cause == null ? "null" : cause.getClass().getSimpleName());
        if (cause != null && cause.getMessage() != null) {
            attrs.put("message", cause.getMessage());
        }
        return new ConnectorMetaChangeEvent.VendorEvent(
                eventId, ts, catalogName,
                Optional.of(id.getDatabaseName().toLowerCase(Locale.ROOT)),
                Optional.of(id.getObjectName().toLowerCase(Locale.ROOT)),
                "paimon", attrs, "paimon.getTable.failed");
    }

    /** Trivial accessor for tests. */
    String catalogName() {
        return catalogName;
    }
}
