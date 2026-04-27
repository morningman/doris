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

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Diffs Iceberg catalog state against a previously-seen
 * {@link IcebergEventCursor.TableState} and produces SPI
 * {@link ConnectorMetaChangeEvent}s.
 *
 * <p>Mapping rules:
 * <ul>
 *   <li>Snapshot id changed →
 *       {@link ConnectorMetaChangeEvent.DataChanged} carrying the new
 *       snapshot id; the iceberg {@code summary().get("operation")}
 *       (append / overwrite / delete / replace) is recorded in
 *       {@code cause}; unknown ops still yield a {@code DataChanged}
 *       (the engine treats it as a generic table-scope invalidation)
 *       plus a sibling
 *       {@link ConnectorMetaChangeEvent.VendorEvent} so nothing is
 *       silently dropped.</li>
 *   <li>Schema id changed →
 *       {@link ConnectorMetaChangeEvent.TableAltered}.</li>
 *   <li>Table no longer present →
 *       {@link ConnectorMetaChangeEvent.TableDropped}.</li>
 *   <li>New table observed → {@link ConnectorMetaChangeEvent.TableCreated}.</li>
 *   <li>Namespace appeared / disappeared →
 *       {@link ConnectorMetaChangeEvent.DatabaseCreated} /
 *       {@link ConnectorMetaChangeEvent.DatabaseDropped}.</li>
 * </ul>
 *
 * <p>Per-partition events are not emitted: iceberg snapshots do not
 * expose a stable enumerable partition list at the metadata layer
 * cheap enough for the polling loop, so all data-change events use
 * {@code Optional.empty()} for the partition spec, which makes the
 * dispatcher invalidate at table scope (per
 * {@code ConnectorEventDispatcher.toInvalidateRequest}).
 */
final class IcebergEventTranslator {

    private static final Logger LOG = LogManager.getLogger(IcebergEventTranslator.class);

    static final String OP_APPEND = "append";
    static final String OP_OVERWRITE = "overwrite";
    static final String OP_DELETE = "delete";
    static final String OP_REPLACE = "replace";

    private static final Set<String> KNOWN_OPS = Set.of(OP_APPEND, OP_OVERWRITE, OP_DELETE, OP_REPLACE);

    private final String catalogName;

    IcebergEventTranslator(String catalogName) {
        this.catalogName = Objects.requireNonNull(catalogName, "catalogName");
    }

    /**
     * Compute the events emitted for one table whose previous state was
     * {@code previous} (may be {@code null} for "first time observed")
     * and whose current loaded value is {@code current} (may be
     * {@code null} if the table no longer exists).
     */
    List<ConnectorMetaChangeEvent> diffTable(long eventId,
                                             Instant ts,
                                             TableIdentifier id,
                                             IcebergEventCursor.TableState previous,
                                             Table current) {
        Objects.requireNonNull(id, "id");
        String db = dbOf(id);
        String tbl = id.name().toLowerCase(Locale.ROOT);

        if (current == null) {
            if (previous == null) {
                return Collections.emptyList();
            }
            return Collections.singletonList(new ConnectorMetaChangeEvent.TableDropped(
                    eventId, ts, catalogName, db, tbl, "iceberg.table.dropped"));
        }

        Snapshot snap = current.currentSnapshot();
        long snapshotId = snap == null ? 0L : snap.snapshotId();
        int schemaId = current.schema() == null ? 0 : current.schema().schemaId();

        if (previous == null) {
            // first time we observe this table → CREATE
            return Collections.singletonList(new ConnectorMetaChangeEvent.TableCreated(
                    eventId, ts, catalogName, db, tbl, "iceberg.table.created"));
        }

        if (previous.snapshotId() == snapshotId && previous.schemaId() == schemaId) {
            return Collections.emptyList();
        }

        List<ConnectorMetaChangeEvent> out = new ArrayList<>(2);
        if (previous.schemaId() != schemaId) {
            out.add(new ConnectorMetaChangeEvent.TableAltered(
                    eventId, ts, catalogName, db, tbl,
                    "iceberg.schema.changed:" + previous.schemaId() + "->" + schemaId));
        }
        if (previous.snapshotId() != snapshotId) {
            String op = snap == null ? null
                    : Optional.ofNullable(snap.operation()).map(s -> s.toLowerCase(Locale.ROOT)).orElse(null);
            String cause = "iceberg.snapshot:" + previous.snapshotId() + "->" + snapshotId
                    + (op == null ? "" : ("/op=" + op));
            out.add(new ConnectorMetaChangeEvent.DataChanged(
                    eventId, ts, catalogName, db, tbl,
                    snap == null ? Optional.empty() : Optional.of(snapshotId),
                    Optional.empty(),
                    cause));
            if (op != null && !KNOWN_OPS.contains(op)) {
                Map<String, String> attrs = new HashMap<>();
                attrs.put("op", op);
                attrs.put("snapshotId", Long.toString(snapshotId));
                out.add(new ConnectorMetaChangeEvent.VendorEvent(
                        eventId, ts, catalogName,
                        Optional.of(db), Optional.of(tbl),
                        "iceberg", attrs, "iceberg.unknown.op"));
            }
        }
        return out;
    }

    /** Database/namespace appeared. */
    ConnectorMetaChangeEvent dbCreated(long eventId, Instant ts, Namespace ns) {
        Objects.requireNonNull(ns, "ns");
        return new ConnectorMetaChangeEvent.DatabaseCreated(
                eventId, ts, catalogName, dbOf(ns), "iceberg.namespace.created");
    }

    /** Database/namespace disappeared. */
    ConnectorMetaChangeEvent dbDropped(long eventId, Instant ts, String db) {
        Objects.requireNonNull(db, "db");
        return new ConnectorMetaChangeEvent.DatabaseDropped(
                eventId, ts, catalogName, db.toLowerCase(Locale.ROOT), "iceberg.namespace.dropped");
    }

    /**
     * Build a vendor fallback event when {@code loadTable} fails for a
     * reason other than NoSuchTable (e.g. transient network); keeps the
     * cursor advancing without losing track of the table.
     */
    ConnectorMetaChangeEvent loadFailed(long eventId, Instant ts, TableIdentifier id, Throwable cause) {
        Map<String, String> attrs = new HashMap<>();
        attrs.put("error", cause == null ? "null" : cause.getClass().getSimpleName());
        if (cause != null && cause.getMessage() != null) {
            attrs.put("message", cause.getMessage());
        }
        return new ConnectorMetaChangeEvent.VendorEvent(
                eventId, ts, catalogName,
                Optional.of(dbOf(id)), Optional.of(id.name().toLowerCase(Locale.ROOT)),
                "iceberg", attrs, "iceberg.loadTable.failed");
    }

    static String dbOf(TableIdentifier id) {
        return id.namespace().toString().toLowerCase(Locale.ROOT);
    }

    static String dbOf(Namespace ns) {
        return ns.toString().toLowerCase(Locale.ROOT);
    }

    /** Trivial accessor for tests / ops introspection. */
    String catalogName() {
        return catalogName;
    }

    @SuppressWarnings("unused")
    private static void quiet(Throwable t) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("translator suppressed", t);
        }
    }
}
