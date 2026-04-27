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

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Translates a single newly-completed {@link HudiInstant} into 0..N
 * {@link ConnectorMetaChangeEvent}s.
 *
 * <p>Mapping rules (see M2-05 task spec):
 * <ul>
 *   <li>{@code commit / deltacommit / replacecommit} →
 *       {@link ConnectorMetaChangeEvent.DataChanged}; if the instant
 *       carries a {@code schemaHash} that differs from the previous
 *       hash for this table, additionally emit
 *       {@link ConnectorMetaChangeEvent.TableAltered}.</li>
 *   <li>{@code clean / rollback / compaction} →
 *       {@link ConnectorMetaChangeEvent.DataChanged} (cache invalidation
 *       at table scope) + sibling
 *       {@link ConnectorMetaChangeEvent.VendorEvent} so the engine can
 *       observe the housekeeping semantic.</li>
 *   <li>{@code savepoint} → {@code VendorEvent} only (no data
 *       movement).</li>
 *   <li>Any other action (Hudi may add new ones) →
 *       {@code VendorEvent} only.</li>
 *   <li>Instant {@code state != COMPLETED} → no events; the watcher
 *       waits until the instant transitions to {@code COMPLETED}.</li>
 * </ul>
 *
 * <p>All events for the same instant share an {@code eventId} (allowed
 * by {@code EventBatch} ascending invariant); the caller advances
 * {@code eventId} between instants.
 */
final class HudiEventTranslator {

    private final String catalogName;

    HudiEventTranslator(String catalogName) {
        this.catalogName = Objects.requireNonNull(catalogName, "catalogName");
    }

    /**
     * Translate one newly-observed instant.
     *
     * @param eventId          eventId slot for this instant
     * @param now              event timestamp
     * @param ref              owning table
     * @param previousSchema   schema hash last known to the watcher
     *                         ({@link Optional#empty()} if no prior commit had a hash)
     * @param instant          the new instant
     * @return events to emit (possibly empty)
     */
    List<ConnectorMetaChangeEvent> translate(long eventId,
                                             Instant now,
                                             HudiTableRef ref,
                                             Optional<String> previousSchema,
                                             HudiInstant instant) {
        Objects.requireNonNull(now, "now");
        Objects.requireNonNull(ref, "ref");
        Objects.requireNonNull(previousSchema, "previousSchema");
        Objects.requireNonNull(instant, "instant");
        if (instant.state() != HudiInstant.State.COMPLETED) {
            return List.of();
        }

        String db = ref.db();
        String tbl = ref.table();
        List<ConnectorMetaChangeEvent> out = new ArrayList<>(2);

        switch (instant.action()) {
            case COMMIT:
            case DELTA_COMMIT:
            case REPLACE_COMMIT: {
                if (schemaChanged(previousSchema, instant.schemaHash())) {
                    out.add(new ConnectorMetaChangeEvent.TableAltered(
                            eventId, now, catalogName, db, tbl,
                            "hudi.schema.changed:"
                                    + previousSchema.orElse("<none>") + "->"
                                    + instant.schemaHash().orElse("<none>")));
                }
                out.add(new ConnectorMetaChangeEvent.DataChanged(
                        eventId, now, catalogName, db, tbl,
                        Optional.empty(), Optional.empty(),
                        "hudi." + instant.action().name().toLowerCase(Locale.ROOT)
                                + ":" + instant.timestamp()));
                return out;
            }
            case CLEAN:
            case ROLLBACK:
            case COMPACTION: {
                String kind = instant.action().name().toLowerCase(Locale.ROOT);
                out.add(new ConnectorMetaChangeEvent.DataChanged(
                        eventId, now, catalogName, db, tbl,
                        Optional.empty(), Optional.empty(),
                        "hudi." + kind + ":" + instant.timestamp()));
                Map<String, String> attrs = new HashMap<>();
                attrs.put("kind", kind);
                attrs.put("instant", instant.timestamp());
                attrs.put("rawAction", instant.rawAction());
                out.add(new ConnectorMetaChangeEvent.VendorEvent(
                        eventId, now, catalogName,
                        Optional.of(db), Optional.of(tbl),
                        "hudi", attrs, "hudi.vendor.kind"));
                return out;
            }
            case SAVEPOINT: {
                Map<String, String> attrs = new HashMap<>();
                attrs.put("kind", "savepoint");
                attrs.put("instant", instant.timestamp());
                attrs.put("rawAction", instant.rawAction());
                out.add(new ConnectorMetaChangeEvent.VendorEvent(
                        eventId, now, catalogName,
                        Optional.of(db), Optional.of(tbl),
                        "hudi", attrs, "hudi.savepoint"));
                return out;
            }
            case UNKNOWN:
            default: {
                Map<String, String> attrs = new HashMap<>();
                attrs.put("kind", "unknown");
                attrs.put("instant", instant.timestamp());
                attrs.put("rawAction", instant.rawAction());
                out.add(new ConnectorMetaChangeEvent.VendorEvent(
                        eventId, now, catalogName,
                        Optional.of(db), Optional.of(tbl),
                        "hudi", attrs, "hudi.unknown.action"));
                return out;
            }
        }
    }

    /** Emit {@code TableCreated} when a previously-unknown table appears. */
    ConnectorMetaChangeEvent tableCreated(long eventId, Instant now, HudiTableRef ref) {
        Objects.requireNonNull(ref, "ref");
        return new ConnectorMetaChangeEvent.TableCreated(
                eventId, now, catalogName, ref.db(), ref.table(), "hudi.table.created");
    }

    /** Emit {@code TableDropped} when a previously-known table disappears. */
    ConnectorMetaChangeEvent tableDropped(long eventId, Instant now, String db, String tbl) {
        return new ConnectorMetaChangeEvent.TableDropped(
                eventId, now, catalogName, db, tbl, "hudi.table.dropped");
    }

    /** Wrap an unexpected per-table failure (lister threw) in a vendor event. */
    ConnectorMetaChangeEvent listFailed(long eventId, Instant now, HudiTableRef ref, Throwable cause) {
        Map<String, String> attrs = new HashMap<>();
        attrs.put("error", cause == null ? "null" : cause.getClass().getSimpleName());
        if (cause != null && cause.getMessage() != null) {
            attrs.put("message", cause.getMessage());
        }
        return new ConnectorMetaChangeEvent.VendorEvent(
                eventId, now, catalogName,
                Optional.of(ref.db()), Optional.of(ref.table()),
                "hudi", attrs, "hudi.timeline.listFailed");
    }

    private static boolean schemaChanged(Optional<String> previous, Optional<String> current) {
        if (current.isEmpty()) {
            return false;
        }
        if (previous.isEmpty()) {
            return true;
        }
        return !previous.get().equals(current.get());
    }

    /** For test introspection only. */
    String catalogName() {
        return catalogName;
    }
}
