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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * {@link Runnable} timeline watcher for the Hudi self-managed event
 * source. Each invocation:
 * <ol>
 *   <li>asks the {@link HudiTableLister} for the current set of Hudi
 *       tables;</li>
 *   <li>emits {@code TableCreated} for newly-discovered tables and
 *       {@code TableDropped} for tables that disappeared;</li>
 *   <li>for every still-present table, asks the
 *       {@link HudiTimelineLister} for the current timeline, diffs
 *       against the per-table state recorded on the previous run, and
 *       calls the publisher once per emitted
 *       {@link ConnectorMetaChangeEvent};</li>
 *   <li>updates the shared {@link HudiEventCursor} (tickId++ + the
 *       per-table latest completed instant timestamp) so
 *       {@link HudiEventSourceOps#initialCursor()} can return it.</li>
 * </ol>
 *
 * <p>The watcher creates <b>no</b> threads itself: it implements
 * {@link Runnable} so M2-12 can wire it to the engine's
 * {@code MasterOnlyScheduler}. It is safe to invoke {@link #run()}
 * concurrently in tests but in production the scheduler runs it
 * sequentially per catalog.
 *
 * <p>Per-table failures (lister throws) are recorded via
 * {@link HudiEventTranslator#listFailed} and the cursor still advances
 * for that table — one bad table cannot block the whole watcher.
 */
final class HudiTimelineWatcher implements Runnable {

    private static final Logger LOG = LogManager.getLogger(HudiTimelineWatcher.class);

    /** Per-table state remembered across {@link #run()} calls. */
    static final class TableState {
        final HudiTableRef ref;
        final Set<String> seenTimestamps;
        Optional<String> lastSchemaHash;
        String lastCompletedTimestamp;

        TableState(HudiTableRef ref) {
            this.ref = ref;
            this.seenTimestamps = new HashSet<>();
            this.lastSchemaHash = Optional.empty();
            this.lastCompletedTimestamp = "";
        }
    }

    /** eventId stride per tick. Hudi rarely produces > 1M events per tick. */
    private static final long EVENT_ID_STRIDE_PER_TICK = 1_000_000L;

    private final String catalogName;
    private final HudiTableLister tableLister;
    private final HudiTimelineLister timelineLister;
    private final HudiEventTranslator translator;
    private final Consumer<ConnectorMetaChangeEvent> publisher;
    private final Map<String, TableState> states = new LinkedHashMap<>();
    private final AtomicReference<HudiEventCursor> cursorRef =
            new AtomicReference<>(HudiEventCursor.empty());

    HudiTimelineWatcher(String catalogName,
                        HudiTableLister tableLister,
                        HudiTimelineLister timelineLister,
                        Consumer<ConnectorMetaChangeEvent> publisher) {
        this.catalogName = Objects.requireNonNull(catalogName, "catalogName");
        this.tableLister = Objects.requireNonNull(tableLister, "tableLister");
        this.timelineLister = Objects.requireNonNull(timelineLister, "timelineLister");
        this.translator = new HudiEventTranslator(catalogName);
        this.publisher = Objects.requireNonNull(publisher, "publisher");
    }

    /** Current cursor (used by {@link HudiEventSourceOps#initialCursor()}). */
    HudiEventCursor currentCursor() {
        return cursorRef.get();
    }

    /** For test introspection. */
    Map<String, TableState> tableStates() {
        return states;
    }

    String catalogName() {
        return catalogName;
    }

    @Override
    public synchronized void run() {
        long nextTick = cursorRef.get().tickId() + 1L;
        long baseEventId = nextTick * EVENT_ID_STRIDE_PER_TICK;
        Instant now = Instant.now();
        int slot = 0;

        List<HudiTableRef> currentRefs;
        try {
            currentRefs = tableLister.listTables();
        } catch (RuntimeException e) {
            LOG.warn("hudi[{}] table discovery failed; skipping tick {}",
                    catalogName, nextTick, e);
            // tickId still advances (cursor is otherwise immutable here).
            cursorRef.set(new HudiEventCursor(nextTick, snapshotLastInstants()));
            return;
        }
        Objects.requireNonNull(currentRefs, "tableLister returned null");

        Set<String> currentKeys = new HashSet<>();
        // discover new tables / refresh refs
        for (HudiTableRef ref : currentRefs) {
            currentKeys.add(ref.key());
            TableState st = states.get(ref.key());
            if (st == null) {
                st = new TableState(ref);
                states.put(ref.key(), st);
                publisher.accept(translator.tableCreated(baseEventId + slot++, now, ref));
            }
        }

        // detect dropped tables
        Set<String> droppedKeys = new HashSet<>(states.keySet());
        droppedKeys.removeAll(currentKeys);
        for (String key : droppedKeys) {
            TableState st = states.remove(key);
            publisher.accept(translator.tableDropped(
                    baseEventId + slot++, now, st.ref.db(), st.ref.table()));
        }

        // diff timeline per surviving table
        for (HudiTableRef ref : currentRefs) {
            TableState st = states.get(ref.key());
            List<HudiInstant> instants;
            try {
                instants = timelineLister.listInstants(ref);
            } catch (RuntimeException e) {
                LOG.warn("hudi[{}] timeline lister failed for {}.{} on tick {}",
                        catalogName, ref.db(), ref.table(), nextTick, e);
                publisher.accept(translator.listFailed(baseEventId + slot++, now, ref, e));
                continue;
            }
            Objects.requireNonNull(instants, "timelineLister returned null");

            // Collect & sort new instants (any state) so seenTimestamps is
            // updated in chronological order.
            List<HudiInstant> sorted = new ArrayList<>(instants);
            Collections.sort(sorted);

            for (HudiInstant inst : sorted) {
                String key = inst.timestamp() + "/" + inst.state().name();
                if (!st.seenTimestamps.add(key)) {
                    continue;
                }
                long eventId = baseEventId + slot;
                List<ConnectorMetaChangeEvent> events =
                        translator.translate(eventId, now, ref, st.lastSchemaHash, inst);
                if (!events.isEmpty()) {
                    slot++;
                }
                for (ConnectorMetaChangeEvent ev : events) {
                    publisher.accept(ev);
                }
                if (inst.state() == HudiInstant.State.COMPLETED) {
                    st.lastCompletedTimestamp = inst.timestamp();
                    if (inst.schemaHash().isPresent()) {
                        st.lastSchemaHash = inst.schemaHash();
                    }
                }
            }
        }

        cursorRef.set(new HudiEventCursor(nextTick, snapshotLastInstants()));
    }

    private Map<String, String> snapshotLastInstants() {
        Map<String, String> out = new TreeMap<>();
        for (Map.Entry<String, TableState> e : states.entrySet()) {
            String last = e.getValue().lastCompletedTimestamp;
            if (last != null && !last.isEmpty()) {
                out.put(e.getKey(), last);
            }
        }
        return out;
    }
}
