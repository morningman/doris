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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.connector.DefaultConnectorContext;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableId;
import org.apache.doris.connector.api.cache.InvalidateRequest;
import org.apache.doris.connector.api.event.ConnectorMetaChangeEvent;
import org.apache.doris.connector.api.event.EventBatch;
import org.apache.doris.connector.api.event.EventCursor;
import org.apache.doris.connector.api.event.EventFilter;
import org.apache.doris.connector.api.event.EventSourceException;
import org.apache.doris.connector.api.event.EventSourceOps;
import org.apache.doris.connector.api.event.SelfManagedEventSource;
import org.apache.doris.connector.cache.ConnectorMetaCacheRegistry;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.MetaIdMappingsLog;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.hive.event.MetastoreEventsProcessor;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * fe-core engine-side dispatcher that drives every {@link EventSourceOps}
 * exposed by a {@link PluginDrivenExternalCatalog} and routes the resulting
 * {@link ConnectorMetaChangeEvent}s to the catalog's
 * {@link ConnectorMetaCacheRegistry} for binding-level invalidation.
 *
 * <p>One singleton per FE; reachable via
 * {@code Env.getCurrentEnv().getConnectorEventDispatcher()}. Polling is
 * delegated to {@link MasterOnlyScheduledExecutor} so that follower FEs do
 * not duplicate the work — the schedule stays armed but each tick is a
 * no-op until the FE wins the master election.</p>
 *
 * <p>The legacy {@link MetastoreEventsProcessor} (still serving the
 * {@code HMSExternalCatalog} HMS-NotificationEvent flow) is owned and
 * started by this dispatcher. The two paths are partitioned by catalog
 * class — legacy only iterates {@code HMSExternalCatalog}, this
 * dispatcher only iterates {@link PluginDrivenExternalCatalog} — so
 * once a hive catalog adopts the plugin SPI (M3-Hive) it stops being
 * picked up by legacy automatically. M2-02 ships the hive
 * {@code EventSourceOps} implementation; the legacy processor remains
 * as fallback for catalogs not yet migrated.</p>
 */
public final class ConnectorEventDispatcher {

    private static final Logger LOG = LogManager.getLogger(ConnectorEventDispatcher.class);

    private static final String POLL_TASK_NAME = "connector-event-poll";
    private static final String SELF_MANAGED_TASK_PREFIX = "connector-self-managed-";
    private static final int DEFAULT_POLL_BATCH = 256;
    private static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofSeconds(1);

    private final MasterOnlyScheduledExecutor executor;
    private final MetastoreEventsProcessor legacyHmsProcessor;
    private final CatalogProvider catalogProvider;
    private final EditLogSink editLogSink;
    private final long pollIntervalMs;
    private final BooleanSupplier isMaster;

    private final ConcurrentHashMap<Long, EventCursor> cursorByCatalogId = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Long> lastEventIdByCatalogId = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, String> selfManagedTaskByCatalogId = new ConcurrentHashMap<>();

    private volatile boolean started;

    public ConnectorEventDispatcher() {
        this(new MetastoreEventsProcessor(),
                () -> Env.getCurrentEnv() != null && Env.getCurrentEnv().isMaster(),
                defaultCatalogProvider(),
                defaultEditLogSink(),
                Math.max(1000L, Config.hms_events_polling_interval_ms));
    }

    @VisibleForTesting
    ConnectorEventDispatcher(MetastoreEventsProcessor legacyHmsProcessor,
                             BooleanSupplier isMaster,
                             CatalogProvider catalogProvider,
                             EditLogSink editLogSink,
                             long pollIntervalMs) {
        this.legacyHmsProcessor = legacyHmsProcessor;
        this.catalogProvider = Objects.requireNonNull(catalogProvider, "catalogProvider");
        this.editLogSink = Objects.requireNonNull(editLogSink, "editLogSink");
        if (pollIntervalMs <= 0) {
            throw new IllegalArgumentException("pollIntervalMs must be positive");
        }
        this.pollIntervalMs = pollIntervalMs;
        this.isMaster = Objects.requireNonNull(isMaster, "isMaster");
        this.executor = new MasterOnlyScheduledExecutor(1, "connector-event-dispatcher", isMaster);
    }

    /**
     * Wire the master-only scheduler. Idempotent; subsequent invocations are
     * no-ops. Must be called from the FE master-transfer path.
     */
    public synchronized void start() {
        if (started) {
            return;
        }
        if (legacyHmsProcessor != null) {
            try {
                legacyHmsProcessor.start();
            } catch (Throwable t) {
                LOG.warn("legacy MetastoreEventsProcessor failed to start; continuing with plugin path only", t);
            }
        }
        executor.scheduleAtFixedRate(POLL_TASK_NAME, this::pollOnce, pollIntervalMs, pollIntervalMs);
        scheduleSelfManagedSources();
        started = true;
        LOG.info("ConnectorEventDispatcher started, pollIntervalMs={}", pollIntervalMs);
    }

    public synchronized void stop() {
        if (!started) {
            return;
        }
        executor.shutdown();
        started = false;
        LOG.info("ConnectorEventDispatcher stopped");
    }

    /** Returns the legacy hive processor for backward-compat callers. */
    public MetastoreEventsProcessor getLegacyHmsProcessor() {
        return legacyHmsProcessor;
    }

    public boolean isStarted() {
        return started;
    }

    public long getPollIntervalMs() {
        return pollIntervalMs;
    }

    /**
     * Single poll iteration: walks every catalog returned by
     * {@link CatalogProvider}, asks for its {@link EventSourceOps}, and if
     * non-{@code NONE} and non-self-managed, polls + dispatches one batch.
     * Self-managed sources are skipped here (they push via
     * {@code ConnectorContext#publishExternalEvent}).
     */
    @VisibleForTesting
    public void pollOnce() {
        for (PluginDrivenExternalCatalog catalog : catalogProvider.listPluginCatalogs()) {
            try {
                pollOneCatalog(catalog);
            } catch (Throwable t) {
                LOG.warn("dispatcher poll failed on catalog {}", safeName(catalog), t);
            }
        }
    }

    private void pollOneCatalog(PluginDrivenExternalCatalog catalog) {
        Connector connector = catalog.getConnector();
        if (connector == null) {
            return;
        }
        DefaultConnectorContext ctx = catalog.getConnectorContext();
        ConnectorSession session = catalog.buildConnectorSession();
        ConnectorMetadata md = connector.getMetadata(session);
        EventSourceOps ops = md.getEventSourceOps();
        if (ops == null || ops == EventSourceOps.NONE) {
            return;
        }
        if (ops.isSelfManaged()) {
            return;
        }
        EventCursor cursor = cursorByCatalogId.computeIfAbsent(catalog.getId(), id -> {
            Optional<EventCursor> initial = ops.initialCursor();
            return initial.orElse(null);
        });
        if (cursor == null) {
            return;
        }
        EventBatch batch;
        try {
            batch = ops.poll(cursor, DEFAULT_POLL_BATCH, DEFAULT_POLL_TIMEOUT, EventFilter.ALL);
        } catch (EventSourceException e) {
            LOG.warn("EventSourceOps.poll failed on catalog {}", catalog.getName(), e);
            return;
        }
        if (batch == null) {
            return;
        }
        for (ConnectorMetaChangeEvent event : batch.events()) {
            dispatchEvent(catalog, ctx, event);
        }
        cursorByCatalogId.put(catalog.getId(), batch.nextCursor());
    }

    /**
     * Engine-side ingress for a single event. Public so M2-02..05 plugin
     * implementations using {@code isSelfManaged()=true} can publish events
     * via {@code ConnectorContext#publishExternalEvent}, which the runtime
     * routes here.
     */
    public void dispatchEvent(PluginDrivenExternalCatalog catalog,
                              DefaultConnectorContext ctx,
                              ConnectorMetaChangeEvent event) {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(event, "event");
        InvalidateRequest req = toInvalidateRequest(event);
        if (req != null && ctx != null) {
            ConnectorMetaCacheRegistry registry = ctx.getCacheRegistry();
            if (registry != null) {
                registry.invalidate(req);
            }
        }
        recordToEditLog(catalog, event);
        lastEventIdByCatalogId.put(catalog.getId(), event.eventId());
    }

    /**
     * Public ingress used by the {@code DefaultConnectorContext.publishExternalEvent}
     * override. Resolves the catalog by name (so plugins do not need to hold an
     * engine-side reference) and routes through {@link #dispatchEvent}. On a
     * follower FE the call is dropped (logged at DEBUG) — only the master
     * fans out invalidations and writes the edit log.
     */
    public void dispatchExternal(String catalogName, ConnectorMetaChangeEvent event) {
        Objects.requireNonNull(catalogName, "catalogName");
        Objects.requireNonNull(event, "event");
        if (!safeIsMaster()) {
            LOG.debug("dispatcher dropping external event for catalog {} on follower",
                    catalogName);
            return;
        }
        PluginDrivenExternalCatalog catalog = findCatalog(catalogName);
        if (catalog == null) {
            LOG.warn("dispatcher: no plugin-driven catalog named {} for external event",
                    catalogName);
            return;
        }
        try {
            dispatchEvent(catalog, catalog.getConnectorContext(), event);
        } catch (Throwable t) {
            LOG.warn("dispatcher: external event for catalog {} failed", catalogName, t);
        }
    }

    /**
     * Attach (or re-attach) the master-only schedule for a single
     * self-managed catalog. No-op when the catalog's source is not
     * {@link SelfManagedEventSource}. Idempotent within an instance.
     */
    public synchronized void attachSelfManaged(PluginDrivenExternalCatalog catalog) {
        Objects.requireNonNull(catalog, "catalog");
        Connector connector = catalog.getConnector();
        if (connector == null) {
            return;
        }
        ConnectorSession session = catalog.buildConnectorSession();
        ConnectorMetadata md = connector.getMetadata(session);
        EventSourceOps ops = md.getEventSourceOps();
        if (!(ops instanceof SelfManagedEventSource sms)) {
            return;
        }
        Runnable task = sms.getSelfManagedTask();
        Objects.requireNonNull(task, "self-managed task");
        long periodMs = Math.max(1000L,
                Config.connector_self_managed_event_poll_interval_seconds * 1000L);
        String name = SELF_MANAGED_TASK_PREFIX + catalog.getName();
        // Initial delay 0 so the watcher catches up immediately at startup;
        // subsequent ticks are paced by `periodMs`.
        executor.scheduleAtFixedRate(name, task, 0L, periodMs);
        selfManagedTaskByCatalogId.put(catalog.getId(), name);
        LOG.info("ConnectorEventDispatcher scheduled self-managed task {} every {}ms",
                name, periodMs);
    }

    /**
     * Cancel the master-only schedule for a previously {@link #attachSelfManaged
     * attached} catalog. Returns {@code true} if a task was found and cancellation
     * was attempted; {@code false} otherwise.
     */
    public synchronized boolean detachSelfManaged(long catalogId) {
        String name = selfManagedTaskByCatalogId.remove(catalogId);
        if (name == null) {
            return false;
        }
        boolean cancelled = executor.cancel(name);
        LOG.info("ConnectorEventDispatcher detached self-managed task {} (cancelled={})",
                name, cancelled);
        return cancelled;
    }

    private void scheduleSelfManagedSources() {
        for (PluginDrivenExternalCatalog catalog : catalogProvider.listPluginCatalogs()) {
            try {
                attachSelfManaged(catalog);
            } catch (Throwable t) {
                LOG.warn("dispatcher: attachSelfManaged failed for catalog {}",
                        safeName(catalog), t);
            }
        }
    }

    private boolean safeIsMaster() {
        try {
            return isMaster.getAsBoolean();
        } catch (Throwable t) {
            LOG.warn("master predicate threw, treating as non-master", t);
            return false;
        }
    }

    private PluginDrivenExternalCatalog findCatalog(String catalogName) {
        for (PluginDrivenExternalCatalog c : catalogProvider.listPluginCatalogs()) {
            if (catalogName.equals(c.getName())) {
                return c;
            }
        }
        return null;
    }

    /**
     * Translate a {@link ConnectorMetaChangeEvent} to the closest matching
     * {@link InvalidateRequest}. Returns {@code null} when the event does
     * not imply any cache invalidation.
     */
    @VisibleForTesting
    static InvalidateRequest toInvalidateRequest(ConnectorMetaChangeEvent event) {
        Optional<String> db = event.database();
        Optional<String> tbl = event.table();
        if (event instanceof ConnectorMetaChangeEvent.PartitionAdded
                || event instanceof ConnectorMetaChangeEvent.PartitionDropped
                || event instanceof ConnectorMetaChangeEvent.PartitionAltered) {
            if (db.isPresent() && tbl.isPresent()) {
                List<String> keys = event.partitionSpec()
                        .map(p -> List.copyOf(p.values().values()))
                        .orElse(List.of());
                return InvalidateRequest.ofPartitions(ConnectorTableId.of(db.get(), tbl.get()), keys);
            }
        }
        if (event instanceof ConnectorMetaChangeEvent.TableCreated
                || event instanceof ConnectorMetaChangeEvent.TableDropped
                || event instanceof ConnectorMetaChangeEvent.TableAltered
                || event instanceof ConnectorMetaChangeEvent.TableRenamed
                || event instanceof ConnectorMetaChangeEvent.DataChanged
                || event instanceof ConnectorMetaChangeEvent.RefChanged) {
            if (db.isPresent() && tbl.isPresent()) {
                return InvalidateRequest.ofTable(ConnectorTableId.of(db.get(), tbl.get()));
            }
        }
        if (event instanceof ConnectorMetaChangeEvent.DatabaseCreated
                || event instanceof ConnectorMetaChangeEvent.DatabaseDropped
                || event instanceof ConnectorMetaChangeEvent.DatabaseAltered) {
            if (db.isPresent()) {
                return InvalidateRequest.ofDatabase(db.get());
            }
        }
        if (event instanceof ConnectorMetaChangeEvent.VendorEvent) {
            if (db.isPresent() && tbl.isPresent()) {
                return InvalidateRequest.ofTable(ConnectorTableId.of(db.get(), tbl.get()));
            }
            if (db.isPresent()) {
                return InvalidateRequest.ofDatabase(db.get());
            }
            return InvalidateRequest.ofCatalog();
        }
        return null;
    }

    private void recordToEditLog(PluginDrivenExternalCatalog catalog, ConnectorMetaChangeEvent event) {
        MetaIdMappingsLog log = new MetaIdMappingsLog();
        log.setCatalogId(catalog.getId());
        log.setLastSyncedEventId(event.eventId());
        log.setFromHmsEvent(false);
        log.setConnectorType(catalog.getType());
        try {
            editLogSink.write(log);
        } catch (Throwable t) {
            LOG.warn("failed to record MetaIdMappingsLog for catalog {} event {}",
                    catalog.getName(), event.eventId(), t);
        }
    }

    private static String safeName(PluginDrivenExternalCatalog c) {
        return c == null ? "<null>" : c.getName();
    }

    @VisibleForTesting
    EventCursor getCursor(long catalogId) {
        return cursorByCatalogId.get(catalogId);
    }

    @VisibleForTesting
    long getLastEventId(long catalogId) {
        Long v = lastEventIdByCatalogId.get(catalogId);
        return v == null ? -1L : v;
    }

    /** Strategy hook for enumerating live plugin catalogs (testable seam). */
    @FunctionalInterface
    public interface CatalogProvider {
        Iterable<PluginDrivenExternalCatalog> listPluginCatalogs();
    }

    /** Strategy hook for persisting MetaIdMappingsLog (testable seam). */
    @FunctionalInterface
    public interface EditLogSink extends Consumer<MetaIdMappingsLog> {
        @Override
        void accept(MetaIdMappingsLog log);

        default void write(MetaIdMappingsLog log) {
            accept(log);
        }
    }

    private static CatalogProvider defaultCatalogProvider() {
        return () -> {
            Env env = Env.getCurrentEnv();
            if (env == null || env.getCatalogMgr() == null) {
                return List.of();
            }
            CatalogMgr mgr = env.getCatalogMgr();
            List<PluginDrivenExternalCatalog> out = new java.util.ArrayList<>();
            for (Long id : mgr.getCatalogIds()) {
                CatalogIf<?> c = mgr.getCatalog(id);
                if (c instanceof PluginDrivenExternalCatalog) {
                    out.add((PluginDrivenExternalCatalog) c);
                }
            }
            return out;
        };
    }

    private static EditLogSink defaultEditLogSink() {
        return log -> {
            Env env = Env.getCurrentEnv();
            if (env != null && env.getEditLog() != null) {
                env.getEditLog().logMetaIdMappingsLog(log);
            }
        };
    }
}
