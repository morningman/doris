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

package org.apache.doris.connector.audit;

import org.apache.doris.catalog.Env;
import org.apache.doris.connector.api.audit.ConnectorAuditEvent;
import org.apache.doris.connector.api.audit.ConnectorAuditOps.AuditEventKind;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.plugin.AuditEvent.AuditEventBuilder;
import org.apache.doris.plugin.AuditEvent.EventType;
import org.apache.doris.qe.AuditEventProcessor;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * D8 — bridges sealed {@link ConnectorAuditEvent} variants into the
 * fe-core {@link AuditEvent} type and hands them to
 * {@link AuditEventProcessor#handleAuditEvent(AuditEvent)}.
 *
 * <p>Each variant maps to a dedicated {@link EventType} ({@code
 * PLUGIN_PLAN_COMPLETED} / {@code PLUGIN_COMMIT_COMPLETED} /
 * {@code PLUGIN_MTMV_REFRESH} / {@code PLUGIN_POLICY_EVAL}) so audit
 * downstream can filter on connector-origin events without colliding
 * with engine-side events.</p>
 *
 * <p>Failures (downstream NPE, processor not initialized, etc.) are
 * logged and swallowed — audit is best-effort and must not surface
 * back to plugin code.</p>
 */
public final class PluginAuditEventBridge {

    private static final Logger LOG = LogManager.getLogger(PluginAuditEventBridge.class);

    /** Test seam: defaults to the live processor reachable through {@link Env}. */
    private static volatile Consumer<AuditEvent> sink = PluginAuditEventBridge::defaultSink;

    private PluginAuditEventBridge() {
    }

    /**
     * Classify a {@link ConnectorAuditEvent} into the matching
     * {@link AuditEventKind} declared by the connector's
     * {@code ConnectorAuditOps#emittedEventKinds()}.
     */
    public static AuditEventKind kindOf(ConnectorAuditEvent event) {
        Objects.requireNonNull(event, "event");
        if (event instanceof ConnectorAuditEvent.PlanCompletedAuditEvent) {
            return AuditEventKind.PLAN_COMPLETED;
        }
        if (event instanceof ConnectorAuditEvent.CommitCompletedAuditEvent) {
            return AuditEventKind.COMMIT_COMPLETED;
        }
        if (event instanceof ConnectorAuditEvent.MtmvRefreshAuditEvent) {
            return AuditEventKind.MTMV_REFRESH;
        }
        if (event instanceof ConnectorAuditEvent.PolicyEvalAuditEvent) {
            return AuditEventKind.POLICY_EVAL;
        }
        throw new IllegalArgumentException(
                "unknown ConnectorAuditEvent variant: " + event.getClass().getName());
    }

    /**
     * Convert and forward a single audit event. Exceptions are logged and
     * swallowed; the publishing connector observes nothing on failure.
     */
    public static void bridge(ConnectorAuditEvent event) {
        Objects.requireNonNull(event, "event");
        AuditEvent fe;
        try {
            fe = convert(event);
        } catch (Throwable t) {
            LOG.warn("failed to convert ConnectorAuditEvent {} for catalog {}",
                    event.getClass().getSimpleName(), event.catalog(), t);
            return;
        }
        try {
            sink.accept(fe);
        } catch (Throwable t) {
            LOG.warn("AuditEventProcessor rejected plugin audit event {} for catalog {}",
                    fe.type, event.catalog(), t);
        }
    }

    @VisibleForTesting
    static AuditEvent convert(ConnectorAuditEvent event) {
        if (event instanceof ConnectorAuditEvent.PlanCompletedAuditEvent e) {
            return baseBuilder(e)
                    .setEventType(EventType.PLUGIN_PLAN_COMPLETED)
                    .setDb(e.database())
                    .setQueriedTablesAndViews(e.database() + "." + e.table())
                    .setPlanTimesMs(String.valueOf(e.planTimeMillis()))
                    .setScanRows(e.scanRangeCount())
                    .build();
        }
        if (event instanceof ConnectorAuditEvent.CommitCompletedAuditEvent e) {
            return baseBuilder(e)
                    .setEventType(EventType.PLUGIN_COMMIT_COMPLETED)
                    .setDb(e.database())
                    .setQueriedTablesAndViews(e.database() + "." + e.table())
                    .setStmt("commitId=" + e.commitId())
                    .setReturnRows(e.rowsWritten())
                    .build();
        }
        if (event instanceof ConnectorAuditEvent.MtmvRefreshAuditEvent e) {
            return baseBuilder(e)
                    .setEventType(EventType.PLUGIN_MTMV_REFRESH)
                    .setDb(e.database())
                    .setQueriedTablesAndViews(e.database() + "." + e.table())
                    .setStmt("partition=" + e.partitionName().orElse("*")
                            + ";snapshot=" + e.snapshotMarker())
                    .build();
        }
        if (event instanceof ConnectorAuditEvent.PolicyEvalAuditEvent e) {
            return baseBuilder(e)
                    .setEventType(EventType.PLUGIN_POLICY_EVAL)
                    .setDb(e.database())
                    .setUser(e.requestingUser())
                    .setQueriedTablesAndViews(e.database() + "." + e.table())
                    .setStmt("policy=" + e.policyKind() + ";applied=" + e.hintApplied())
                    .build();
        }
        throw new IllegalArgumentException(
                "unknown ConnectorAuditEvent variant: " + event.getClass().getName());
    }

    private static AuditEventBuilder baseBuilder(ConnectorAuditEvent event) {
        return new AuditEventBuilder()
                .setTimestamp(event.eventTimeMillis())
                .setCtl(event.catalog())
                .setQueryId(event.queryId().orElse(""));
    }

    private static void defaultSink(AuditEvent ae) {
        Env env = Env.getCurrentEnv();
        if (env == null) {
            LOG.debug("no current Env; dropping plugin audit event of type {}", ae.type);
            return;
        }
        AuditEventProcessor processor = env.getAuditEventProcessor();
        if (processor == null) {
            LOG.debug("no AuditEventProcessor; dropping plugin audit event of type {}", ae.type);
            return;
        }
        processor.handleAuditEvent(ae);
    }

    /**
     * Test seam — install a custom sink. Always restore the previous
     * value with {@link #setSink} in {@code @AfterEach} to avoid bleeding
     * state across tests.
     */
    @VisibleForTesting
    public static void setSink(Consumer<AuditEvent> override) {
        sink = override == null ? PluginAuditEventBridge::defaultSink : override;
    }
}
