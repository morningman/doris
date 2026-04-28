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

package org.apache.doris.connector.hive.audit;

import org.apache.doris.connector.api.ConnectorHttpSecurityHook;
import org.apache.doris.connector.api.audit.ConnectorAuditEvent;
import org.apache.doris.connector.api.audit.ConnectorAuditEvent.PlanCompletedAuditEvent;
import org.apache.doris.connector.api.scan.ConnectorScanRequest;
import org.apache.doris.connector.hive.HiveScanPlanProvider;
import org.apache.doris.connector.hive.HiveTableHandle;
import org.apache.doris.connector.hive.HiveTableType;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.spi.ConnectorContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class HiveScanPlanProviderAuditTest {

    private static final String CATALOG = "hive_cat";
    private static final String DB = "db1";
    private static final String TBL = "t1";

    /** Recording context that captures publishAuditEvent calls. */
    private static final class RecordingContext implements ConnectorContext {
        private final String catalog;
        final List<ConnectorAuditEvent> events = new ArrayList<>();
        Throwable publishFailure;

        RecordingContext(String catalog) {
            this.catalog = catalog;
        }

        @Override
        public String getCatalogName() {
            return catalog;
        }

        @Override
        public long getCatalogId() {
            return 1L;
        }

        @Override
        public ConnectorHttpSecurityHook getHttpSecurityHook() {
            return ConnectorHttpSecurityHook.NOOP;
        }

        @Override
        public void publishAuditEvent(ConnectorAuditEvent event) {
            if (publishFailure != null) {
                if (publishFailure instanceof RuntimeException) {
                    throw (RuntimeException) publishFailure;
                }
                throw new RuntimeException(publishFailure);
            }
            events.add(event);
        }
    }

    /** Build a partitioned handle whose pruned partitions list is empty. */
    private HiveTableHandle emptyPartitionsHandle() {
        return new HiveTableHandle.Builder(DB, TBL, HiveTableType.HIVE)
                .partitionKeyNames(Collections.singletonList("dt"))
                .prunedPartitions(Collections.emptyList())
                .build();
    }

    @Test
    void successfulPlanScanPublishesExactlyOnePlanCompletedEvent() {
        HmsClient client = Mockito.mock(HmsClient.class);
        RecordingContext ctx = new RecordingContext(CATALOG);
        HiveScanPlanProvider provider = new HiveScanPlanProvider(client, new HashMap<>(), ctx);

        provider.planScan(ConnectorScanRequest.from(null, emptyPartitionsHandle(), Collections.emptyList(), Optional.empty()));

        Assertions.assertEquals(1, ctx.events.size());
        Assertions.assertTrue(ctx.events.get(0) instanceof PlanCompletedAuditEvent);
    }

    @Test
    void publishedEventCarriesCatalogDbTableAndZeroScanRanges() {
        HmsClient client = Mockito.mock(HmsClient.class);
        RecordingContext ctx = new RecordingContext(CATALOG);
        HiveScanPlanProvider provider = new HiveScanPlanProvider(client, new HashMap<>(), ctx);

        provider.planScan(ConnectorScanRequest.from(null, emptyPartitionsHandle(), Collections.emptyList(), Optional.empty()));

        PlanCompletedAuditEvent ev = (PlanCompletedAuditEvent) ctx.events.get(0);
        Assertions.assertEquals(CATALOG, ev.catalog());
        Assertions.assertEquals(DB, ev.database());
        Assertions.assertEquals(TBL, ev.table());
        Assertions.assertEquals(0L, ev.scanRangeCount());
        Assertions.assertEquals(Optional.empty(), ev.queryId());
        Assertions.assertTrue(ev.eventTimeMillis() > 0L);
        Assertions.assertTrue(ev.planTimeMillis() >= 0L);
    }

    @Test
    void planScanReturningEmptyRangesStillPublishes() {
        HmsClient client = Mockito.mock(HmsClient.class);
        RecordingContext ctx = new RecordingContext(CATALOG);
        HiveScanPlanProvider provider = new HiveScanPlanProvider(client, new HashMap<>(), ctx);

        List<?> ranges = provider.planScan(ConnectorScanRequest.from(null, emptyPartitionsHandle(),
                Collections.emptyList(), Optional.empty()));

        Assertions.assertTrue(ranges.isEmpty());
        Assertions.assertEquals(1, ctx.events.size());
    }

    @Test
    void planScanFailureDoesNotPublish() {
        HmsClient client = Mockito.mock(HmsClient.class);
        Mockito.when(client.listPartitionNames(Mockito.anyString(), Mockito.anyString(), Mockito.anyInt()))
                .thenThrow(new RuntimeException("hms boom"));
        RecordingContext ctx = new RecordingContext(CATALOG);
        HiveScanPlanProvider provider = new HiveScanPlanProvider(client, new HashMap<>(), ctx);

        // Partitioned handle WITHOUT pruned partitions → forces HMS lookup which throws.
        HiveTableHandle handle = new HiveTableHandle.Builder(DB, TBL, HiveTableType.HIVE)
                .partitionKeyNames(Collections.singletonList("dt"))
                .build();

        Assertions.assertThrows(RuntimeException.class,
                () -> provider.planScan(ConnectorScanRequest.from(null, handle, Collections.emptyList(), Optional.empty())));
        Assertions.assertTrue(ctx.events.isEmpty(), "no audit event on failure");
    }

    @Test
    void publishExceptionDoesNotBubbleOutOfPlanScan() {
        HmsClient client = Mockito.mock(HmsClient.class);
        RecordingContext ctx = new RecordingContext(CATALOG);
        ctx.publishFailure = new IllegalStateException("publisher down");
        HiveScanPlanProvider provider = new HiveScanPlanProvider(client, new HashMap<>(), ctx);

        // planScan must complete normally even though publishAuditEvent throws.
        List<?> ranges = provider.planScan(ConnectorScanRequest.from(null, emptyPartitionsHandle(),
                Collections.emptyList(), Optional.empty()));
        Assertions.assertTrue(ranges.isEmpty());
        Assertions.assertTrue(ctx.events.isEmpty());
    }

    @Test
    void publishErrorAlsoDoesNotBubbleOutOfPlanScan() {
        HmsClient client = Mockito.mock(HmsClient.class);
        RecordingContext ctx = new RecordingContext(CATALOG);
        ctx.publishFailure = new OutOfMemoryError("simulated");
        HiveScanPlanProvider provider = new HiveScanPlanProvider(client, new HashMap<>(), ctx);

        Assertions.assertDoesNotThrow(() -> provider.planScan(ConnectorScanRequest.from(null, emptyPartitionsHandle(),
                Collections.emptyList(), Optional.empty())));
    }

    @Test
    void nullContextSkipsPublishWithoutFailure() {
        HmsClient client = Mockito.mock(HmsClient.class);
        // Back-compat ctor → null context, audit publication is skipped.
        HiveScanPlanProvider provider = new HiveScanPlanProvider(client, new HashMap<>());

        Assertions.assertDoesNotThrow(() -> provider.planScan(ConnectorScanRequest.from(null, emptyPartitionsHandle(),
                Collections.emptyList(), Optional.empty())));
    }

    @Test
    void multiplePlanScanCallsEmitMultipleEvents() {
        HmsClient client = Mockito.mock(HmsClient.class);
        RecordingContext ctx = new RecordingContext(CATALOG);
        HiveScanPlanProvider provider = new HiveScanPlanProvider(client, new HashMap<>(), ctx);

        provider.planScan(ConnectorScanRequest.from(null, emptyPartitionsHandle(), Collections.emptyList(), Optional.empty()));
        provider.planScan(ConnectorScanRequest.from(null, emptyPartitionsHandle(), Collections.emptyList(), Optional.empty()));
        provider.planScan(ConnectorScanRequest.from(null, emptyPartitionsHandle(), Collections.emptyList(), Optional.empty()));

        Assertions.assertEquals(3, ctx.events.size());
        for (ConnectorAuditEvent ev : ctx.events) {
            Assertions.assertTrue(ev instanceof PlanCompletedAuditEvent);
            Assertions.assertEquals(CATALOG, ((PlanCompletedAuditEvent) ev).catalog());
        }
    }

    @Test
    void recordingContextIsolatesEventsBetweenInstances() {
        HmsClient client = Mockito.mock(HmsClient.class);
        RecordingContext ctxA = new RecordingContext("cat_a");
        RecordingContext ctxB = new RecordingContext("cat_b");
        HiveScanPlanProvider providerA = new HiveScanPlanProvider(client, new HashMap<>(), ctxA);
        HiveScanPlanProvider providerB = new HiveScanPlanProvider(client, new HashMap<>(), ctxB);

        providerA.planScan(ConnectorScanRequest.from(null, emptyPartitionsHandle(), Collections.emptyList(), Optional.empty()));
        providerB.planScan(ConnectorScanRequest.from(null, emptyPartitionsHandle(), Collections.emptyList(), Optional.empty()));
        providerB.planScan(ConnectorScanRequest.from(null, emptyPartitionsHandle(), Collections.emptyList(), Optional.empty()));

        Assertions.assertEquals(1, ctxA.events.size());
        Assertions.assertEquals(2, ctxB.events.size());
        Assertions.assertEquals("cat_a", ((PlanCompletedAuditEvent) ctxA.events.get(0)).catalog());
        for (ConnectorAuditEvent ev : ctxB.events) {
            Assertions.assertEquals("cat_b", ((PlanCompletedAuditEvent) ev).catalog());
        }
    }

    /** Sanity: a Map with at least one key/value still has the catalog props passed through. */
    @Test
    void planScanPublishesEvenWithCustomCatalogProperties() {
        HmsClient client = Mockito.mock(HmsClient.class);
        RecordingContext ctx = new RecordingContext(CATALOG);
        Map<String, String> props = new HashMap<>();
        props.put("uri", "thrift://nowhere:9083");
        HiveScanPlanProvider provider = new HiveScanPlanProvider(client, props, ctx);

        provider.planScan(ConnectorScanRequest.from(null, emptyPartitionsHandle(), Collections.emptyList(), Optional.empty()));

        Assertions.assertEquals(1, ctx.events.size());
    }
}
