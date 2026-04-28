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

package org.apache.doris.connector.iceberg.systable;

import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;
import org.apache.doris.connector.api.scan.ConnectorScanRequest;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;

import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

class IcebergMetadataScanFactoryTest {

    @Test
    void createReturnsProviderForGivenType() {
        BiFunction<String, String, Table> loader = (db, t) -> null;
        IcebergMetadataScanFactory factory = new IcebergMetadataScanFactory(
                MetadataTableType.SNAPSHOTS, loader);
        ConnectorScanPlanProvider provider = factory.create(
                "db", "tbl", "snapshots", Optional.empty());
        Assertions.assertNotNull(provider);
        Assertions.assertTrue(provider instanceof IcebergMetadataScanPlanProvider);
        IcebergMetadataScanPlanProvider p = (IcebergMetadataScanPlanProvider) provider;
        Assertions.assertEquals("db", p.database());
        Assertions.assertEquals("tbl", p.table());
        Assertions.assertEquals(MetadataTableType.SNAPSHOTS, p.metadataTableType());
        Assertions.assertEquals(ConnectorScanRangeType.FILE_SCAN, provider.getScanRangeType());
    }

    @Test
    void createDoesNotInvokeBaseTableLoader() {
        AtomicInteger calls = new AtomicInteger();
        IcebergMetadataScanFactory factory = new IcebergMetadataScanFactory(
                MetadataTableType.HISTORY,
                (db, t) -> {
                    calls.incrementAndGet();
                    return null;
                });
        factory.create("db", "tbl", "history", Optional.empty());
        Assertions.assertEquals(0, calls.get(),
                "factory.create() must be lazy — base table loader called only on resolveMetadataTable");
    }

    @Test
    void planScanReturnsEmptyAndDoesNotInvokeLoader() {
        AtomicInteger calls = new AtomicInteger();
        IcebergMetadataScanFactory factory = new IcebergMetadataScanFactory(
                MetadataTableType.REFS,
                (db, t) -> {
                    calls.incrementAndGet();
                    return null;
                });
        ConnectorScanPlanProvider provider = factory.create(
                "db", "tbl", "refs", Optional.empty());
        List<ConnectorScanRange> ranges = provider.planScan(ConnectorScanRequest.from(
                null, org.mockito.Mockito.mock(
                        org.apache.doris.connector.api.handle.ConnectorTableHandle.class),
                Collections.emptyList(), Optional.empty()));
        // M1-13 publishes the SPI surface; range generation is M1-15. Empty plan
        // is the contract until then.
        Assertions.assertTrue(ranges.isEmpty());
        Assertions.assertEquals(0, calls.get(),
                "planScan must not load the base table in M1-13 (deferred to M1-15)");
    }

    @Test
    void resolveMetadataTableInvokesBaseTableLoaderWithGivenIdentity() {
        AtomicReference<String> dbSeen = new AtomicReference<>();
        AtomicReference<String> tblSeen = new AtomicReference<>();
        IcebergMetadataScanFactory factory = new IcebergMetadataScanFactory(
                MetadataTableType.MANIFESTS,
                (db, t) -> {
                    dbSeen.set(db);
                    tblSeen.set(t);
                    // Throw to short-circuit before MetadataTableUtils dereferences null.
                    throw new IllegalStateException("loader-invoked");
                });
        ConnectorScanPlanProvider provider = factory.create(
                "myDb", "myTbl", "manifests", Optional.empty());
        IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class,
                ((IcebergMetadataScanPlanProvider) provider)::resolveMetadataTable);
        Assertions.assertEquals("loader-invoked", ex.getMessage());
        Assertions.assertEquals("myDb", dbSeen.get());
        Assertions.assertEquals("myTbl", tblSeen.get());
    }

    @Test
    void factoryAcceptsButIgnoresVersionWhenSpecDeclaresVersionAgnostic() {
        // The seven sys tables register acceptsTableVersion(false), so fe-core
        // already passes Optional.empty(); but the factory must accept any
        // version arg without exploding (forward-compat).
        IcebergMetadataScanFactory factory = new IcebergMetadataScanFactory(
                MetadataTableType.PARTITIONS, (db, t) -> null);
        ConnectorScanPlanProvider provider = factory.create(
                "db", "tbl", "partitions", Optional.of(new ConnectorTableVersion.BySnapshotId(123L)));
        Assertions.assertNotNull(provider);
    }

    @Test
    void nullArgumentsRejected() {
        IcebergMetadataScanFactory factory = new IcebergMetadataScanFactory(
                MetadataTableType.FILES, (db, t) -> null);
        Assertions.assertThrows(NullPointerException.class,
                () -> factory.create(null, "tbl", "files", Optional.empty()));
        Assertions.assertThrows(NullPointerException.class,
                () -> factory.create("db", null, "files", Optional.empty()));
        Assertions.assertThrows(NullPointerException.class,
                () -> factory.create("db", "tbl", null, Optional.empty()));
        Assertions.assertThrows(NullPointerException.class,
                () -> factory.create("db", "tbl", "files", null));
        Assertions.assertThrows(NullPointerException.class,
                () -> new IcebergMetadataScanFactory(null, (db, t) -> null));
        Assertions.assertThrows(NullPointerException.class,
                () -> new IcebergMetadataScanFactory(MetadataTableType.FILES, null));
    }

    @Test
    void factoryExposesItsMetadataTableType() {
        for (MetadataTableType type : new MetadataTableType[]{
                MetadataTableType.SNAPSHOTS, MetadataTableType.HISTORY,
                MetadataTableType.FILES, MetadataTableType.ENTRIES,
                MetadataTableType.MANIFESTS, MetadataTableType.REFS,
                MetadataTableType.PARTITIONS}) {
            IcebergMetadataScanFactory factory = new IcebergMetadataScanFactory(
                    type, (db, t) -> null);
            Assertions.assertEquals(type, factory.metadataTableType());
        }
    }
}
