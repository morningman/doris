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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRequest;
import org.apache.doris.connector.api.timetravel.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;
import org.apache.doris.connector.api.timetravel.RefKind;
import org.apache.doris.connector.iceberg.source.IcebergScanPlanProvider;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

class IcebergTimeTravelTest {

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()));

    private InMemoryCatalog catalog;
    private IcebergConnectorMetadata metadata;
    private Table table;
    private long snap1;
    private long snap2;

    @BeforeEach
    void setUp() {
        catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        ((SupportsNamespaces) catalog).createNamespace(Namespace.of("db"));
        catalog.createTable(TableIdentifier.of("db", "t"), SCHEMA, PartitionSpec.unpartitioned(),
                Collections.singletonMap("format-version", "2"));
        metadata = new IcebergConnectorMetadata(catalog, new HashMap<>(), null, null, null);
        table = catalog.loadTable(TableIdentifier.of("db", "t"));

        appendDataFile("a");
        snap1 = table.currentSnapshot().snapshotId();
        // Sleep a moment so timestamps differ; iceberg snapshot timestampMillis
        // is monotonic but commits inside the same ms tick can collide.
        sleepMs(2L);
        appendDataFile("b");
        snap2 = table.currentSnapshot().snapshotId();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
    }

    private void appendDataFile(String name) {
        DataFile df = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("file:/tmp/iceberg-time-travel/" + UUID.randomUUID() + "/" + name + ".parquet")
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(1024L)
                .withRecordCount(10L)
                .build();
        table.newAppend().appendFile(df).commit();
        table.refresh();
    }

    private static void sleepMs(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void getTableHandleByVersionBySnapshotId() {
        IcebergTableHandle h = (IcebergTableHandle) metadata.getTableHandle(
                null, "db", "t",
                Optional.of(new ConnectorTableVersion.BySnapshotId(snap1)),
                Optional.empty()).orElseThrow();
        Assertions.assertEquals(snap1, h.getSnapshotId().longValue());
    }

    @Test
    void getTableHandleByTimestampResolvesEarlierSnapshot() {
        long snap2CommitMs = table.snapshot(snap2).timestampMillis();
        // Pick a timestamp strictly after snap1 commit and before snap2 commit.
        long tsBeforeSnap2 = snap2CommitMs - 1L;
        IcebergTableHandle h = (IcebergTableHandle) metadata.getTableHandle(
                null, "db", "t",
                Optional.of(new ConnectorTableVersion.ByTimestamp(Instant.ofEpochMilli(tsBeforeSnap2))),
                Optional.empty()).orElseThrow();
        Assertions.assertEquals(snap1, h.getSnapshotId().longValue());
    }

    @Test
    void getTableHandleByRefBranchMain() {
        IcebergTableHandle h = (IcebergTableHandle) metadata.getTableHandle(
                null, "db", "t",
                Optional.of(new ConnectorTableVersion.ByRef("main", RefKind.BRANCH)),
                Optional.empty()).orElseThrow();
        Assertions.assertEquals(snap2, h.getSnapshotId().longValue());
        Assertions.assertEquals("branch:main", h.getRefSpec());
    }

    @Test
    void getTableHandleByRefUnknownThrows() {
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.getTableHandle(null, "db", "t",
                        Optional.of(new ConnectorTableVersion.ByRef("nonexistent", RefKind.BRANCH)),
                        Optional.empty()));
        Assertions.assertTrue(ex.getMessage().contains("nonexistent"));
    }

    @Test
    void getTableHandleByRefTagResolvesTaggedSnapshot() {
        table.manageSnapshots().createTag("v1", snap1).commit();
        IcebergTableHandle h = (IcebergTableHandle) metadata.getTableHandle(
                null, "db", "t",
                Optional.of(new ConnectorTableVersion.ByRef("v1", RefKind.TAG)),
                Optional.empty()).orElseThrow();
        Assertions.assertEquals(snap1, h.getSnapshotId().longValue());
        Assertions.assertEquals("tag:v1", h.getRefSpec());
    }

    @Test
    void getTableHandleByRefKindMismatchThrows() {
        // 'main' is a branch; querying it as TAG must fail.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.getTableHandle(null, "db", "t",
                        Optional.of(new ConnectorTableVersion.ByRef("main", RefKind.TAG)),
                        Optional.empty()));
        Assertions.assertTrue(ex.getMessage().contains("not a tag"));
    }

    @Test
    void getTableHandleByOpaque() {
        IcebergTableHandle h = (IcebergTableHandle) metadata.getTableHandle(
                null, "db", "t",
                Optional.of(new ConnectorTableVersion.ByOpaque(
                        "iceberg:" + snap1 + ":1700000000000")),
                Optional.empty()).orElseThrow();
        Assertions.assertEquals(snap1, h.getSnapshotId().longValue());
    }

    @Test
    void planScanHonoursMvccSnapshotOverHandleSnapshot() {
        // Handle pinned to snap2 (current), but scan request carries an mvcc
        // snapshot pointing at snap1; the scan must read snap1.
        IcebergTableHandle handle = (IcebergTableHandle) metadata.getTableHandle(
                null, "db", "t").orElseThrow();
        Assertions.assertEquals(snap2, handle.getSnapshotId().longValue());

        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(
                (db, tbl) -> table, Collections.emptyMap(), null);
        ConnectorMvccSnapshot mvcc = new IcebergConnectorMvccSnapshot(
                snap1, Instant.ofEpochMilli(table.snapshot(snap1).timestampMillis()));
        ConnectorScanRequest req = ConnectorScanRequest.builder()
                .session(null)
                .table(handle)
                .mvccSnapshot(Optional.of(mvcc))
                .build();
        List<ConnectorScanRange> ranges = provider.planScan(req);
        // snap1 has 1 file, snap2 has 2 files; pinning to snap1 must produce 1 range.
        Assertions.assertEquals(1, ranges.size());
    }

    @Test
    void planScanHonoursVersionWhenHandleHasNoSnapshot() {
        IcebergTableHandle handleNoSnap = IcebergTableHandle.builder()
                .dbName("db").tableName("t").build();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(
                (db, tbl) -> table, Collections.emptyMap(), null);
        ConnectorScanRequest req = ConnectorScanRequest.builder()
                .session(null)
                .table(handleNoSnap)
                .version(Optional.of(new ConnectorTableVersion.BySnapshotId(snap1)))
                .build();
        List<ConnectorScanRange> ranges = provider.planScan(req);
        Assertions.assertEquals(1, ranges.size());
    }

    @Test
    void legacyPlanScanStillWorks() {
        ConnectorTableHandle handle = metadata.getTableHandle(null, "db", "t").orElseThrow();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(
                (db, tbl) -> table, Collections.emptyMap(), null);
        List<ConnectorScanRange> ranges = provider.planScan(ConnectorScanRequest.from(
                null, handle, Collections.emptyList(), Optional.empty()));
        Assertions.assertEquals(2, ranges.size());
    }

    @Test
    void mvccSnapshotCodecFromProviderRoundTrips() {
        IcebergConnectorProvider p = new IcebergConnectorProvider();
        ConnectorMvccSnapshot.Codec codec = p.getMvccSnapshotCodec().orElseThrow();
        IcebergConnectorMvccSnapshot orig =
                new IcebergConnectorMvccSnapshot(snap2, Instant.ofEpochMilli(1234567890L));
        ConnectorMvccSnapshot back = codec.decode(codec.encode(orig));
        Assertions.assertEquals(orig, back);
    }

    @Test
    void getTableHandleWithEmptyVersionAndRefSpecMatchesLegacy() {
        IcebergTableHandle defaulted = (IcebergTableHandle) metadata.getTableHandle(
                null, "db", "t", Optional.empty(), Optional.empty()).orElseThrow();
        IcebergTableHandle legacy = (IcebergTableHandle) metadata.getTableHandle(
                null, "db", "t").orElseThrow();
        Assertions.assertEquals(legacy.getSnapshotId(), defaulted.getSnapshotId());
        Assertions.assertNull(defaulted.getRefSpec());
    }
}
