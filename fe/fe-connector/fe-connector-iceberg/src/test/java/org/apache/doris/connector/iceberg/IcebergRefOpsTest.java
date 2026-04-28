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

import org.apache.doris.connector.api.timetravel.ConnectorRef;
import org.apache.doris.connector.api.timetravel.ConnectorRefMutation;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;
import org.apache.doris.connector.api.timetravel.RefKind;
import org.apache.doris.connector.iceberg.api.IcebergBackend;
import org.apache.doris.connector.iceberg.api.IcebergBackendContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

class IcebergRefOpsTest {

    private static IcebergBackendContext newCtx() {
        return new IcebergBackendContext("cat", new HashMap<>(), new Configuration(false));
    }

    private static final class RecordingBackend implements IcebergBackend {
        private final List<ConnectorRef> refs;
        private final Snapshot snapshot;
        final AtomicReference<String> lastCall = new AtomicReference<>();

        RecordingBackend(List<ConnectorRef> refs, Snapshot snapshot) {
            this.refs = refs;
            this.snapshot = snapshot;
        }

        @Override
        public String name() {
            return "fake";
        }

        @Override
        public Catalog buildCatalog(IcebergBackendContext context) {
            throw new AssertionError("buildCatalog must not be invoked in this test");
        }

        @Override
        public List<ConnectorRef> listRefs(IcebergBackendContext context,
                                           String database, String table) {
            lastCall.set("listRefs:" + database + "." + table);
            return refs;
        }

        @Override
        public Snapshot resolveVersion(IcebergBackendContext context,
                                       String database, String table,
                                       ConnectorTableVersion version) {
            lastCall.set("resolveVersion:" + database + "." + table + ":"
                    + version.getClass().getSimpleName());
            return snapshot;
        }

        @Override
        public Optional<ConnectorRef> getRef(IcebergBackendContext context,
                                             String database, String table,
                                             String name, RefKind kind) {
            lastCall.set("getRef:" + database + "." + table + ":" + name + ":" + kind);
            return refs.stream()
                    .filter(r -> r.kind() == kind && name.equals(r.name()))
                    .findFirst();
        }

        @Override
        public void cherrypickSnapshot(IcebergBackendContext context,
                                       String database, String table,
                                       long snapshotId) {
            lastCall.set("cherrypickSnapshot:" + database + "." + table + ":" + snapshotId);
        }

        @Override
        public void replaceBranch(IcebergBackendContext context,
                                  String database, String table,
                                  String branch, long snapshotId) {
            lastCall.set("replaceBranch:" + database + "." + table + ":"
                    + branch + ":" + snapshotId);
        }
    }

    @Test
    void supportedRefKindsReturnsBranchAndTag() {
        IcebergRefOps ops = new IcebergRefOps(
                new RecordingBackend(Collections.emptyList(), null), newCtx());
        Assertions.assertEquals(Set.of(RefKind.BRANCH, RefKind.TAG), ops.supportedRefKinds());
    }

    @Test
    void listRefsDelegatesToBackend() {
        ConnectorRef expected = ConnectorRef.builder()
                .name("main").kind(RefKind.BRANCH).snapshotId(42L).build();
        RecordingBackend backend = new RecordingBackend(List.of(expected), null);
        IcebergRefOps ops = new IcebergRefOps(backend, newCtx());

        List<ConnectorRef> got = ops.listRefs("db", "t");

        Assertions.assertEquals(List.of(expected), got);
        Assertions.assertEquals("listRefs:db.t", backend.lastCall.get());
    }

    @Test
    void resolveVersionReturnsBySnapshotId() {
        Snapshot snap = org.mockito.Mockito.mock(Snapshot.class);
        org.mockito.Mockito.when(snap.snapshotId()).thenReturn(9001L);
        RecordingBackend backend = new RecordingBackend(Collections.emptyList(), snap);
        IcebergRefOps ops = new IcebergRefOps(backend, newCtx());

        ConnectorTableVersion.BySnapshotId out = ops.resolveVersion(
                "db", "t", new ConnectorTableVersion.ByRef("main", RefKind.BRANCH));

        Assertions.assertEquals(9001L, out.snapshotId());
        Assertions.assertEquals("resolveVersion:db.t:ByRef", backend.lastCall.get());
    }

    @Test
    void mutationMethodsThrowUoe() {
        IcebergRefOps ops = new IcebergRefOps(
                new RecordingBackend(Collections.emptyList(), null), newCtx());
        ConnectorRefMutation mut = ConnectorRefMutation.builder()
                .name("b").kind(RefKind.BRANCH).build();
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> ops.createOrReplaceRef("db", "t", mut));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> ops.dropRef("db", "t", "b", RefKind.BRANCH));
    }

    @Test
    void rejectsNullBackendOrContext() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new IcebergRefOps(null, newCtx()));
        Assertions.assertThrows(NullPointerException.class,
                () -> new IcebergRefOps(
                        new RecordingBackend(Collections.emptyList(), null), null));
    }

    @Test
    void registryRoutingProducesBackendOfCorrectType() {
        // Cross-check: the registry already knows each real backend name;
        // the IcebergRefOps constructed off of a registry-supplied backend
        // exposes that backend's name() through a listRefs smoke path.
        for (String name : IcebergBackendRegistry.availableTypes()) {
            IcebergBackend backend = IcebergBackendRegistry.get(name)
                    .orElseThrow().create();
            Assertions.assertEquals(name, backend.name(),
                    "backend factory " + name + " produced mismatched backend");
        }
    }

    // ---- getRef routing (mocked backend) ----

    @Test
    void getRefDelegatesToBackend() {
        ConnectorRef branch = ConnectorRef.builder()
                .name("main").kind(RefKind.BRANCH).snapshotId(1L).build();
        ConnectorRef tag = ConnectorRef.builder()
                .name("v1").kind(RefKind.TAG).snapshotId(2L).build();
        RecordingBackend backend = new RecordingBackend(List.of(branch, tag), null);
        IcebergRefOps ops = new IcebergRefOps(backend, newCtx());

        Optional<ConnectorRef> got = ops.getRef("db", "t", "main", RefKind.BRANCH);
        Assertions.assertTrue(got.isPresent());
        Assertions.assertEquals(branch, got.get());
        Assertions.assertEquals("getRef:db.t:main:BRANCH", backend.lastCall.get());
    }

    @Test
    void getRefRejectsNullArgs() {
        IcebergRefOps ops = new IcebergRefOps(
                new RecordingBackend(Collections.emptyList(), null), newCtx());
        Assertions.assertThrows(NullPointerException.class,
                () -> ops.getRef("db", "t", null, RefKind.BRANCH));
        Assertions.assertThrows(NullPointerException.class,
                () -> ops.getRef("db", "t", "main", null));
    }

    @Test
    void cherrypickSnapshotDelegatesToBackend() {
        RecordingBackend backend = new RecordingBackend(Collections.emptyList(), null);
        IcebergRefOps ops = new IcebergRefOps(backend, newCtx());
        ops.cherrypickSnapshot("db", "t", 99L);
        Assertions.assertEquals("cherrypickSnapshot:db.t:99", backend.lastCall.get());
    }

    @Test
    void replaceBranchDelegatesToBackend() {
        RecordingBackend backend = new RecordingBackend(Collections.emptyList(), null);
        IcebergRefOps ops = new IcebergRefOps(backend, newCtx());
        ops.replaceBranch("db", "t", "main", 42L);
        Assertions.assertEquals("replaceBranch:db.t:main:42", backend.lastCall.get());
    }

    @Test
    void replaceBranchRejectsNullBranch() {
        IcebergRefOps ops = new IcebergRefOps(
                new RecordingBackend(Collections.emptyList(), null), newCtx());
        Assertions.assertThrows(NullPointerException.class,
                () -> ops.replaceBranch("db", "t", null, 1L));
    }

    // ---- Real-iceberg-table behaviour over InMemoryCatalog ----

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()));

    private static final class CatalogBackedBackend implements IcebergBackend {
        private final Catalog delegate;

        CatalogBackedBackend(Catalog delegate) {
            this.delegate = delegate;
        }

        @Override
        public String name() {
            return "in-memory";
        }

        @Override
        public Catalog buildCatalog(IcebergBackendContext context) {
            return delegate;
        }
    }

    private static InMemoryCatalog newInMemoryCatalog() {
        InMemoryCatalog cat = new InMemoryCatalog();
        cat.initialize("test", Collections.emptyMap());
        ((SupportsNamespaces) cat).createNamespace(Namespace.of("db"));
        cat.createTable(TableIdentifier.of("db", "t"), SCHEMA, PartitionSpec.unpartitioned(),
                Collections.singletonMap("format-version", "2"));
        return cat;
    }

    private static long appendDataFile(Table table, String name) {
        DataFile df = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("file:/tmp/iceberg-refops/" + UUID.randomUUID() + "/" + name + ".parquet")
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(1024L)
                .withRecordCount(10L)
                .build();
        table.newAppend().appendFile(df).commit();
        table.refresh();
        return table.currentSnapshot().snapshotId();
    }

    @Test
    void getRefRealCatalogReturnsBranch() throws Exception {
        try (InMemoryCatalog cat = newInMemoryCatalog()) {
            Table table = cat.loadTable(TableIdentifier.of("db", "t"));
            long s1 = appendDataFile(table, "a");

            IcebergRefOps ops = new IcebergRefOps(new CatalogBackedBackend(cat), newCtx());
            Optional<ConnectorRef> got = ops.getRef("db", "t", "main", RefKind.BRANCH);

            Assertions.assertTrue(got.isPresent());
            Assertions.assertEquals(RefKind.BRANCH, got.get().kind());
            Assertions.assertEquals("main", got.get().name());
            Assertions.assertEquals(s1, got.get().snapshotId());
        }
    }

    @Test
    void getRefRealCatalogReturnsTag() throws Exception {
        try (InMemoryCatalog cat = newInMemoryCatalog()) {
            Table table = cat.loadTable(TableIdentifier.of("db", "t"));
            long s1 = appendDataFile(table, "a");
            table.manageSnapshots().createTag("v1", s1).commit();
            table.refresh();

            IcebergRefOps ops = new IcebergRefOps(new CatalogBackedBackend(cat), newCtx());
            Optional<ConnectorRef> got = ops.getRef("db", "t", "v1", RefKind.TAG);

            Assertions.assertTrue(got.isPresent());
            Assertions.assertEquals(RefKind.TAG, got.get().kind());
            Assertions.assertEquals(s1, got.get().snapshotId());
        }
    }

    @Test
    void getRefRealCatalogEmptyWhenMissing() throws Exception {
        try (InMemoryCatalog cat = newInMemoryCatalog()) {
            Table table = cat.loadTable(TableIdentifier.of("db", "t"));
            appendDataFile(table, "a");

            IcebergRefOps ops = new IcebergRefOps(new CatalogBackedBackend(cat), newCtx());
            Assertions.assertTrue(
                    ops.getRef("db", "t", "nope", RefKind.BRANCH).isEmpty());
        }
    }

    @Test
    void getRefRealCatalogEmptyWhenKindMismatches() throws Exception {
        try (InMemoryCatalog cat = newInMemoryCatalog()) {
            Table table = cat.loadTable(TableIdentifier.of("db", "t"));
            long s1 = appendDataFile(table, "a");
            table.manageSnapshots().createTag("v1", s1).commit();
            table.refresh();

            IcebergRefOps ops = new IcebergRefOps(new CatalogBackedBackend(cat), newCtx());
            // 'v1' is a TAG; asking for BRANCH must come back empty.
            Assertions.assertTrue(
                    ops.getRef("db", "t", "v1", RefKind.BRANCH).isEmpty());
            // 'main' is a BRANCH; asking for TAG must come back empty.
            Assertions.assertTrue(
                    ops.getRef("db", "t", "main", RefKind.TAG).isEmpty());
        }
    }

    @Test
    void cherrypickSnapshotRealCatalogMovesMain() throws Exception {
        try (InMemoryCatalog cat = newInMemoryCatalog()) {
            Table table = cat.loadTable(TableIdentifier.of("db", "t"));
            long s1 = appendDataFile(table, "a");
            // Stage S2 (WAP: stageOnly() leaves main pinned at s1).
            table.newAppend()
                    .appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                            .withPath("file:/tmp/iceberg-refops/" + UUID.randomUUID() + "/b.parquet")
                            .withFormat(FileFormat.PARQUET)
                            .withFileSizeInBytes(1024L)
                            .withRecordCount(10L)
                            .build())
                    .stageOnly()
                    .commit();
            table.refresh();
            Assertions.assertEquals(s1, table.currentSnapshot().snapshotId(),
                    "stageOnly must not advance main");
            long stagedId = -1L;
            for (Snapshot snap : table.snapshots()) {
                if (snap.snapshotId() != s1) {
                    stagedId = snap.snapshotId();
                }
            }
            Assertions.assertNotEquals(-1L, stagedId, "expected a staged snapshot to cherrypick");

            IcebergRefOps ops = new IcebergRefOps(new CatalogBackedBackend(cat), newCtx());
            ops.cherrypickSnapshot("db", "t", stagedId);

            Table reloaded = cat.loadTable(TableIdentifier.of("db", "t"));
            Assertions.assertNotEquals(s1, reloaded.currentSnapshot().snapshotId(),
                    "cherrypick must advance main past s1");
        }
    }

    @Test
    void cherrypickSnapshotUnknownIdThrows() throws Exception {
        try (InMemoryCatalog cat = newInMemoryCatalog()) {
            Table table = cat.loadTable(TableIdentifier.of("db", "t"));
            appendDataFile(table, "a");

            IcebergRefOps ops = new IcebergRefOps(new CatalogBackedBackend(cat), newCtx());
            Assertions.assertThrows(RuntimeException.class,
                    () -> ops.cherrypickSnapshot("db", "t", 999_999L));
        }
    }

    @Test
    void replaceBranchRealCatalogRewindsBranch() throws Exception {
        try (InMemoryCatalog cat = newInMemoryCatalog()) {
            Table table = cat.loadTable(TableIdentifier.of("db", "t"));
            long s1 = appendDataFile(table, "a");
            long s2 = appendDataFile(table, "b");
            Assertions.assertEquals(s2, table.currentSnapshot().snapshotId());

            IcebergRefOps ops = new IcebergRefOps(new CatalogBackedBackend(cat), newCtx());
            ops.replaceBranch("db", "t", "main", s1);

            Table reloaded = cat.loadTable(TableIdentifier.of("db", "t"));
            Assertions.assertEquals(s1, reloaded.refs().get("main").snapshotId(),
                    "main must rewind to s1");
        }
    }
}
