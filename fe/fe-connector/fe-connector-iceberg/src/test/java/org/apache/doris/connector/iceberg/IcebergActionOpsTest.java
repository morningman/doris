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

import org.apache.doris.connector.api.ConnectorTableId;
import org.apache.doris.connector.api.action.ActionArgument;
import org.apache.doris.connector.api.action.ActionArgumentType;
import org.apache.doris.connector.api.action.ActionDescriptor;
import org.apache.doris.connector.api.action.ActionInvocation;
import org.apache.doris.connector.api.action.ActionResult;
import org.apache.doris.connector.api.action.ActionTarget;
import org.apache.doris.connector.api.action.ConnectorActionOps;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

class IcebergActionOpsTest {

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()));

    // ---- listActions ----

    @Test
    void listActionsTableScopeReturnsThreeDescriptors() {
        IcebergActionOps ops = new IcebergActionOps((db, tbl) -> {
            throw new AssertionError("loader must not be called");
        });
        List<ActionDescriptor> descriptors = ops.listActions(
                ConnectorActionOps.Scope.TABLE, Optional.empty());
        Set<String> names = descriptors.stream()
                .map(ActionDescriptor::name)
                .collect(Collectors.toSet());
        Assertions.assertEquals(
                Set.of("expire_snapshots", "remove_orphan_files", "rewrite_data_files"),
                names);
        ActionDescriptor expire = findDescriptor(descriptors, "expire_snapshots");
        Assertions.assertEquals(ConnectorActionOps.Scope.TABLE, expire.scope());
        Set<String> argNames = expire.arguments().stream()
                .map(ActionArgument::name)
                .collect(Collectors.toSet());
        Assertions.assertEquals(
                Set.of("older_than", "retain_last", "snapshot_ids",
                        "clean_expired_files", "dry_run"),
                argNames);
        Assertions.assertEquals(2, expire.resultSchema().size());
        Assertions.assertEquals("snapshot_id", expire.resultSchema().get(0).name());
        Assertions.assertEquals(ActionArgumentType.LONG,
                expire.resultSchema().get(0).type());
    }

    @Test
    void listActionsCatalogAndSchemaScopesAreEmpty() {
        IcebergActionOps ops = new IcebergActionOps((db, tbl) -> null);
        Assertions.assertTrue(ops.listActions(
                ConnectorActionOps.Scope.CATALOG, Optional.empty()).isEmpty());
        Assertions.assertTrue(ops.listActions(
                ConnectorActionOps.Scope.SCHEMA,
                Optional.of(ActionTarget.ofSchema("db"))).isEmpty());
    }

    @Test
    void removeOrphanFilesDescriptorDryRunDefaultsTrue() {
        IcebergActionOps ops = new IcebergActionOps((db, tbl) -> null);
        ActionDescriptor d = findDescriptor(
                ops.listActions(ConnectorActionOps.Scope.TABLE, Optional.empty()),
                "remove_orphan_files");
        ActionArgument dryRun = d.arguments().stream()
                .filter(a -> a.name().equals("dry_run"))
                .findFirst().orElseThrow();
        Assertions.assertEquals("true", dryRun.defaultValue().orElse(null));
    }

    // ---- expire_snapshots ----

    @Test
    void expireSnapshotsRequiresAtLeastOneSelector(@TempDir Path tmp) {
        Table table = newHadoopTable(tmp, "t1");
        IcebergActionOps ops = new IcebergActionOps((db, tbl) -> table);
        ActionResult r = ops.executeExpireSnapshots("db", "t1", new LinkedHashMap<>());
        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        Assertions.assertTrue(r.errorMessage().orElse("").contains("at least one of"),
                r.errorMessage().orElse(""));
    }

    @Test
    void expireSnapshotsRetainLastMustBePositive(@TempDir Path tmp) {
        Table table = newHadoopTable(tmp, "t2");
        IcebergActionOps ops = new IcebergActionOps((db, tbl) -> table);
        Map<String, Object> args = baseExpireArgs();
        args.put("retain_last", 0L);
        ActionResult r = ops.executeExpireSnapshots("db", "t2", args);
        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        Assertions.assertTrue(r.errorMessage().orElse("").contains("retain_last"));
    }

    @Test
    void expireSnapshotsDryRunListsCandidatesWithoutMutating(@TempDir Path tmp) {
        Table table = newHadoopTable(tmp, "t3");
        long s1 = appendDataFile(table, "f1");
        long s2 = appendDataFile(table, "f2");
        long s3 = appendDataFile(table, "f3");

        IcebergActionOps ops = new IcebergActionOps((db, tbl) -> table);
        Map<String, Object> args = baseExpireArgs();
        args.put("retain_last", 1L);
        args.put("dry_run", true);
        ActionResult r = ops.executeExpireSnapshots("db", "t3", args);

        Assertions.assertEquals(ActionResult.Lifecycle.COMPLETED, r.lifecycle());
        Set<Long> wouldExpire = r.rows().stream()
                .map(row -> (Long) row.get("snapshot_id"))
                .collect(Collectors.toSet());
        Assertions.assertEquals(Set.of(s1, s2), wouldExpire);
        for (Map<String, Object> row : r.rows()) {
            Assertions.assertEquals("WOULD_EXPIRE", row.get("action"));
        }

        // Table must remain untouched.
        table.refresh();
        Set<Long> after = StreamSupport.stream(table.snapshots().spliterator(), false)
                .map(Snapshot::snapshotId)
                .collect(Collectors.toSet());
        Assertions.assertEquals(Set.of(s1, s2, s3), after);
    }

    @Test
    void expireSnapshotsCommitRemovesExpiredSnapshots(@TempDir Path tmp) {
        Table table = newHadoopTable(tmp, "t4");
        long s1 = appendDataFile(table, "f1");
        long s2 = appendDataFile(table, "f2");
        long s3 = appendDataFile(table, "f3");

        IcebergActionOps ops = new IcebergActionOps((db, tbl) -> table);
        Map<String, Object> args = baseExpireArgs();
        args.put("retain_last", 1L);
        ActionResult r = ops.executeExpireSnapshots("db", "t4", args);

        Assertions.assertEquals(ActionResult.Lifecycle.COMPLETED, r.lifecycle());
        Set<Long> expired = r.rows().stream()
                .map(row -> (Long) row.get("snapshot_id"))
                .collect(Collectors.toSet());
        Assertions.assertEquals(Set.of(s1, s2), expired);
        for (Map<String, Object> row : r.rows()) {
            Assertions.assertEquals("EXPIRED", row.get("action"));
        }
        table.refresh();
        Set<Long> after = StreamSupport.stream(table.snapshots().spliterator(), false)
                .map(Snapshot::snapshotId)
                .collect(Collectors.toSet());
        Assertions.assertEquals(Set.of(s3), after);
    }

    @Test
    void expireSnapshotsByExplicitIdsHonored(@TempDir Path tmp) {
        Table table = newHadoopTable(tmp, "t5");
        long s1 = appendDataFile(table, "f1");
        long s2 = appendDataFile(table, "f2");
        long s3 = appendDataFile(table, "f3");

        IcebergActionOps ops = new IcebergActionOps((db, tbl) -> table);
        // Only snapshot_ids set: explicit removal of s1 only (s1 is an ancestor
        // several steps back, so removing it does not orphan any ref).
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("older_than", null);
        args.put("retain_last", null);
        args.put("snapshot_ids", String.valueOf(s1));
        args.put("clean_expired_files", true);
        args.put("dry_run", false);
        ActionResult r = ops.executeExpireSnapshots("db", "t5", args);

        Assertions.assertEquals(ActionResult.Lifecycle.COMPLETED, r.lifecycle());
        Set<Long> expired = r.rows().stream()
                .map(row -> (Long) row.get("snapshot_id"))
                .collect(Collectors.toSet());
        Assertions.assertEquals(Set.of(s1), expired);
        table.refresh();
        Set<Long> after = StreamSupport.stream(table.snapshots().spliterator(), false)
                .map(Snapshot::snapshotId)
                .collect(Collectors.toSet());
        Assertions.assertEquals(Set.of(s2, s3), after);
    }

    @Test
    void expireSnapshotsCleanExpiredFilesFalseRespected(@TempDir Path tmp) throws IOException {
        Table table = newHadoopTable(tmp, "t6");
        long s1 = appendDataFile(table, "f1");
        appendDataFile(table, "f2");
        appendDataFile(table, "f3");

        // Capture file path of s1's data file before expire so we can verify it
        // remains on disk when clean_expired_files=false.
        Snapshot snap1 = table.snapshot(s1);
        String dataFilePath = snap1.addedDataFiles(table.io()).iterator().next()
                .location();
        Path onDisk = filePathFromUri(dataFilePath);
        Assertions.assertTrue(Files.exists(onDisk),
                "data file should exist before expire: " + onDisk);

        IcebergActionOps ops = new IcebergActionOps((db, tbl) -> table);
        Map<String, Object> args = baseExpireArgs();
        args.put("retain_last", 1L);
        args.put("clean_expired_files", false);
        ActionResult r = ops.executeExpireSnapshots("db", "t6", args);
        Assertions.assertEquals(ActionResult.Lifecycle.COMPLETED, r.lifecycle());

        // Data file must remain on disk.
        Assertions.assertTrue(Files.exists(onDisk),
                "data file must remain when clean_expired_files=false: " + onDisk);
    }

    @Test
    void expireSnapshotsLoadFailureWrappedAsFailed() {
        IcebergActionOps ops = new IcebergActionOps((db, tbl) -> {
            throw new RuntimeException("boom");
        });
        Map<String, Object> args = baseExpireArgs();
        args.put("retain_last", 1L);
        ActionResult r = ops.executeExpireSnapshots("db", "t", args);
        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        Assertions.assertTrue(r.errorMessage().orElse("").contains("boom"));
    }

    // ---- remove_orphan_files ----

    @Test
    void removeOrphanFilesDryRunListsOrphans(@TempDir Path tmp) throws IOException {
        Table table = newHadoopTable(tmp, "ro1");
        appendDataFile(table, "live");

        // Create an orphan file in the table location with old mtime.
        Path orphan = Path.of(table.location().replace("file:", ""), "data", "orphan.parquet");
        Files.createDirectories(orphan.getParent());
        Files.writeString(orphan, "x");
        Files.setLastModifiedTime(orphan, FileTime.fromMillis(1000L));

        IcebergActionOps ops = new IcebergActionOps((db, tbl) -> table);
        Map<String, Object> args = newOrphanArgs();
        args.put("dry_run", true);
        ActionResult r = ops.executeRemoveOrphanFiles("db", "ro1", args);
        Assertions.assertEquals(ActionResult.Lifecycle.COMPLETED, r.lifecycle());

        Set<String> reported = r.rows().stream()
                .map(row -> (String) row.get("orphan_file_path"))
                .map(IcebergActionOps::normalizePath)
                .collect(Collectors.toSet());
        Assertions.assertTrue(reported.contains(IcebergActionOps.normalizePath(orphan.toUri().toString())),
                "orphan file should be listed; got: " + reported);
        for (Map<String, Object> row : r.rows()) {
            Assertions.assertEquals("WOULD_DELETE", row.get("action"));
        }
        // dry_run must not delete the file.
        Assertions.assertTrue(Files.exists(orphan));
    }

    @Test
    void removeOrphanFilesDeletesWhenDryRunFalse(@TempDir Path tmp) throws IOException {
        Table table = newHadoopTable(tmp, "ro2");
        appendDataFile(table, "live");

        Path orphan = Path.of(table.location().replace("file:", ""), "data", "orphan2.parquet");
        Files.createDirectories(orphan.getParent());
        Files.writeString(orphan, "x");
        Files.setLastModifiedTime(orphan, FileTime.fromMillis(1000L));

        IcebergActionOps ops = new IcebergActionOps((db, tbl) -> table);
        Map<String, Object> args = newOrphanArgs();
        args.put("dry_run", false);
        ActionResult r = ops.executeRemoveOrphanFiles("db", "ro2", args);
        Assertions.assertEquals(ActionResult.Lifecycle.COMPLETED, r.lifecycle());

        boolean foundDeleted = r.rows().stream()
                .anyMatch(row -> "DELETED".equals(row.get("action"))
                        && IcebergActionOps.normalizePath((String) row.get("orphan_file_path"))
                                .equals(IcebergActionOps.normalizePath(orphan.toUri().toString())));
        Assertions.assertTrue(foundDeleted, "orphan should be marked DELETED, got: " + r.rows());
        Assertions.assertFalse(Files.exists(orphan), "orphan file should be deleted on disk");
    }

    @Test
    void removeOrphanFilesPreservesLiveDataFile(@TempDir Path tmp) {
        Table table = newHadoopTable(tmp, "ro3");
        long s1 = appendDataFile(table, "live");
        String liveLoc = table.snapshot(s1).addedDataFiles(table.io()).iterator().next().location();
        String liveNorm = IcebergActionOps.normalizePath(liveLoc);

        IcebergActionOps ops = new IcebergActionOps((db, tbl) -> table);
        Map<String, Object> args = newOrphanArgs();
        args.put("dry_run", true);
        ActionResult r = ops.executeRemoveOrphanFiles("db", "ro3", args);
        Assertions.assertEquals(ActionResult.Lifecycle.COMPLETED, r.lifecycle());

        for (Map<String, Object> row : r.rows()) {
            String got = IcebergActionOps.normalizePath((String) row.get("orphan_file_path"));
            Assertions.assertNotEquals(liveNorm, got,
                    "live data file must not be reported as orphan: " + got);
        }
    }

    @Test
    void removeOrphanFilesOlderThanThresholdRespected(@TempDir Path tmp) throws IOException {
        Table table = newHadoopTable(tmp, "ro4");
        appendDataFile(table, "live");

        Path freshOrphan = Path.of(table.location().replace("file:", ""), "data", "fresh.parquet");
        Files.createDirectories(freshOrphan.getParent());
        Files.writeString(freshOrphan, "x");
        // mtime = "now"; threshold below = much earlier so this should be skipped.
        long now = System.currentTimeMillis();
        Files.setLastModifiedTime(freshOrphan, FileTime.fromMillis(now));

        IcebergActionOps ops = new IcebergActionOps((db, tbl) -> table);
        Map<String, Object> args = newOrphanArgs();
        args.put("older_than", now - 60_000L);
        args.put("dry_run", true);
        ActionResult r = ops.executeRemoveOrphanFiles("db", "ro4", args);
        Assertions.assertEquals(ActionResult.Lifecycle.COMPLETED, r.lifecycle());

        String freshNorm = IcebergActionOps.normalizePath(freshOrphan.toUri().toString());
        for (Map<String, Object> row : r.rows()) {
            Assertions.assertNotEquals(freshNorm,
                    IcebergActionOps.normalizePath((String) row.get("orphan_file_path")),
                    "fresh orphan must not be reported when older_than threshold excludes it");
        }
        Assertions.assertTrue(Files.exists(freshOrphan));
    }

    // ---- rewrite_data_files (descriptor-only; execution declared unsupported) ----

    @Test
    void rewriteDataFilesReturnsFailedWithDescriptiveMessage() {
        IcebergActionOps ops = new IcebergActionOps((db, tbl) -> null);
        ActionInvocation inv = ActionInvocation.builder()
                .name("rewrite_data_files")
                .scope(ConnectorActionOps.Scope.TABLE)
                .target(ActionTarget.ofTable(ConnectorTableId.of("db", "t")))
                .arguments(new HashMap<>())
                .build();
        ActionResult r = ops.executeAction(inv);
        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        String msg = r.errorMessage().orElse("");
        Assertions.assertTrue(msg.contains("rewrite_data_files"), msg);
        Assertions.assertTrue(msg.contains("BE compaction worker"), msg);
    }

    // ---- executeAction routing & invariants ----

    @Test
    void executeActionRejectsNonTableScope() {
        IcebergActionOps ops = new IcebergActionOps((db, tbl) -> null);
        ActionInvocation inv = ActionInvocation.builder()
                .name("expire_snapshots")
                .scope(ConnectorActionOps.Scope.SCHEMA)
                .target(ActionTarget.ofSchema("db"))
                .build();
        ActionResult r = ops.executeAction(inv);
        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
    }

    @Test
    void executeActionUnknownNameReturnsFailed() {
        IcebergActionOps ops = new IcebergActionOps((db, tbl) -> null);
        ActionInvocation inv = ActionInvocation.builder()
                .name("does_not_exist")
                .scope(ConnectorActionOps.Scope.TABLE)
                .target(ActionTarget.ofTable(ConnectorTableId.of("db", "t")))
                .build();
        ActionResult r = ops.executeAction(inv);
        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        Assertions.assertTrue(r.errorMessage().orElse("").contains("Unknown iceberg action"));
    }

    @Test
    void executeActionRoundTripExpireSnapshotsViaInvocation(@TempDir Path tmp) {
        Table table = newHadoopTable(tmp, "rt1");
        appendDataFile(table, "a");
        appendDataFile(table, "b");

        IcebergActionOps ops = new IcebergActionOps((db, tbl) -> table);
        Map<String, Object> args = baseExpireArgs();
        args.put("retain_last", 1L);
        args.put("dry_run", true);
        ActionInvocation inv = ActionInvocation.builder()
                .name("EXPIRE_SNAPSHOTS") // case-insensitive
                .scope(ConnectorActionOps.Scope.TABLE)
                .target(ActionTarget.ofTable(ConnectorTableId.of("db", "rt1")))
                .arguments(args)
                .build();
        ActionResult r = ops.executeAction(inv);
        Assertions.assertEquals(ActionResult.Lifecycle.COMPLETED, r.lifecycle());
        Assertions.assertEquals(1, r.rows().size());
    }

    @Test
    void normalizePathHandlesFileSchemeAndNullSafe() {
        Assertions.assertNull(IcebergActionOps.normalizePath(null));
        // Same path expressed two different ways must normalize equivalently.
        String a = IcebergActionOps.normalizePath("file:/tmp/a/./b");
        String b = IcebergActionOps.normalizePath("file:/tmp/a/b");
        Assertions.assertEquals(a, b);
    }

    @Test
    void constructorRejectsNullLoader() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new IcebergActionOps((BiFunction<String, String, Table>) null));
    }

    // ---- helpers ----

    private static ActionDescriptor findDescriptor(List<ActionDescriptor> all, String name) {
        return all.stream().filter(d -> d.name().equals(name)).findFirst().orElseThrow();
    }

    private static Map<String, Object> baseExpireArgs() {
        Map<String, Object> m = new LinkedHashMap<>();
        // Use future timestamp so retain_last actually triggers expiration
        // (iceberg's ExpireSnapshots only removes snapshots older than this
        // wall-clock cutoff; without it, retain_last is a no-op for fresh
        // snapshots).
        m.put("older_than", System.currentTimeMillis() + 60_000L);
        m.put("retain_last", null);
        m.put("snapshot_ids", null);
        m.put("clean_expired_files", true);
        m.put("dry_run", false);
        return m;
    }

    private static Map<String, Object> newOrphanArgs() {
        Map<String, Object> m = new LinkedHashMap<>();
        // Threshold = "now + buffer" -> all on-disk files (live or orphan)
        // are eligible by mtime; reachable-set filtering is what protects
        // live files.
        m.put("older_than", System.currentTimeMillis() + 60_000L);
        m.put("location", null);
        m.put("dry_run", true);
        return m;
    }

    private static Table newHadoopTable(Path tmp, String name) {
        HadoopTables tables = new HadoopTables(new Configuration(false));
        Path loc = tmp.resolve(name);
        return tables.create(SCHEMA, PartitionSpec.unpartitioned(),
                Map.of("format-version", "2"), loc.toUri().toString());
    }

    private static long appendDataFile(Table table, String name) {
        DataFile df = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(table.location() + "/data/" + name + "-" + UUID.randomUUID() + ".parquet")
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(1024L)
                .withRecordCount(10L)
                .build();
        // Materialize the path on disk so listPrefix / orphan walker sees it.
        try {
            Path p = filePathFromUri(df.location());
            Files.createDirectories(p.getParent());
            Files.writeString(p, "data");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        table.newAppend().appendFile(df).commit();
        table.refresh();
        return table.currentSnapshot().snapshotId();
    }

    private static Path filePathFromUri(String uri) {
        if (uri.startsWith("file:")) {
            return Path.of(java.net.URI.create(uri));
        }
        return Path.of(uri);
    }
}
