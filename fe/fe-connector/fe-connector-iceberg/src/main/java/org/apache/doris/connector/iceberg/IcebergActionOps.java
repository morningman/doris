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

import org.apache.doris.connector.api.action.ActionArgument;
import org.apache.doris.connector.api.action.ActionArgumentType;
import org.apache.doris.connector.api.action.ActionDescriptor;
import org.apache.doris.connector.api.action.ActionInvocation;
import org.apache.doris.connector.api.action.ActionResult;
import org.apache.doris.connector.api.action.ActionTarget;
import org.apache.doris.connector.api.action.ConnectorActionOps;

import com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

/**
 * Iceberg implementation of {@link ConnectorActionOps}, exposing the standard
 * Iceberg housekeeping procedures via the SPI:
 *
 * <ul>
 *   <li>{@code expire_snapshots} — wired against {@link Table#expireSnapshots()}.</li>
 *   <li>{@code remove_orphan_files} — pure-FE walker over
 *       {@link SupportsPrefixOperations#listPrefix(String)} comparing against
 *       reachable paths from {@link ReachableFileUtil} and per-snapshot
 *       manifests.</li>
 *   <li>{@code rewrite_data_files} — descriptor only; execution requires a BE
 *       compaction worker that is not yet wired (see
 *       {@code TODO M3-12-be-compaction-worker}).</li>
 * </ul>
 *
 * <p>Argument values arrive already type-coerced by the engine dispatcher
 * (see {@code PluginDrivenExecuteActionCommand}); optional arguments without a
 * default arrive as {@code null}.</p>
 */
public final class IcebergActionOps implements ConnectorActionOps {

    private static final Logger LOG = LogManager.getLogger(IcebergActionOps.class);

    /** Default {@code older_than} window for {@code remove_orphan_files}: 3 days. */
    static final long DEFAULT_ORPHAN_RETENTION_MS = TimeUnit.DAYS.toMillis(3);

    static final String ACTION_EXPIRE_SNAPSHOTS = "expire_snapshots";
    static final String ACTION_REMOVE_ORPHAN_FILES = "remove_orphan_files";
    static final String ACTION_REWRITE_DATA_FILES = "rewrite_data_files";

    private static final List<ActionDescriptor> TABLE_ACTIONS = List.of(
            buildExpireSnapshotsDescriptor(),
            buildRemoveOrphanFilesDescriptor(),
            buildRewriteDataFilesDescriptor());

    private final BiFunction<String, String, Table> tableLoader;

    public IcebergActionOps(BiFunction<String, String, Table> tableLoader) {
        Objects.requireNonNull(tableLoader, "tableLoader");
        this.tableLoader = tableLoader;
    }

    // -------------------------------------------------------------------- listActions

    @Override
    public List<ActionDescriptor> listActions(Scope scope, Optional<ActionTarget> target) {
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(target, "target");
        if (scope != Scope.TABLE) {
            return Collections.emptyList();
        }
        return TABLE_ACTIONS;
    }

    // -------------------------------------------------------------------- executeAction

    @Override
    public ActionResult executeAction(ActionInvocation invocation) {
        Objects.requireNonNull(invocation, "invocation");
        if (invocation.scope() != Scope.TABLE) {
            return ActionResult.failed("Iceberg actions are only supported at TABLE scope");
        }
        ActionTarget target = invocation.target()
                .orElseThrow(() -> new IllegalArgumentException("invocation.target is required"));
        if (target.id().isEmpty()) {
            return ActionResult.failed("Iceberg actions require a TABLE target");
        }
        String db = target.database();
        String tbl = target.id().get().table();
        String name = invocation.name();
        if (ACTION_EXPIRE_SNAPSHOTS.equalsIgnoreCase(name)) {
            return executeExpireSnapshots(db, tbl, invocation.arguments());
        }
        if (ACTION_REMOVE_ORPHAN_FILES.equalsIgnoreCase(name)) {
            return executeRemoveOrphanFiles(db, tbl, invocation.arguments());
        }
        if (ACTION_REWRITE_DATA_FILES.equalsIgnoreCase(name)) {
            return ActionResult.failed(
                    "rewrite_data_files requires a BE compaction worker; not yet wired "
                            + "(tracked: M3-12-be-compaction-worker)");
        }
        return ActionResult.failed("Unknown iceberg action: " + name);
    }

    // -------------------------------------------------------------------- expire_snapshots

    @VisibleForTesting
    ActionResult executeExpireSnapshots(String db, String tbl, Map<String, Object> args) {
        Long olderThan = (Long) args.get("older_than");
        Long retainLast = (Long) args.get("retain_last");
        String snapshotIdsRaw = (String) args.get("snapshot_ids");
        Boolean cleanExpiredFiles = (Boolean) args.get("clean_expired_files");
        Boolean dryRun = (Boolean) args.get("dry_run");

        List<Long> snapshotIds = parseSnapshotIds(snapshotIdsRaw);
        if (olderThan == null && retainLast == null && snapshotIds.isEmpty()) {
            return ActionResult.failed(
                    "expire_snapshots requires at least one of "
                            + "'older_than', 'retain_last', 'snapshot_ids'");
        }
        if (retainLast != null && retainLast <= 0) {
            return ActionResult.failed("'retain_last' must be > 0, got " + retainLast);
        }

        Table table;
        try {
            table = tableLoader.apply(db, tbl);
        } catch (RuntimeException e) {
            return ActionResult.failed("Failed to load iceberg table " + db + "." + tbl
                    + ": " + e.getMessage());
        }

        ExpireSnapshots op = table.expireSnapshots();
        if (olderThan != null) {
            op = op.expireOlderThan(olderThan);
        }
        if (retainLast != null) {
            op = op.retainLast(retainLast.intValue());
        }
        for (Long id : snapshotIds) {
            op = op.expireSnapshotId(id);
        }
        boolean cleanFiles = cleanExpiredFiles == null || cleanExpiredFiles;
        op = op.cleanExpiredFiles(cleanFiles);

        boolean isDryRun = dryRun != null && dryRun;
        List<Snapshot> expired;
        try {
            expired = op.apply();
        } catch (RuntimeException e) {
            return ActionResult.failed("expire_snapshots planning failed: " + e.getMessage());
        }

        List<Map<String, Object>> rows = new ArrayList<>();
        for (Snapshot s : expired) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("snapshot_id", s.snapshotId());
            row.put("action", isDryRun ? "WOULD_EXPIRE" : "EXPIRED");
            rows.add(row);
        }

        if (!isDryRun) {
            try {
                op.commit();
            } catch (RuntimeException e) {
                return ActionResult.failed("expire_snapshots commit failed: " + e.getMessage());
            }
        }
        return ActionResult.completed(rows);
    }

    private static List<Long> parseSnapshotIds(String raw) {
        if (raw == null || raw.isBlank()) {
            return Collections.emptyList();
        }
        List<Long> ids = new ArrayList<>();
        for (String part : raw.split(",")) {
            String trimmed = part.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            try {
                ids.add(Long.parseLong(trimmed));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "snapshot_ids: invalid id '" + trimmed + "'", e);
            }
        }
        return ids;
    }

    // -------------------------------------------------------------------- remove_orphan_files

    @VisibleForTesting
    ActionResult executeRemoveOrphanFiles(String db, String tbl, Map<String, Object> args) {
        Long olderThan = (Long) args.get("older_than");
        String location = (String) args.get("location");
        Boolean dryRun = (Boolean) args.get("dry_run");
        long threshold = olderThan != null
                ? olderThan
                : System.currentTimeMillis() - DEFAULT_ORPHAN_RETENTION_MS;
        boolean isDryRun = dryRun == null || dryRun;

        Table table;
        try {
            table = tableLoader.apply(db, tbl);
        } catch (RuntimeException e) {
            return ActionResult.failed("Failed to load iceberg table " + db + "." + tbl
                    + ": " + e.getMessage());
        }
        String prefix = location != null && !location.isBlank() ? location : table.location();
        FileIO io = table.io();
        if (!(io instanceof SupportsPrefixOperations)) {
            return ActionResult.failed(
                    "remove_orphan_files: FileIO " + io.getClass().getName()
                            + " does not implement SupportsPrefixOperations; "
                            + "configure a prefix-capable FileIO (e.g. HadoopFileIO, S3FileIO)");
        }
        SupportsPrefixOperations prefixIo = (SupportsPrefixOperations) io;

        Set<String> reachable;
        try {
            reachable = collectReachablePaths(table, io);
        } catch (RuntimeException | IOException e) {
            return ActionResult.failed("remove_orphan_files: failed to enumerate "
                    + "reachable files: " + e.getMessage());
        }

        List<Map<String, Object>> rows = new ArrayList<>();
        try {
            for (FileInfo info : prefixIo.listPrefix(prefix)) {
                if (info.createdAtMillis() >= threshold) {
                    continue;
                }
                String norm = normalizePath(info.location());
                if (reachable.contains(norm)) {
                    continue;
                }
                String act;
                if (isDryRun) {
                    act = "WOULD_DELETE";
                } else {
                    try {
                        io.deleteFile(info.location());
                        act = "DELETED";
                    } catch (RuntimeException e) {
                        LOG.warn("remove_orphan_files: deleteFile failed for {}: {}",
                                info.location(), e.getMessage());
                        act = "DELETE_FAILED:" + e.getMessage();
                    }
                }
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("orphan_file_path", info.location());
                row.put("action", act);
                rows.add(row);
            }
        } catch (RuntimeException e) {
            return ActionResult.failed("remove_orphan_files: listPrefix failed for '"
                    + prefix + "': " + e.getMessage());
        }
        return ActionResult.completed(rows);
    }

    @VisibleForTesting
    static Set<String> collectReachablePaths(Table table, FileIO io) throws IOException {
        Set<String> reachable = new HashSet<>();
        for (String p : ReachableFileUtil.metadataFileLocations(table, true)) {
            reachable.add(normalizePath(p));
        }
        for (String p : ReachableFileUtil.manifestListLocations(table)) {
            reachable.add(normalizePath(p));
        }
        for (String p : ReachableFileUtil.statisticsFilesLocations(table)) {
            reachable.add(normalizePath(p));
        }
        String hint = ReachableFileUtil.versionHintLocation(table);
        if (hint != null) {
            reachable.add(normalizePath(hint));
        }

        Map<Integer, PartitionSpec> specs = table.specs();
        for (Snapshot snapshot : table.snapshots()) {
            for (ManifestFile mf : snapshot.allManifests(io)) {
                reachable.add(normalizePath(mf.path()));
                if (mf.content() == ManifestContent.DATA) {
                    try (CloseableIterable<String> paths = ManifestFiles.readPaths(mf, io)) {
                        for (String p : paths) {
                            reachable.add(normalizePath(p));
                        }
                    }
                } else {
                    try (ManifestReader<DeleteFile> reader =
                                 ManifestFiles.readDeleteManifest(mf, io, specs)) {
                        for (DeleteFile df : reader) {
                            reachable.add(normalizePath(df.location()));
                        }
                    }
                }
            }
        }
        return reachable;
    }

    @VisibleForTesting
    static String normalizePath(String raw) {
        if (raw == null) {
            return null;
        }
        try {
            URI u = new URI(raw).normalize();
            String scheme = u.getScheme();
            String path = u.getPath();
            if (scheme == null) {
                return path == null ? raw : path;
            }
            return scheme.toLowerCase(Locale.ROOT) + "://"
                    + (u.getAuthority() == null ? "" : u.getAuthority())
                    + (path == null ? "" : path);
        } catch (URISyntaxException e) {
            return raw;
        }
    }

    // -------------------------------------------------------------------- descriptors

    private static ActionDescriptor buildExpireSnapshotsDescriptor() {
        return ActionDescriptor.builder()
                .name(ACTION_EXPIRE_SNAPSHOTS)
                .scope(Scope.TABLE)
                .addArgument(new ActionArgument("older_than", ActionArgumentType.LONG, false))
                .addArgument(new ActionArgument("retain_last", ActionArgumentType.LONG, false))
                .addArgument(new ActionArgument("snapshot_ids", ActionArgumentType.STRING, false))
                .addArgument(new ActionArgument(
                        "clean_expired_files", ActionArgumentType.BOOLEAN, false, "true"))
                .addArgument(new ActionArgument(
                        "dry_run", ActionArgumentType.BOOLEAN, false, "false"))
                .addResultColumn(new ActionDescriptor.ResultColumn(
                        "snapshot_id", ActionArgumentType.LONG))
                .addResultColumn(new ActionDescriptor.ResultColumn(
                        "action", ActionArgumentType.STRING))
                .transactional(true)
                .blocking(true)
                .idempotent(true)
                .build();
    }

    private static ActionDescriptor buildRemoveOrphanFilesDescriptor() {
        return ActionDescriptor.builder()
                .name(ACTION_REMOVE_ORPHAN_FILES)
                .scope(Scope.TABLE)
                .addArgument(new ActionArgument("older_than", ActionArgumentType.LONG, false))
                .addArgument(new ActionArgument("location", ActionArgumentType.STRING, false))
                .addArgument(new ActionArgument(
                        "dry_run", ActionArgumentType.BOOLEAN, false, "true"))
                .addResultColumn(new ActionDescriptor.ResultColumn(
                        "orphan_file_path", ActionArgumentType.STRING))
                .addResultColumn(new ActionDescriptor.ResultColumn(
                        "action", ActionArgumentType.STRING))
                .transactional(false)
                .blocking(true)
                .idempotent(true)
                .build();
    }

    private static ActionDescriptor buildRewriteDataFilesDescriptor() {
        return ActionDescriptor.builder()
                .name(ACTION_REWRITE_DATA_FILES)
                .scope(Scope.TABLE)
                .addArgument(new ActionArgument(
                        "partition_filter", ActionArgumentType.STRING, false))
                .addArgument(new ActionArgument(
                        "target_file_size_bytes", ActionArgumentType.LONG, false))
                .addResultColumn(new ActionDescriptor.ResultColumn(
                        "rewritten_data_files_count", ActionArgumentType.LONG))
                .addResultColumn(new ActionDescriptor.ResultColumn(
                        "added_data_files_count", ActionArgumentType.LONG))
                .transactional(true)
                .blocking(true)
                .idempotent(false)
                .build();
    }
}
