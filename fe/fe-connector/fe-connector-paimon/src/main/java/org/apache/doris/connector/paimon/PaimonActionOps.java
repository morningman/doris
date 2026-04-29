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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.api.action.ActionArgument;
import org.apache.doris.connector.api.action.ActionArgumentType;
import org.apache.doris.connector.api.action.ActionDescriptor;
import org.apache.doris.connector.api.action.ActionInvocation;
import org.apache.doris.connector.api.action.ActionResult;
import org.apache.doris.connector.api.action.ActionTarget;
import org.apache.doris.connector.api.action.ConnectorActionOps;

import com.google.common.annotations.VisibleForTesting;
import org.apache.paimon.Snapshot;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.BranchManager;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * Paimon implementation of {@link ConnectorActionOps}, exposing the standard
 * paimon table maintenance procedures via the SPI:
 *
 * <ul>
 *   <li>{@code create_tag} / {@code delete_tag} — wired against
 *       {@link Table#createTag(String, long)} (and overloads) and
 *       {@link Table#deleteTag(String)}.</li>
 *   <li>{@code create_branch} / {@code delete_branch} — wired against
 *       {@link Table#createBranch(String)} /
 *       {@link Table#createBranch(String, String)} (from a tag) and
 *       {@link Table#deleteBranch(String)}. Reserved branch name
 *       {@code main} is rejected up front (paimon also enforces this but
 *       a clear up-front message is friendlier).</li>
 *   <li>{@code compact} — descriptor only; execution requires a BE
 *       compaction worker that is not yet wired (mirrors iceberg
 *       {@code rewrite_data_files}; tracked by
 *       {@code TODO M3-12-be-compaction-worker}).</li>
 * </ul>
 *
 * <p>Paimon's native {@code Table.createBranch} accepts either no source
 * (current head) or a tag name; there is no {@code (branch, snapshotId)}
 * variant. The {@code create_branch} action therefore exposes only
 * {@code from_tag}; users wanting to branch from an arbitrary snapshot id
 * should first {@code create_tag} from that snapshot and then
 * {@code create_branch} from the tag. This mirrors the rationale already
 * documented in {@code PaimonRefOps}.</p>
 *
 * <p>Argument values arrive already type-coerced by the engine dispatcher
 * (see {@code PluginDrivenExecuteActionCommand}); optional arguments
 * without a default arrive as {@code null}.</p>
 */
public final class PaimonActionOps implements ConnectorActionOps {

    static final String ACTION_CREATE_TAG = "create_tag";
    static final String ACTION_DELETE_TAG = "delete_tag";
    static final String ACTION_CREATE_BRANCH = "create_branch";
    static final String ACTION_DELETE_BRANCH = "delete_branch";
    static final String ACTION_COMPACT = "compact";

    private static final List<ActionDescriptor> TABLE_ACTIONS = List.of(
            buildCreateTagDescriptor(),
            buildDeleteTagDescriptor(),
            buildCreateBranchDescriptor(),
            buildDeleteBranchDescriptor(),
            buildCompactDescriptor());

    private final BiFunction<String, String, Table> tableLoader;

    public PaimonActionOps(BiFunction<String, String, Table> tableLoader) {
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
            return ActionResult.failed("Paimon actions are only supported at TABLE scope");
        }
        ActionTarget target = invocation.target()
                .orElseThrow(() -> new IllegalArgumentException("invocation.target is required"));
        if (target.id().isEmpty()) {
            return ActionResult.failed("Paimon actions require a TABLE target");
        }
        String db = target.database();
        String tbl = target.id().get().table();
        String name = invocation.name();
        String lower = name.toLowerCase(Locale.ROOT);
        switch (lower) {
            case ACTION_CREATE_TAG:
                return executeCreateTag(db, tbl, invocation.arguments());
            case ACTION_DELETE_TAG:
                return executeDeleteTag(db, tbl, invocation.arguments());
            case ACTION_CREATE_BRANCH:
                return executeCreateBranch(db, tbl, invocation.arguments());
            case ACTION_DELETE_BRANCH:
                return executeDeleteBranch(db, tbl, invocation.arguments());
            case ACTION_COMPACT:
                return ActionResult.failed(
                        "compact requires a BE compaction worker; not yet wired "
                                + "(tracked: M3-12-be-compaction-worker)");
            default:
                return ActionResult.failed("Unknown paimon action: " + name);
        }
    }

    // -------------------------------------------------------------------- create_tag

    @VisibleForTesting
    ActionResult executeCreateTag(String db, String tbl, Map<String, Object> args) {
        String tagName = (String) args.get("tag");
        if (tagName == null || tagName.isBlank()) {
            return ActionResult.failed("create_tag: 'tag' is required and must be non-blank");
        }
        Long snapshotId = (Long) args.get("snapshot_id");
        if (snapshotId != null && snapshotId <= 0) {
            return ActionResult.failed("create_tag: 'snapshot_id' must be > 0, got " + snapshotId);
        }
        Long retentionHours = (Long) args.get("time_retained_hours");
        if (retentionHours != null && retentionHours <= 0) {
            return ActionResult.failed(
                    "create_tag: 'time_retained_hours' must be > 0, got " + retentionHours);
        }
        Duration retention = retentionHours == null ? null : Duration.ofHours(retentionHours);

        Table table = loadOrFail(db, tbl);
        if (table == null) {
            return ActionResult.failed("Failed to load paimon table " + db + "." + tbl);
        }

        long resolvedSnapshot;
        try {
            if (snapshotId != null) {
                if (retention != null) {
                    table.createTag(tagName, snapshotId, retention);
                } else {
                    table.createTag(tagName, snapshotId);
                }
                resolvedSnapshot = snapshotId;
            } else {
                if (retention != null) {
                    table.createTag(tagName, retention);
                } else {
                    table.createTag(tagName);
                }
                Optional<Snapshot> latest = table.latestSnapshot();
                resolvedSnapshot = latest.map(Snapshot::id).orElse(-1L);
            }
        } catch (RuntimeException e) {
            return ActionResult.failed("create_tag failed for " + db + "." + tbl
                    + " tag='" + tagName + "': " + e.getMessage());
        }

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("tag", tagName);
        row.put("snapshot_id", resolvedSnapshot);
        row.put("action", "CREATED");
        return ActionResult.completed(List.of(row));
    }

    // -------------------------------------------------------------------- delete_tag

    @VisibleForTesting
    ActionResult executeDeleteTag(String db, String tbl, Map<String, Object> args) {
        String tagName = (String) args.get("tag");
        if (tagName == null || tagName.isBlank()) {
            return ActionResult.failed("delete_tag: 'tag' is required and must be non-blank");
        }
        Table table = loadOrFail(db, tbl);
        if (table == null) {
            return ActionResult.failed("Failed to load paimon table " + db + "." + tbl);
        }
        try {
            table.deleteTag(tagName);
        } catch (RuntimeException e) {
            return ActionResult.failed("delete_tag failed for " + db + "." + tbl
                    + " tag='" + tagName + "': " + e.getMessage());
        }
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("tag", tagName);
        row.put("action", "DELETED");
        return ActionResult.completed(List.of(row));
    }

    // -------------------------------------------------------------------- create_branch

    @VisibleForTesting
    ActionResult executeCreateBranch(String db, String tbl, Map<String, Object> args) {
        String branch = (String) args.get("branch");
        if (branch == null || branch.isBlank()) {
            return ActionResult.failed("create_branch: 'branch' is required and must be non-blank");
        }
        if (BranchManager.isMainBranch(branch)) {
            return ActionResult.failed(
                    "create_branch: '" + branch + "' is the reserved main branch name");
        }
        String fromTag = (String) args.get("from_tag");
        if (fromTag != null && fromTag.isBlank()) {
            fromTag = null;
        }

        Table table = loadOrFail(db, tbl);
        if (table == null) {
            return ActionResult.failed("Failed to load paimon table " + db + "." + tbl);
        }
        try {
            if (fromTag != null) {
                table.createBranch(branch, fromTag);
            } else {
                table.createBranch(branch);
            }
        } catch (RuntimeException e) {
            return ActionResult.failed("create_branch failed for " + db + "." + tbl
                    + " branch='" + branch + "': " + e.getMessage());
        }
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("branch", branch);
        row.put("from_tag", fromTag == null ? "" : fromTag);
        row.put("action", "CREATED");
        return ActionResult.completed(List.of(row));
    }

    // -------------------------------------------------------------------- delete_branch

    @VisibleForTesting
    ActionResult executeDeleteBranch(String db, String tbl, Map<String, Object> args) {
        String branch = (String) args.get("branch");
        if (branch == null || branch.isBlank()) {
            return ActionResult.failed("delete_branch: 'branch' is required and must be non-blank");
        }
        if (BranchManager.isMainBranch(branch)) {
            return ActionResult.failed(
                    "delete_branch: refusing to delete the reserved main branch");
        }
        Table table = loadOrFail(db, tbl);
        if (table == null) {
            return ActionResult.failed("Failed to load paimon table " + db + "." + tbl);
        }
        try {
            table.deleteBranch(branch);
        } catch (RuntimeException e) {
            return ActionResult.failed("delete_branch failed for " + db + "." + tbl
                    + " branch='" + branch + "': " + e.getMessage());
        }
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("branch", branch);
        row.put("action", "DELETED");
        return ActionResult.completed(List.of(row));
    }

    // -------------------------------------------------------------------- helpers

    private Table loadOrFail(String db, String tbl) {
        try {
            return tableLoader.apply(db, tbl);
        } catch (RuntimeException e) {
            return null;
        }
    }

    // -------------------------------------------------------------------- descriptors

    private static ActionDescriptor buildCreateTagDescriptor() {
        return ActionDescriptor.builder()
                .name(ACTION_CREATE_TAG)
                .scope(Scope.TABLE)
                .addArgument(new ActionArgument("tag", ActionArgumentType.STRING, true))
                .addArgument(new ActionArgument("snapshot_id", ActionArgumentType.LONG, false))
                .addArgument(new ActionArgument(
                        "time_retained_hours", ActionArgumentType.LONG, false))
                .addResultColumn(new ActionDescriptor.ResultColumn(
                        "tag", ActionArgumentType.STRING))
                .addResultColumn(new ActionDescriptor.ResultColumn(
                        "snapshot_id", ActionArgumentType.LONG))
                .addResultColumn(new ActionDescriptor.ResultColumn(
                        "action", ActionArgumentType.STRING))
                .transactional(true)
                .blocking(true)
                .idempotent(false)
                .build();
    }

    private static ActionDescriptor buildDeleteTagDescriptor() {
        return ActionDescriptor.builder()
                .name(ACTION_DELETE_TAG)
                .scope(Scope.TABLE)
                .addArgument(new ActionArgument("tag", ActionArgumentType.STRING, true))
                .addResultColumn(new ActionDescriptor.ResultColumn(
                        "tag", ActionArgumentType.STRING))
                .addResultColumn(new ActionDescriptor.ResultColumn(
                        "action", ActionArgumentType.STRING))
                .transactional(true)
                .blocking(true)
                .idempotent(true)
                .build();
    }

    private static ActionDescriptor buildCreateBranchDescriptor() {
        return ActionDescriptor.builder()
                .name(ACTION_CREATE_BRANCH)
                .scope(Scope.TABLE)
                .addArgument(new ActionArgument("branch", ActionArgumentType.STRING, true))
                .addArgument(new ActionArgument("from_tag", ActionArgumentType.STRING, false))
                .addResultColumn(new ActionDescriptor.ResultColumn(
                        "branch", ActionArgumentType.STRING))
                .addResultColumn(new ActionDescriptor.ResultColumn(
                        "from_tag", ActionArgumentType.STRING))
                .addResultColumn(new ActionDescriptor.ResultColumn(
                        "action", ActionArgumentType.STRING))
                .transactional(true)
                .blocking(true)
                .idempotent(false)
                .build();
    }

    private static ActionDescriptor buildDeleteBranchDescriptor() {
        return ActionDescriptor.builder()
                .name(ACTION_DELETE_BRANCH)
                .scope(Scope.TABLE)
                .addArgument(new ActionArgument("branch", ActionArgumentType.STRING, true))
                .addResultColumn(new ActionDescriptor.ResultColumn(
                        "branch", ActionArgumentType.STRING))
                .addResultColumn(new ActionDescriptor.ResultColumn(
                        "action", ActionArgumentType.STRING))
                .transactional(true)
                .blocking(true)
                .idempotent(true)
                .build();
    }

    private static ActionDescriptor buildCompactDescriptor() {
        return ActionDescriptor.builder()
                .name(ACTION_COMPACT)
                .scope(Scope.TABLE)
                .addArgument(new ActionArgument("partition", ActionArgumentType.STRING, false))
                .addArgument(new ActionArgument("where", ActionArgumentType.STRING, false))
                .addArgument(new ActionArgument(
                        "order_strategy", ActionArgumentType.STRING, false))
                .addResultColumn(new ActionDescriptor.ResultColumn(
                        "partition", ActionArgumentType.STRING))
                .addResultColumn(new ActionDescriptor.ResultColumn(
                        "action", ActionArgumentType.STRING))
                .transactional(true)
                .blocking(true)
                .idempotent(false)
                .build();
    }
}
