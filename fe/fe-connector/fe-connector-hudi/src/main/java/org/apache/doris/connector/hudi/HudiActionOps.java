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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.api.action.ActionArgument;
import org.apache.doris.connector.api.action.ActionArgumentType;
import org.apache.doris.connector.api.action.ActionDescriptor;
import org.apache.doris.connector.api.action.ActionInvocation;
import org.apache.doris.connector.api.action.ActionResult;
import org.apache.doris.connector.api.action.ActionTarget;
import org.apache.doris.connector.api.action.ConnectorActionOps;

import com.google.common.annotations.VisibleForTesting;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Hudi implementation of {@link ConnectorActionOps}, exposing the standard
 * Hudi maintenance procedures as catalog-qualified actions:
 *
 * <ul>
 *   <li>{@code clean} — registers the descriptor (retain_commits, dry_run,
 *       target_partitions) so {@code SHOW PROCEDURES} and the
 *       {@code CALL} dispatcher (M3-10) enumerate it accurately. The
 *       {@code fe-connector-hudi} module currently depends only on
 *       {@code hudi-common} + {@code hudi-hadoop-mr}; no
 *       {@code HoodieJavaWriteClient} / engine is on the classpath, so
 *       execution returns {@link ActionResult.Lifecycle#FAILED} after
 *       argument validation. Tracked by {@code TODO M3-13-be-hudi-engine-worker}.</li>
 *   <li>{@code compaction} — descriptor only; compaction is heavier than
 *       clean (needs file-group rewriting compute) and is gated on the
 *       same BE worker. Always returns {@code FAILED} for now.</li>
 * </ul>
 *
 * <p>The {@code archive} / {@code rollback} hudi procedures are intentionally
 * not exposed in this milestone — the M3-13 plan entry scopes the surface to
 * {@code clean} / {@code compaction}, and adding more descriptors without
 * matching execution paths would be misleading to {@code SHOW PROCEDURES}
 * consumers.</p>
 *
 * <p>Argument values arrive already type-coerced by the engine dispatcher
 * (see {@code PluginDrivenExecuteActionCommand}); optional arguments without
 * a default arrive as {@code null}.</p>
 */
public final class HudiActionOps implements ConnectorActionOps {

    static final String ACTION_CLEAN = "clean";
    static final String ACTION_COMPACTION = "compaction";

    /** Stable error message fragment used by tests + future debugging. */
    static final String NO_ENGINE_MESSAGE =
            "requires a BE hudi engine worker; not yet wired "
                    + "(tracked: M3-13-be-hudi-engine-worker)";

    private static final List<ActionDescriptor> TABLE_ACTIONS = List.of(
            buildCleanDescriptor(),
            buildCompactionDescriptor());

    public HudiActionOps() {
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
            return ActionResult.failed("Hudi actions are only supported at TABLE scope");
        }
        ActionTarget target = invocation.target()
                .orElseThrow(() -> new IllegalArgumentException("invocation.target is required"));
        if (target.id().isEmpty()) {
            return ActionResult.failed("Hudi actions require a TABLE target");
        }
        String db = target.database();
        String tbl = target.id().get().table();
        String name = invocation.name();
        String lower = name.toLowerCase(Locale.ROOT);
        switch (lower) {
            case ACTION_CLEAN:
                return executeClean(db, tbl, invocation.arguments());
            case ACTION_COMPACTION:
                return executeCompaction(db, tbl, invocation.arguments());
            default:
                return ActionResult.failed("Unknown hudi action: " + name);
        }
    }

    // -------------------------------------------------------------------- clean

    @VisibleForTesting
    ActionResult executeClean(String db, String tbl, Map<String, Object> args) {
        Long retainCommits = (Long) args.get("retain_commits");
        if (retainCommits != null && retainCommits < 0) {
            return ActionResult.failed(
                    "clean: 'retain_commits' must be >= 0, got " + retainCommits);
        }
        // Boolean arrives as Boolean; null means default-applied at the engine layer.
        Object dryRunRaw = args.get("dry_run");
        if (dryRunRaw != null && !(dryRunRaw instanceof Boolean)) {
            return ActionResult.failed(
                    "clean: 'dry_run' must be BOOLEAN, got "
                            + dryRunRaw.getClass().getSimpleName());
        }
        Object partitionsRaw = args.get("target_partitions");
        if (partitionsRaw != null && !(partitionsRaw instanceof String)) {
            return ActionResult.failed(
                    "clean: 'target_partitions' must be STRING, got "
                            + partitionsRaw.getClass().getSimpleName());
        }
        return ActionResult.failed(
                "clean for " + db + "." + tbl + ": " + NO_ENGINE_MESSAGE);
    }

    // -------------------------------------------------------------------- compaction

    @VisibleForTesting
    ActionResult executeCompaction(String db, String tbl, Map<String, Object> args) {
        Object instantRaw = args.get("instant");
        if (instantRaw != null && !(instantRaw instanceof String)) {
            return ActionResult.failed(
                    "compaction: 'instant' must be STRING, got "
                            + instantRaw.getClass().getSimpleName());
        }
        Object dryRunRaw = args.get("dry_run");
        if (dryRunRaw != null && !(dryRunRaw instanceof Boolean)) {
            return ActionResult.failed(
                    "compaction: 'dry_run' must be BOOLEAN, got "
                            + dryRunRaw.getClass().getSimpleName());
        }
        return ActionResult.failed(
                "compaction for " + db + "." + tbl + ": " + NO_ENGINE_MESSAGE);
    }

    // -------------------------------------------------------------------- descriptors

    private static ActionDescriptor buildCleanDescriptor() {
        return ActionDescriptor.builder()
                .name(ACTION_CLEAN)
                .scope(Scope.TABLE)
                .addArgument(new ActionArgument("retain_commits", ActionArgumentType.LONG, false))
                .addArgument(new ActionArgument(
                        "dry_run", ActionArgumentType.BOOLEAN, false, "true"))
                .addArgument(new ActionArgument(
                        "target_partitions", ActionArgumentType.STRING, false))
                .addResultColumn(new ActionDescriptor.ResultColumn(
                        "commit_time", ActionArgumentType.STRING))
                .addResultColumn(new ActionDescriptor.ResultColumn(
                        "deleted_files", ActionArgumentType.LONG))
                .addResultColumn(new ActionDescriptor.ResultColumn(
                        "partitions_cleaned", ActionArgumentType.LONG))
                .addResultColumn(new ActionDescriptor.ResultColumn(
                        "action", ActionArgumentType.STRING))
                .transactional(true)
                .blocking(true)
                .idempotent(false)
                .build();
    }

    private static ActionDescriptor buildCompactionDescriptor() {
        return ActionDescriptor.builder()
                .name(ACTION_COMPACTION)
                .scope(Scope.TABLE)
                .addArgument(new ActionArgument("instant", ActionArgumentType.STRING, false))
                .addArgument(new ActionArgument(
                        "dry_run", ActionArgumentType.BOOLEAN, false, "true"))
                .addResultColumn(new ActionDescriptor.ResultColumn(
                        "instant", ActionArgumentType.STRING))
                .addResultColumn(new ActionDescriptor.ResultColumn(
                        "file_groups_compacted", ActionArgumentType.LONG))
                .addResultColumn(new ActionDescriptor.ResultColumn(
                        "action", ActionArgumentType.STRING))
                .transactional(true)
                .blocking(true)
                .idempotent(false)
                .build();
    }
}
