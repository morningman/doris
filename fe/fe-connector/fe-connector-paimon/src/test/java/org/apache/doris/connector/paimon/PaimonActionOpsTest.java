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

import org.apache.doris.connector.api.ConnectorTableId;
import org.apache.doris.connector.api.action.ActionArgument;
import org.apache.doris.connector.api.action.ActionArgumentType;
import org.apache.doris.connector.api.action.ActionDescriptor;
import org.apache.doris.connector.api.action.ActionInvocation;
import org.apache.doris.connector.api.action.ActionResult;
import org.apache.doris.connector.api.action.ActionTarget;
import org.apache.doris.connector.api.action.ConnectorActionOps;

import org.apache.paimon.Snapshot;
import org.apache.paimon.table.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

class PaimonActionOpsTest {

    // ---- helpers ----

    private static PaimonActionOps newOps(Table table) {
        BiFunction<String, String, Table> loader = (db, tbl) -> table;
        return new PaimonActionOps(loader);
    }

    private static PaimonActionOps newOpsLoadFails(RuntimeException error) {
        return new PaimonActionOps((db, tbl) -> {
            throw error;
        });
    }

    private static ActionInvocation newInvocation(String name, ConnectorActionOps.Scope scope,
                                                  ActionTarget target, Map<String, Object> args) {
        ActionInvocation.Builder b = ActionInvocation.builder()
                .name(name)
                .scope(scope)
                .arguments(args);
        if (target != null) {
            b.target(target);
        }
        return b.build();
    }

    private static ActionTarget tableTarget() {
        return ActionTarget.ofTable(ConnectorTableId.of("db", "t"));
    }

    private static ActionDescriptor findDescriptor(List<ActionDescriptor> all, String name) {
        return all.stream()
                .filter(d -> d.name().equals(name))
                .findFirst()
                .orElseThrow();
    }

    // ---- listActions ----

    @Test
    void listActionsTableScopeReturnsFiveDescriptors() {
        PaimonActionOps ops = newOps(Mockito.mock(Table.class));
        List<ActionDescriptor> descriptors = ops.listActions(
                ConnectorActionOps.Scope.TABLE, Optional.empty());
        Set<String> names = descriptors.stream()
                .map(ActionDescriptor::name)
                .collect(Collectors.toSet());
        Assertions.assertEquals(
                Set.of("create_tag", "delete_tag", "create_branch",
                        "delete_branch", "compact"),
                names);
        ActionDescriptor createTag = findDescriptor(descriptors, "create_tag");
        Assertions.assertEquals(ConnectorActionOps.Scope.TABLE, createTag.scope());
        Set<String> argNames = createTag.arguments().stream()
                .map(ActionArgument::name)
                .collect(Collectors.toSet());
        Assertions.assertEquals(
                Set.of("tag", "snapshot_id", "time_retained_hours"), argNames);
        ActionArgument tagArg = createTag.arguments().stream()
                .filter(a -> a.name().equals("tag"))
                .findFirst().orElseThrow();
        Assertions.assertTrue(tagArg.required());
        Assertions.assertEquals(ActionArgumentType.STRING, tagArg.type());
        Assertions.assertEquals(3, createTag.resultSchema().size());
        Assertions.assertEquals("snapshot_id", createTag.resultSchema().get(1).name());
        Assertions.assertEquals(ActionArgumentType.LONG,
                createTag.resultSchema().get(1).type());
    }

    @Test
    void listActionsCatalogAndSchemaScopesAreEmpty() {
        PaimonActionOps ops = newOps(Mockito.mock(Table.class));
        Assertions.assertTrue(ops.listActions(
                ConnectorActionOps.Scope.CATALOG, Optional.empty()).isEmpty());
        Assertions.assertTrue(ops.listActions(
                ConnectorActionOps.Scope.SCHEMA,
                Optional.of(ActionTarget.ofSchema("db"))).isEmpty());
    }

    @Test
    void createBranchDescriptorAdvertisesOnlyFromTag() {
        PaimonActionOps ops = newOps(Mockito.mock(Table.class));
        ActionDescriptor d = findDescriptor(
                ops.listActions(ConnectorActionOps.Scope.TABLE, Optional.empty()),
                "create_branch");
        Set<String> argNames = d.arguments().stream()
                .map(ActionArgument::name)
                .collect(Collectors.toSet());
        // Paimon's native createBranch has no (branch, snapshotId) variant;
        // 'from_snapshot_id' is intentionally not advertised.
        Assertions.assertEquals(Set.of("branch", "from_tag"), argNames);
    }

    // ---- create_tag ----

    @Test
    void createTagWithSnapshotIdInvokesTwoArgVariant() {
        Table table = Mockito.mock(Table.class);
        PaimonActionOps ops = newOps(table);
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("tag", "T1");
        args.put("snapshot_id", 7L);
        args.put("time_retained_hours", null);

        ActionResult r = ops.executeCreateTag("db", "t", args);

        Assertions.assertEquals(ActionResult.Lifecycle.COMPLETED, r.lifecycle());
        Mockito.verify(table).createTag("T1", 7L);
        Mockito.verify(table, Mockito.never()).createTag(Mockito.eq("T1"));
        Assertions.assertEquals(1, r.rows().size());
        Map<String, Object> row = r.rows().get(0);
        Assertions.assertEquals("T1", row.get("tag"));
        Assertions.assertEquals(7L, row.get("snapshot_id"));
        Assertions.assertEquals("CREATED", row.get("action"));
    }

    @Test
    void createTagWithoutSnapshotIdUsesCurrentSnapshot() {
        Table table = Mockito.mock(Table.class);
        Snapshot snap = Mockito.mock(Snapshot.class);
        Mockito.when(snap.id()).thenReturn(42L);
        Mockito.when(table.latestSnapshot()).thenReturn(Optional.of(snap));
        PaimonActionOps ops = newOps(table);

        Map<String, Object> args = new LinkedHashMap<>();
        args.put("tag", "T2");

        ActionResult r = ops.executeCreateTag("db", "t", args);

        Assertions.assertEquals(ActionResult.Lifecycle.COMPLETED, r.lifecycle());
        Mockito.verify(table).createTag("T2");
        Mockito.verify(table, Mockito.never()).createTag(Mockito.anyString(), Mockito.anyLong());
        Assertions.assertEquals(42L, r.rows().get(0).get("snapshot_id"));
    }

    @Test
    void createTagWithRetentionAndSnapshotInvokesThreeArgVariant() {
        Table table = Mockito.mock(Table.class);
        PaimonActionOps ops = newOps(table);
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("tag", "T3");
        args.put("snapshot_id", 10L);
        args.put("time_retained_hours", 24L);

        ActionResult r = ops.executeCreateTag("db", "t", args);

        Assertions.assertEquals(ActionResult.Lifecycle.COMPLETED, r.lifecycle());
        Mockito.verify(table).createTag("T3", 10L, Duration.ofHours(24));
    }

    @Test
    void createTagWithRetentionNoSnapshotInvokesNameDurationVariant() {
        Table table = Mockito.mock(Table.class);
        Snapshot snap = Mockito.mock(Snapshot.class);
        Mockito.when(snap.id()).thenReturn(5L);
        Mockito.when(table.latestSnapshot()).thenReturn(Optional.of(snap));
        PaimonActionOps ops = newOps(table);
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("tag", "T4");
        args.put("time_retained_hours", 12L);

        ActionResult r = ops.executeCreateTag("db", "t", args);

        Assertions.assertEquals(ActionResult.Lifecycle.COMPLETED, r.lifecycle());
        Mockito.verify(table).createTag("T4", Duration.ofHours(12));
        Assertions.assertEquals(5L, r.rows().get(0).get("snapshot_id"));
    }

    @Test
    void createTagMissingTagFails() {
        Table table = Mockito.mock(Table.class);
        PaimonActionOps ops = newOps(table);
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("tag", null);

        ActionResult r = ops.executeCreateTag("db", "t", args);

        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        Assertions.assertTrue(r.errorMessage().orElse("").contains("'tag' is required"),
                r.errorMessage().orElse(""));
        Mockito.verifyNoInteractions(table);
    }

    @Test
    void createTagBlankTagFails() {
        Table table = Mockito.mock(Table.class);
        PaimonActionOps ops = newOps(table);
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("tag", "  ");

        ActionResult r = ops.executeCreateTag("db", "t", args);

        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        Mockito.verifyNoInteractions(table);
    }

    @Test
    void createTagNonPositiveSnapshotFails() {
        Table table = Mockito.mock(Table.class);
        PaimonActionOps ops = newOps(table);
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("tag", "T");
        args.put("snapshot_id", 0L);

        ActionResult r = ops.executeCreateTag("db", "t", args);

        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        Assertions.assertTrue(r.errorMessage().orElse("").contains("snapshot_id"));
        Mockito.verifyNoInteractions(table);
    }

    @Test
    void createTagDuplicateSurfacesPaimonError() {
        Table table = Mockito.mock(Table.class);
        Mockito.doThrow(new IllegalArgumentException("Tag T already exists"))
                .when(table).createTag("T", 3L);
        PaimonActionOps ops = newOps(table);
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("tag", "T");
        args.put("snapshot_id", 3L);

        ActionResult r = ops.executeCreateTag("db", "t", args);

        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        Assertions.assertTrue(r.errorMessage().orElse("").contains("already exists"));
    }

    @Test
    void createTagFailedTableLoadReportedClearly() {
        PaimonActionOps ops = newOpsLoadFails(new RuntimeException("boom"));
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("tag", "T");

        ActionResult r = ops.executeCreateTag("db", "t", args);

        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        Assertions.assertTrue(r.errorMessage().orElse("").contains("Failed to load"));
    }

    // ---- delete_tag ----

    @Test
    void deleteTagHappyPath() {
        Table table = Mockito.mock(Table.class);
        PaimonActionOps ops = newOps(table);
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("tag", "T");

        ActionResult r = ops.executeDeleteTag("db", "t", args);

        Assertions.assertEquals(ActionResult.Lifecycle.COMPLETED, r.lifecycle());
        Mockito.verify(table).deleteTag("T");
        Assertions.assertEquals("DELETED", r.rows().get(0).get("action"));
        Assertions.assertEquals("T", r.rows().get(0).get("tag"));
    }

    @Test
    void deleteTagMissingArgFails() {
        Table table = Mockito.mock(Table.class);
        PaimonActionOps ops = newOps(table);
        ActionResult r = ops.executeDeleteTag("db", "t", new LinkedHashMap<>());
        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        Mockito.verifyNoInteractions(table);
    }

    @Test
    void deleteTagUnknownSurfacesPaimonError() {
        Table table = Mockito.mock(Table.class);
        Mockito.doThrow(new IllegalArgumentException("Tag missing does not exist"))
                .when(table).deleteTag("missing");
        PaimonActionOps ops = newOps(table);
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("tag", "missing");

        ActionResult r = ops.executeDeleteTag("db", "t", args);

        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        Assertions.assertTrue(r.errorMessage().orElse("").contains("does not exist"));
    }

    // ---- create_branch ----

    @Test
    void createBranchFromTagInvokesTwoArgVariant() {
        Table table = Mockito.mock(Table.class);
        PaimonActionOps ops = newOps(table);
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("branch", "feature");
        args.put("from_tag", "rc1");

        ActionResult r = ops.executeCreateBranch("db", "t", args);

        Assertions.assertEquals(ActionResult.Lifecycle.COMPLETED, r.lifecycle());
        Mockito.verify(table).createBranch("feature", "rc1");
        Mockito.verify(table, Mockito.never()).createBranch(Mockito.eq("feature"));
        Assertions.assertEquals("rc1", r.rows().get(0).get("from_tag"));
    }

    @Test
    void createBranchWithoutSourceUsesCurrentHead() {
        Table table = Mockito.mock(Table.class);
        PaimonActionOps ops = newOps(table);
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("branch", "feature");

        ActionResult r = ops.executeCreateBranch("db", "t", args);

        Assertions.assertEquals(ActionResult.Lifecycle.COMPLETED, r.lifecycle());
        Mockito.verify(table).createBranch("feature");
        Mockito.verify(table, Mockito.never())
                .createBranch(Mockito.anyString(), Mockito.anyString());
        Assertions.assertEquals("", r.rows().get(0).get("from_tag"));
    }

    @Test
    void createBranchBlankFromTagTreatedAsAbsent() {
        Table table = Mockito.mock(Table.class);
        PaimonActionOps ops = newOps(table);
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("branch", "feature");
        args.put("from_tag", "   ");

        ActionResult r = ops.executeCreateBranch("db", "t", args);

        Assertions.assertEquals(ActionResult.Lifecycle.COMPLETED, r.lifecycle());
        Mockito.verify(table).createBranch("feature");
    }

    @Test
    void createBranchMissingNameFails() {
        Table table = Mockito.mock(Table.class);
        PaimonActionOps ops = newOps(table);
        ActionResult r = ops.executeCreateBranch("db", "t", new LinkedHashMap<>());
        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        Assertions.assertTrue(r.errorMessage().orElse("").contains("'branch' is required"));
        Mockito.verifyNoInteractions(table);
    }

    @Test
    void createBranchReservedMainNameFails() {
        Table table = Mockito.mock(Table.class);
        PaimonActionOps ops = newOps(table);
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("branch", "main");

        ActionResult r = ops.executeCreateBranch("db", "t", args);

        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        Assertions.assertTrue(r.errorMessage().orElse("").contains("main"));
        Mockito.verifyNoInteractions(table);
    }

    @Test
    void createBranchUnderlyingErrorSurfaced() {
        Table table = Mockito.mock(Table.class);
        Mockito.doThrow(new RuntimeException("tag rc1 not found"))
                .when(table).createBranch("feature", "rc1");
        PaimonActionOps ops = newOps(table);
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("branch", "feature");
        args.put("from_tag", "rc1");

        ActionResult r = ops.executeCreateBranch("db", "t", args);

        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        Assertions.assertTrue(r.errorMessage().orElse("").contains("not found"));
    }

    // ---- delete_branch ----

    @Test
    void deleteBranchHappyPath() {
        Table table = Mockito.mock(Table.class);
        PaimonActionOps ops = newOps(table);
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("branch", "feature");

        ActionResult r = ops.executeDeleteBranch("db", "t", args);

        Assertions.assertEquals(ActionResult.Lifecycle.COMPLETED, r.lifecycle());
        Mockito.verify(table).deleteBranch("feature");
        Assertions.assertEquals("DELETED", r.rows().get(0).get("action"));
    }

    @Test
    void deleteBranchMainRefused() {
        Table table = Mockito.mock(Table.class);
        PaimonActionOps ops = newOps(table);
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("branch", "main");

        ActionResult r = ops.executeDeleteBranch("db", "t", args);

        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        Assertions.assertTrue(r.errorMessage().orElse("").contains("main"));
        Mockito.verifyNoInteractions(table);
    }

    @Test
    void deleteBranchMissingArgFails() {
        Table table = Mockito.mock(Table.class);
        PaimonActionOps ops = newOps(table);
        ActionResult r = ops.executeDeleteBranch("db", "t", new LinkedHashMap<>());
        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        Mockito.verifyNoInteractions(table);
    }

    // ---- compact (descriptor only) ----

    @Test
    void compactReturnsFailedWithBeWorkerMessage() {
        Table table = Mockito.mock(Table.class);
        PaimonActionOps ops = newOps(table);
        ActionInvocation inv = newInvocation(
                "compact", ConnectorActionOps.Scope.TABLE, tableTarget(),
                new LinkedHashMap<>());

        ActionResult r = ops.executeAction(inv);

        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        Assertions.assertTrue(
                r.errorMessage().orElse("").contains("BE compaction worker"));
        Assertions.assertTrue(
                r.errorMessage().orElse("").contains("M3-12-be-compaction-worker"));
    }

    @Test
    void compactDescriptorPublishesPartitionWhereOrderStrategyArgs() {
        PaimonActionOps ops = newOps(Mockito.mock(Table.class));
        ActionDescriptor d = findDescriptor(
                ops.listActions(ConnectorActionOps.Scope.TABLE, Optional.empty()),
                "compact");
        Set<String> argNames = d.arguments().stream()
                .map(ActionArgument::name)
                .collect(Collectors.toSet());
        Assertions.assertEquals(
                Set.of("partition", "where", "order_strategy"), argNames);
    }

    // ---- executeAction dispatch ----

    @Test
    void executeActionDispatchesByCaseInsensitiveName() {
        Table table = Mockito.mock(Table.class);
        PaimonActionOps ops = newOps(table);
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("tag", "T");
        ActionInvocation inv = newInvocation(
                "DELETE_TAG", ConnectorActionOps.Scope.TABLE, tableTarget(), args);

        ActionResult r = ops.executeAction(inv);

        Assertions.assertEquals(ActionResult.Lifecycle.COMPLETED, r.lifecycle());
        Mockito.verify(table).deleteTag("T");
    }

    @Test
    void executeActionUnknownNameFails() {
        Table table = Mockito.mock(Table.class);
        PaimonActionOps ops = newOps(table);
        ActionInvocation inv = newInvocation(
                "no_such_action", ConnectorActionOps.Scope.TABLE, tableTarget(),
                new LinkedHashMap<>());

        ActionResult r = ops.executeAction(inv);

        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        Assertions.assertTrue(r.errorMessage().orElse("").contains("Unknown paimon action"));
    }

    @Test
    void executeActionNonTableScopeFails() {
        PaimonActionOps ops = newOps(Mockito.mock(Table.class));
        ActionInvocation inv = newInvocation(
                "create_tag", ConnectorActionOps.Scope.SCHEMA,
                ActionTarget.ofSchema("db"), new LinkedHashMap<>());

        ActionResult r = ops.executeAction(inv);

        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        Assertions.assertTrue(r.errorMessage().orElse("").contains("TABLE scope"));
    }

    @Test
    void executeActionMissingTargetThrows() {
        PaimonActionOps ops = newOps(Mockito.mock(Table.class));
        ActionInvocation inv = newInvocation(
                "create_tag", ConnectorActionOps.Scope.TABLE, null,
                new LinkedHashMap<>());

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ops.executeAction(inv));
    }

    @Test
    void argumentCoercionContractRespected() {
        // Arguments arrive already coerced: tag=String, snapshot_id=Long,
        // time_retained_hours=Long. The plugin must not perform any further
        // String->Long parsing.
        Table table = Mockito.mock(Table.class);
        PaimonActionOps ops = newOps(table);
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("tag", "T");
        args.put("snapshot_id", Long.valueOf(99L));
        args.put("time_retained_hours", Long.valueOf(6L));

        ActionResult r = ops.executeCreateTag("db", "t", args);

        Assertions.assertEquals(ActionResult.Lifecycle.COMPLETED, r.lifecycle());
        Mockito.verify(table).createTag("T", 99L, Duration.ofHours(6));
        Assertions.assertEquals(99L, r.rows().get(0).get("snapshot_id"));
    }
}
