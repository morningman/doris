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

import org.apache.doris.connector.api.ConnectorTableId;
import org.apache.doris.connector.api.action.ActionArgument;
import org.apache.doris.connector.api.action.ActionArgumentType;
import org.apache.doris.connector.api.action.ActionDescriptor;
import org.apache.doris.connector.api.action.ActionInvocation;
import org.apache.doris.connector.api.action.ActionResult;
import org.apache.doris.connector.api.action.ActionTarget;
import org.apache.doris.connector.api.action.ConnectorActionOps;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

class HudiActionOpsTest {

    // ---- helpers ----

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
    void listActionsTableScopeReturnsCleanAndCompactionDescriptors() {
        HudiActionOps ops = new HudiActionOps();
        List<ActionDescriptor> descriptors = ops.listActions(
                ConnectorActionOps.Scope.TABLE, Optional.empty());
        Set<String> names = descriptors.stream()
                .map(ActionDescriptor::name)
                .collect(Collectors.toSet());
        Assertions.assertEquals(Set.of("clean", "compaction"), names);
    }

    @Test
    void cleanDescriptorAdvertisesExpectedArgsAndResultColumns() {
        HudiActionOps ops = new HudiActionOps();
        ActionDescriptor clean = findDescriptor(
                ops.listActions(ConnectorActionOps.Scope.TABLE, Optional.empty()),
                "clean");
        Assertions.assertEquals(ConnectorActionOps.Scope.TABLE, clean.scope());

        Map<String, ActionArgument> argByName = clean.arguments().stream()
                .collect(Collectors.toMap(ActionArgument::name, a -> a));
        Assertions.assertEquals(
                Set.of("retain_commits", "dry_run", "target_partitions"),
                argByName.keySet());
        Assertions.assertEquals(ActionArgumentType.LONG,
                argByName.get("retain_commits").type());
        Assertions.assertFalse(argByName.get("retain_commits").required());
        Assertions.assertEquals(ActionArgumentType.BOOLEAN,
                argByName.get("dry_run").type());
        Assertions.assertEquals("true",
                argByName.get("dry_run").defaultValue().orElse(null));
        Assertions.assertEquals(ActionArgumentType.STRING,
                argByName.get("target_partitions").type());

        List<ActionDescriptor.ResultColumn> cols = clean.resultSchema();
        Assertions.assertEquals(4, cols.size());
        Assertions.assertEquals("commit_time", cols.get(0).name());
        Assertions.assertEquals(ActionArgumentType.STRING, cols.get(0).type());
        Assertions.assertEquals("deleted_files", cols.get(1).name());
        Assertions.assertEquals(ActionArgumentType.LONG, cols.get(1).type());
        Assertions.assertEquals("partitions_cleaned", cols.get(2).name());
        Assertions.assertEquals(ActionArgumentType.LONG, cols.get(2).type());
        Assertions.assertEquals("action", cols.get(3).name());
    }

    @Test
    void compactionDescriptorAdvertisesExpectedArgsAndResultColumns() {
        HudiActionOps ops = new HudiActionOps();
        ActionDescriptor compaction = findDescriptor(
                ops.listActions(ConnectorActionOps.Scope.TABLE, Optional.empty()),
                "compaction");
        Set<String> argNames = compaction.arguments().stream()
                .map(ActionArgument::name)
                .collect(Collectors.toSet());
        Assertions.assertEquals(Set.of("instant", "dry_run"), argNames);

        ActionArgument instant = compaction.arguments().stream()
                .filter(a -> a.name().equals("instant"))
                .findFirst().orElseThrow();
        Assertions.assertEquals(ActionArgumentType.STRING, instant.type());
        Assertions.assertFalse(instant.required());

        List<ActionDescriptor.ResultColumn> cols = compaction.resultSchema();
        Assertions.assertEquals(3, cols.size());
        Assertions.assertEquals("instant", cols.get(0).name());
        Assertions.assertEquals("file_groups_compacted", cols.get(1).name());
        Assertions.assertEquals(ActionArgumentType.LONG, cols.get(1).type());
        Assertions.assertEquals("action", cols.get(2).name());
    }

    @Test
    void listActionsCatalogAndSchemaScopesAreEmpty() {
        HudiActionOps ops = new HudiActionOps();
        Assertions.assertTrue(ops.listActions(
                ConnectorActionOps.Scope.CATALOG, Optional.empty()).isEmpty());
        Assertions.assertTrue(ops.listActions(
                ConnectorActionOps.Scope.SCHEMA,
                Optional.of(ActionTarget.ofSchema("db"))).isEmpty());
    }

    // ---- clean execution (descriptor-only; engine not wired) ----

    @Test
    void cleanWithoutArgsReturnsFailedRequiresEngine() {
        HudiActionOps ops = new HudiActionOps();
        ActionResult r = ops.executeAction(newInvocation(
                "clean", ConnectorActionOps.Scope.TABLE,
                tableTarget(), Collections.emptyMap()));
        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        String msg = r.errorMessage().orElse("");
        Assertions.assertTrue(msg.contains("M3-13-be-hudi-engine-worker"), msg);
        Assertions.assertTrue(msg.contains("db.t"), msg);
    }

    @Test
    void cleanWithDryRunAndTargetPartitionsValidatesThenFailsOnEngine() {
        HudiActionOps ops = new HudiActionOps();
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("retain_commits", 5L);
        args.put("dry_run", Boolean.FALSE);
        args.put("target_partitions", "year=2024,year=2025");
        ActionResult r = ops.executeClean("db", "t", args);
        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        Assertions.assertTrue(r.errorMessage().orElse("")
                .contains(HudiActionOps.NO_ENGINE_MESSAGE));
    }

    @Test
    void cleanRejectsNegativeRetainCommitsBeforeEngineCheck() {
        HudiActionOps ops = new HudiActionOps();
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("retain_commits", -1L);
        ActionResult r = ops.executeClean("db", "t", args);
        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        String msg = r.errorMessage().orElse("");
        Assertions.assertTrue(msg.contains("'retain_commits' must be >= 0"), msg);
        // Ensure we didn't reach the engine-required failure.
        Assertions.assertFalse(msg.contains("M3-13-be-hudi-engine-worker"), msg);
    }

    @Test
    void cleanAcceptsZeroRetainCommitsAndEmptyPartitions() {
        HudiActionOps ops = new HudiActionOps();
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("retain_commits", 0L);
        args.put("target_partitions", "");
        ActionResult r = ops.executeClean("db", "t", args);
        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        // Validation passed; failure is the engine-required path.
        Assertions.assertTrue(r.errorMessage().orElse("")
                .contains(HudiActionOps.NO_ENGINE_MESSAGE));
    }

    @Test
    void cleanRejectsNonBooleanDryRun() {
        HudiActionOps ops = new HudiActionOps();
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("dry_run", "true");
        ActionResult r = ops.executeClean("db", "t", args);
        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        Assertions.assertTrue(r.errorMessage().orElse("")
                .contains("'dry_run' must be BOOLEAN"));
    }

    // ---- compaction execution ----

    @Test
    void compactionDefaultPostureReturnsFailedRequiresEngine() {
        HudiActionOps ops = new HudiActionOps();
        ActionResult r = ops.executeAction(newInvocation(
                "compaction", ConnectorActionOps.Scope.TABLE,
                tableTarget(), Collections.emptyMap()));
        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        Assertions.assertTrue(r.errorMessage().orElse("")
                .contains(HudiActionOps.NO_ENGINE_MESSAGE));
    }

    @Test
    void compactionWithExplicitInstantStillFailedRequiresEngine() {
        HudiActionOps ops = new HudiActionOps();
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("instant", "20240101120000000");
        args.put("dry_run", Boolean.TRUE);
        ActionResult r = ops.executeCompaction("db", "t", args);
        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        String msg = r.errorMessage().orElse("");
        Assertions.assertTrue(msg.contains(HudiActionOps.NO_ENGINE_MESSAGE), msg);
        Assertions.assertTrue(msg.contains("compaction for db.t"), msg);
    }

    @Test
    void compactionRejectsNonStringInstant() {
        HudiActionOps ops = new HudiActionOps();
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("instant", 123L);
        ActionResult r = ops.executeCompaction("db", "t", args);
        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        Assertions.assertTrue(r.errorMessage().orElse("")
                .contains("'instant' must be STRING"));
    }

    // ---- dispatch / target validation ----

    @Test
    void unknownActionNameFails() {
        HudiActionOps ops = new HudiActionOps();
        ActionResult r = ops.executeAction(newInvocation(
                "savepoint", ConnectorActionOps.Scope.TABLE,
                tableTarget(), Collections.emptyMap()));
        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        Assertions.assertTrue(r.errorMessage().orElse("")
                .contains("Unknown hudi action: savepoint"));
    }

    @Test
    void caseInsensitiveActionDispatch() {
        HudiActionOps ops = new HudiActionOps();
        ActionResult r = ops.executeAction(newInvocation(
                "CLEAN", ConnectorActionOps.Scope.TABLE,
                tableTarget(), Collections.emptyMap()));
        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        Assertions.assertTrue(r.errorMessage().orElse("")
                .contains(HudiActionOps.NO_ENGINE_MESSAGE));
    }

    @Test
    void nonTableScopeIsRejected() {
        HudiActionOps ops = new HudiActionOps();
        ActionResult r = ops.executeAction(newInvocation(
                "clean", ConnectorActionOps.Scope.CATALOG,
                null, Collections.emptyMap()));
        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        Assertions.assertTrue(r.errorMessage().orElse("")
                .contains("only supported at TABLE scope"));
    }

    @Test
    void schemaTargetWithoutTableIsRejected() {
        HudiActionOps ops = new HudiActionOps();
        ActionResult r = ops.executeAction(newInvocation(
                "clean", ConnectorActionOps.Scope.TABLE,
                ActionTarget.ofSchema("db"), Collections.emptyMap()));
        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        Assertions.assertTrue(r.errorMessage().orElse("")
                .contains("require a TABLE target"));
    }

    // ---- metadata wiring ----

    @Test
    void metadataActionOpsIsNonEmptyAndCachesInstance() {
        HudiConnectorMetadata md = new HudiConnectorMetadata(null, Collections.emptyMap());
        Optional<ConnectorActionOps> ops1 = md.actionOps();
        Optional<ConnectorActionOps> ops2 = md.actionOps();
        Assertions.assertTrue(ops1.isPresent());
        Assertions.assertTrue(ops2.isPresent());
        Assertions.assertSame(ops1.get(), ops2.get());
        Assertions.assertTrue(ops1.get() instanceof HudiActionOps);
    }
}
