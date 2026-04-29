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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.NameSpaceContext;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.action.ActionArgument;
import org.apache.doris.connector.api.action.ActionArgumentType;
import org.apache.doris.connector.api.action.ActionDescriptor;
import org.apache.doris.connector.api.action.ActionInvocation;
import org.apache.doris.connector.api.action.ActionResult;
import org.apache.doris.connector.api.action.ConnectorActionOps;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.PluginDrivenExternalDatabase;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.StmtExecutor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Unit tests for {@link PluginDrivenExecuteActionCommand}. Verifies action
 * resolution, argument coercion, default substitution, privilege checking and
 * non-plugin-driven rejection. A fake connector exposes a synthetic action so
 * the command path can be exercised end-to-end without a running cluster.
 */
public class PluginDrivenExecuteActionCommandTest {

    private static final String CATALOG = "ctl";
    private static final String DATABASE = "db";
    private static final String TABLE = "tbl";
    private static final String ACTION = "rewrite_data_files";

    private MockedStatic<Env> mockedEnv;
    private Env env;
    private CatalogMgr catalogMgr;
    private AccessControllerManager accessManager;
    private ConnectContext ctx;
    private StmtExecutor executor;
    private ConnectorActionOps actionOps;

    @BeforeEach
    public void setUp() {
        env = Mockito.mock(Env.class);
        catalogMgr = Mockito.mock(CatalogMgr.class);
        accessManager = Mockito.mock(AccessControllerManager.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        // default: allow LOAD_PRIV
        Mockito.when(accessManager.checkTblPriv(Mockito.any(ConnectContext.class),
                Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                Mockito.eq(PrivPredicate.LOAD))).thenReturn(true);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

        ctx = Mockito.mock(ConnectContext.class);
        Mockito.when(ctx.getNameSpaceContext()).thenReturn(Mockito.mock(NameSpaceContext.class));
        Mockito.when(ctx.getQualifiedUser()).thenReturn("alice");
        Mockito.when(ctx.getRemoteIP()).thenReturn("127.0.0.1");
        Mockito.when(ctx.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);
        executor = Mockito.mock(StmtExecutor.class);

        actionOps = Mockito.mock(ConnectorActionOps.class);
    }

    @AfterEach
    public void tearDown() {
        mockedEnv.close();
    }

    // -------- helpers --------

    private TableNameInfo tn() {
        TableNameInfo n = new TableNameInfo(CATALOG, DATABASE, TABLE);
        // Stub the no-op analyze: tableNameInfo is constructed already-resolved.
        return n;
    }

    private void registerPluginCatalog(Optional<ConnectorActionOps> ops) {
        PluginDrivenExternalCatalog cat = Mockito.mock(PluginDrivenExternalCatalog.class);
        PluginDrivenExternalDatabase database = Mockito.mock(PluginDrivenExternalDatabase.class);
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.getRemoteDbName()).thenReturn(DATABASE);
        Mockito.when(table.getRemoteName()).thenReturn(TABLE);
        Mockito.doReturn(table).when(database).getTableNullable(TABLE);
        Mockito.doReturn(database).when(cat).getDbNullable(DATABASE);

        Connector connector = Mockito.mock(Connector.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        Mockito.when(connector.getMetadata(session)).thenReturn(metadata);
        Mockito.when(metadata.actionOps()).thenReturn(ops);
        Mockito.when(cat.getConnector()).thenReturn(connector);
        Mockito.when(cat.buildConnectorSession()).thenReturn(session);

        Mockito.doReturn(cat).when(catalogMgr).getCatalog(CATALOG);
    }

    private ActionDescriptor descriptorWithDefaults() {
        return ActionDescriptor.builder()
                .name(ACTION)
                .scope(ConnectorActionOps.Scope.TABLE)
                .addArgument(new ActionArgument("target_file_size_bytes", ActionArgumentType.LONG, false,
                        "134217728"))
                .addArgument(new ActionArgument("strategy", ActionArgumentType.STRING, true))
                .addResultColumn(
                        new ActionDescriptor.ResultColumn("rewritten_files_count", ActionArgumentType.LONG))
                .build();
    }

    private void stubAction(ActionDescriptor desc, ActionResult result) {
        Mockito.when(actionOps.listActions(Mockito.eq(ConnectorActionOps.Scope.TABLE),
                Mockito.any())).thenReturn(Collections.singletonList(desc));
        Mockito.when(actionOps.executeAction(Mockito.any(ActionInvocation.class)))
                .thenReturn(result);
    }

    // ===== happy path =====

    @Test
    public void happyPath_resultRowsRendered() throws Exception {
        registerPluginCatalog(Optional.of(actionOps));
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("rewritten_files_count", 7L);
        stubAction(descriptorWithDefaults(),
                ActionResult.completed(Collections.singletonList(row)));

        Map<String, Expression> named = new LinkedHashMap<>();
        named.put("strategy", new StringLiteral("binpack"));
        PluginDrivenExecuteActionCommand cmd = new PluginDrivenExecuteActionCommand(
                tn(), ACTION, Collections.emptyList(), named, "CALL ctl.db.tbl.rewrite_data_files(...)");

        cmd.run(ctx, executor);

        ArgumentCaptor<ShowResultSet> captor = ArgumentCaptor.forClass(ShowResultSet.class);
        Mockito.verify(executor).sendResultSet(captor.capture());
        List<List<String>> rows = captor.getValue().getResultRows();
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("7", rows.get(0).get(0));
    }

    @Test
    public void zeroArgCall_succeedsWhenAllArgsHaveDefaultsOrAreOptional() throws Exception {
        registerPluginCatalog(Optional.of(actionOps));
        ActionDescriptor desc = ActionDescriptor.builder()
                .name(ACTION)
                .scope(ConnectorActionOps.Scope.TABLE)
                .addArgument(new ActionArgument("opt", ActionArgumentType.LONG, false, "10"))
                .build();
        stubAction(desc, ActionResult.completedEmpty());

        PluginDrivenExecuteActionCommand cmd = new PluginDrivenExecuteActionCommand(
                tn(), ACTION, Collections.emptyList(), Collections.emptyMap(), "");
        cmd.run(ctx, executor);

        ArgumentCaptor<ActionInvocation> inv = ArgumentCaptor.forClass(ActionInvocation.class);
        Mockito.verify(actionOps).executeAction(inv.capture());
        Assertions.assertEquals(10L, inv.getValue().arguments().get("opt"));
    }

    @Test
    public void argumentCoercion_stringToLong() throws Exception {
        registerPluginCatalog(Optional.of(actionOps));
        ActionDescriptor desc = ActionDescriptor.builder()
                .name(ACTION)
                .scope(ConnectorActionOps.Scope.TABLE)
                .addArgument(new ActionArgument("count", ActionArgumentType.LONG, true))
                .build();
        stubAction(desc, ActionResult.completedEmpty());

        Map<String, Expression> named = new LinkedHashMap<>();
        named.put("count", new StringLiteral("42"));
        new PluginDrivenExecuteActionCommand(tn(), ACTION,
                Collections.emptyList(), named, "").run(ctx, executor);

        ArgumentCaptor<ActionInvocation> inv = ArgumentCaptor.forClass(ActionInvocation.class);
        Mockito.verify(actionOps).executeAction(inv.capture());
        Assertions.assertEquals(42L, inv.getValue().arguments().get("count"));
    }

    @Test
    public void positionalAndDefault_substitution() throws Exception {
        registerPluginCatalog(Optional.of(actionOps));
        stubAction(descriptorWithDefaults(), ActionResult.completedEmpty());

        new PluginDrivenExecuteActionCommand(tn(), ACTION,
                Collections.emptyList(),
                Collections.singletonMap("strategy", new StringLiteral("sort")),
                "").run(ctx, executor);

        ArgumentCaptor<ActionInvocation> inv = ArgumentCaptor.forClass(ActionInvocation.class);
        Mockito.verify(actionOps).executeAction(inv.capture());
        Assertions.assertEquals(134217728L, inv.getValue().arguments().get("target_file_size_bytes"));
        Assertions.assertEquals("sort", inv.getValue().arguments().get("strategy"));
    }

    @Test
    public void unknownProcedure_throwsClearError() {
        registerPluginCatalog(Optional.of(actionOps));
        Mockito.when(actionOps.listActions(Mockito.any(), Mockito.any()))
                .thenReturn(Collections.emptyList());

        PluginDrivenExecuteActionCommand cmd = new PluginDrivenExecuteActionCommand(
                tn(), "no_such_action", Collections.emptyList(), Collections.emptyMap(), "");
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> cmd.run(ctx, executor));
        Assertions.assertTrue(ex.getMessage().contains("Unknown action"));
    }

    @Test
    public void missingRequiredArg_throwsAnalysisException() {
        registerPluginCatalog(Optional.of(actionOps));
        stubAction(descriptorWithDefaults(), ActionResult.completedEmpty());

        PluginDrivenExecuteActionCommand cmd = new PluginDrivenExecuteActionCommand(
                tn(), ACTION, Collections.emptyList(), Collections.emptyMap(), "");
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> cmd.run(ctx, executor));
        Assertions.assertTrue(ex.getMessage().contains("strategy"));
    }

    @Test
    public void duplicateNamedArg_byPositionalCollision_isRejected() {
        registerPluginCatalog(Optional.of(actionOps));
        stubAction(descriptorWithDefaults(), ActionResult.completedEmpty());

        PluginDrivenExecuteActionCommand cmd = new PluginDrivenExecuteActionCommand(
                tn(), ACTION,
                Collections.singletonList(new IntegerLiteral(99)),
                Collections.singletonMap("target_file_size_bytes", new IntegerLiteral(100)),
                "");
        Assertions.assertThrows(AnalysisException.class, () -> cmd.run(ctx, executor));
    }

    @Test
    public void nonPluginDrivenCatalog_isRejected() {
        InternalCatalog internal = Mockito.mock(InternalCatalog.class);
        Mockito.doReturn(internal).when(catalogMgr).getCatalog(CATALOG);

        PluginDrivenExecuteActionCommand cmd = new PluginDrivenExecuteActionCommand(
                tn(), ACTION, Collections.emptyList(), Collections.emptyMap(), "");
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> cmd.run(ctx, executor));
        Assertions.assertTrue(ex.getMessage().contains("plugin-driven"));
    }

    @Test
    public void privilegeDenied_throwsAnalysisException() {
        registerPluginCatalog(Optional.of(actionOps));
        Mockito.when(accessManager.checkTblPriv(Mockito.any(ConnectContext.class),
                Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                Mockito.eq(PrivPredicate.LOAD))).thenReturn(false);

        PluginDrivenExecuteActionCommand cmd = new PluginDrivenExecuteActionCommand(
                tn(), ACTION, Collections.emptyList(), Collections.emptyMap(), "");
        Assertions.assertThrows(Exception.class, () -> cmd.run(ctx, executor));
    }

    @Test
    public void connectorWithoutActionOps_isRejected() {
        registerPluginCatalog(Optional.empty());

        PluginDrivenExecuteActionCommand cmd = new PluginDrivenExecuteActionCommand(
                tn(), ACTION, Collections.emptyList(), Collections.emptyMap(), "");
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> cmd.run(ctx, executor));
        Assertions.assertTrue(ex.getMessage().contains("does not expose any actions"));
    }

    @Test
    public void table_lookup_failureProducesClearError() {
        PluginDrivenExternalCatalog cat = Mockito.mock(PluginDrivenExternalCatalog.class);
        PluginDrivenExternalDatabase database = Mockito.mock(PluginDrivenExternalDatabase.class);
        Mockito.doReturn(database).when(cat).getDbNullable(DATABASE);
        Mockito.doReturn(null).when(database).getTableNullable(TABLE);
        Mockito.doReturn(cat).when(catalogMgr).getCatalog(CATALOG);

        PluginDrivenExecuteActionCommand cmd = new PluginDrivenExecuteActionCommand(
                tn(), ACTION, Collections.emptyList(), Collections.emptyMap(), "");
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> cmd.run(ctx, executor));
        Assertions.assertTrue(ex.getMessage().contains("does not exist"));
    }
}
