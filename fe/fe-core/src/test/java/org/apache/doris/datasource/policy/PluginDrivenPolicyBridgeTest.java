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

package org.apache.doris.datasource.policy;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.credential.UserContext;
import org.apache.doris.connector.api.policy.ColumnMaskHint;
import org.apache.doris.connector.api.policy.ConnectorPolicyContext;
import org.apache.doris.connector.api.policy.PolicyChangeNotification;
import org.apache.doris.connector.api.policy.PolicyKind;
import org.apache.doris.connector.api.policy.PolicyOps;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.mysql.privilege.DataMaskPolicy;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

/**
 * Unit tests for {@link PluginDrivenPolicyBridge}.
 *
 * <p>Coverage matrix:</p>
 * <ul>
 *   <li>{@code supportsRlsAt}: non-PluginDriven catalog, missing capability,
 *       missing {@code policyOps()}, plugin returns {@code false}, plugin
 *       throws.</li>
 *   <li>{@code hintForColumn}: missing capability, hint empty, hint present,
 *       plugin throws.</li>
 *   <li>{@code onPolicyChanged}: non-PluginDriven catalog, missing both
 *       capabilities, only RLS capability declared (one-of-two), plugin
 *       throws.</li>
 *   <li>{@code adaptMaskHint}: NULLIFY / REDACT / EXPRESSION rendering.</li>
 * </ul>
 */
public class PluginDrivenPolicyBridgeTest {

    private static final String CATALOG = "plugin_cat";
    private static final String DATABASE = "db1";
    private static final String TABLE = "t1";
    private static final String COLUMN = "c1";
    private static final String USER = "alice";

    private MockedStatic<Env> mockedEnv;
    private Env env;
    private CatalogMgr catalogMgr;

    @Before
    public void setUp() {
        env = Mockito.mock(Env.class);
        catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
    }

    @After
    public void tearDown() {
        mockedEnv.close();
    }

    // -------- helpers --------

    private TableNameInfo tn() {
        return new TableNameInfo(CATALOG, DATABASE, TABLE);
    }

    private ConnectorPolicyContext ctx() {
        return new ConnectorPolicyContext(CATALOG, DATABASE, TABLE, USER, Optional.empty());
    }

    private UserContext userCtx() {
        return UserContext.builder().username(USER).build();
    }

    private PolicyChangeNotification notif() {
        return new PolicyChangeNotification(CATALOG, DATABASE, Optional.of(TABLE),
                PolicyKind.ROW_FILTER, Instant.EPOCH, Optional.empty());
    }

    private PluginDrivenExternalCatalog buildPluginCatalog(Set<ConnectorCapability> caps,
            Optional<PolicyOps> ops) {
        PolicyOps opsValue = ops.orElse(null);
        return buildPluginCatalogWith(caps, opsValue == null
                ? Optional.empty() : Optional.of(opsValue));
    }

    private PluginDrivenExternalCatalog buildPluginCatalogWith(Set<ConnectorCapability> caps,
            Optional<PolicyOps> ops) {
        PluginDrivenExternalCatalog cat = Mockito.mock(PluginDrivenExternalCatalog.class);
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getCapabilities()).thenReturn(caps);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        Mockito.when(metadata.policyOps()).thenReturn(ops);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        Mockito.when(connector.getMetadata(session)).thenReturn(metadata);
        Mockito.when(cat.buildConnectorSession()).thenReturn(session);
        Mockito.when(cat.getConnector()).thenReturn(connector);
        return cat;
    }

    private void registerCatalog(Object catalog) {
        Mockito.when(catalogMgr.getCatalog(CATALOG)).thenReturn(
                (org.apache.doris.datasource.CatalogIf) catalog);
    }

    // ===== supportsRlsAt =====

    @Test
    public void supportsRlsAt_returnsEmpty_whenCatalogIsNotPluginDriven() {
        InternalCatalog internal = Mockito.mock(InternalCatalog.class);
        registerCatalog(internal);

        Assert.assertEquals(Optional.empty(),
                PluginDrivenPolicyBridge.supportsRlsAt(tn(), ctx()));
    }

    @Test
    public void supportsRlsAt_returnsEmpty_whenCapabilityMissing() {
        PolicyOps ops = Mockito.mock(PolicyOps.class);
        PluginDrivenExternalCatalog cat = buildPluginCatalogWith(
                EnumSet.noneOf(ConnectorCapability.class), Optional.of(ops));
        registerCatalog(cat);

        Assert.assertEquals(Optional.empty(),
                PluginDrivenPolicyBridge.supportsRlsAt(tn(), ctx()));
        Mockito.verifyNoInteractions(ops);
    }

    @Test
    public void supportsRlsAt_returnsEmpty_whenPolicyOpsEmpty() {
        PluginDrivenExternalCatalog cat = buildPluginCatalogWith(
                EnumSet.of(ConnectorCapability.SUPPORTS_RLS_HINT), Optional.empty());
        registerCatalog(cat);

        Assert.assertEquals(Optional.empty(),
                PluginDrivenPolicyBridge.supportsRlsAt(tn(), ctx()));
    }

    @Test
    public void supportsRlsAt_returnsFalse_whenPluginRejects() {
        PolicyOps ops = Mockito.mock(PolicyOps.class);
        Mockito.when(ops.supportsRlsAt(Mockito.eq(DATABASE), Mockito.eq(TABLE), Mockito.any()))
                .thenReturn(false);
        PluginDrivenExternalCatalog cat = buildPluginCatalogWith(
                EnumSet.of(ConnectorCapability.SUPPORTS_RLS_HINT), Optional.of(ops));
        registerCatalog(cat);

        Optional<Boolean> res = PluginDrivenPolicyBridge.supportsRlsAt(tn(), ctx());
        Assert.assertTrue(res.isPresent());
        Assert.assertFalse(res.get());
    }

    @Test
    public void supportsRlsAt_returnsTrue_whenPluginAllows() {
        PolicyOps ops = Mockito.mock(PolicyOps.class);
        Mockito.when(ops.supportsRlsAt(Mockito.eq(DATABASE), Mockito.eq(TABLE), Mockito.any()))
                .thenReturn(true);
        PluginDrivenExternalCatalog cat = buildPluginCatalogWith(
                EnumSet.of(ConnectorCapability.SUPPORTS_RLS_HINT), Optional.of(ops));
        registerCatalog(cat);

        Optional<Boolean> res = PluginDrivenPolicyBridge.supportsRlsAt(tn(), ctx());
        Assert.assertEquals(Optional.of(true), res);
    }

    @Test
    public void supportsRlsAt_swallowsPluginException() {
        PolicyOps ops = Mockito.mock(PolicyOps.class);
        Mockito.when(ops.supportsRlsAt(Mockito.anyString(), Mockito.anyString(), Mockito.any()))
                .thenThrow(new RuntimeException("plugin boom"));
        PluginDrivenExternalCatalog cat = buildPluginCatalogWith(
                EnumSet.of(ConnectorCapability.SUPPORTS_RLS_HINT), Optional.of(ops));
        registerCatalog(cat);

        Assert.assertEquals(Optional.empty(),
                PluginDrivenPolicyBridge.supportsRlsAt(tn(), ctx()));
    }

    // ===== hintForColumn =====

    @Test
    public void hintForColumn_returnsEmpty_whenCapabilityMissing() {
        PolicyOps ops = Mockito.mock(PolicyOps.class);
        PluginDrivenExternalCatalog cat = buildPluginCatalogWith(
                EnumSet.of(ConnectorCapability.SUPPORTS_RLS_HINT), Optional.of(ops));
        registerCatalog(cat);

        Assert.assertEquals(Optional.empty(),
                PluginDrivenPolicyBridge.hintForColumn(tn(), COLUMN, userCtx()));
        Mockito.verifyNoInteractions(ops);
    }

    @Test
    public void hintForColumn_returnsEmpty_whenPluginReturnsEmpty() {
        PolicyOps ops = Mockito.mock(PolicyOps.class);
        Mockito.when(ops.hintForColumn(Mockito.eq(DATABASE), Mockito.eq(TABLE),
                Mockito.eq(COLUMN), Mockito.any())).thenReturn(Optional.empty());
        PluginDrivenExternalCatalog cat = buildPluginCatalogWith(
                EnumSet.of(ConnectorCapability.SUPPORTS_MASK_HINT), Optional.of(ops));
        registerCatalog(cat);

        Assert.assertEquals(Optional.empty(),
                PluginDrivenPolicyBridge.hintForColumn(tn(), COLUMN, userCtx()));
    }

    @Test
    public void hintForColumn_returnsHint_whenPluginVolunteers() {
        ColumnMaskHint hint = ColumnMaskHint.redact();
        PolicyOps ops = Mockito.mock(PolicyOps.class);
        Mockito.when(ops.hintForColumn(Mockito.eq(DATABASE), Mockito.eq(TABLE),
                Mockito.eq(COLUMN), Mockito.any())).thenReturn(Optional.of(hint));
        PluginDrivenExternalCatalog cat = buildPluginCatalogWith(
                EnumSet.of(ConnectorCapability.SUPPORTS_MASK_HINT), Optional.of(ops));
        registerCatalog(cat);

        Optional<ColumnMaskHint> res = PluginDrivenPolicyBridge.hintForColumn(tn(),
                COLUMN, userCtx());
        Assert.assertTrue(res.isPresent());
        Assert.assertSame(hint, res.get());
    }

    @Test
    public void hintForColumn_swallowsPluginException() {
        PolicyOps ops = Mockito.mock(PolicyOps.class);
        Mockito.when(ops.hintForColumn(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.any()))
                .thenThrow(new RuntimeException("plugin boom"));
        PluginDrivenExternalCatalog cat = buildPluginCatalogWith(
                EnumSet.of(ConnectorCapability.SUPPORTS_MASK_HINT), Optional.of(ops));
        registerCatalog(cat);

        Assert.assertEquals(Optional.empty(),
                PluginDrivenPolicyBridge.hintForColumn(tn(), COLUMN, userCtx()));
    }

    // ===== onPolicyChanged =====

    @Test
    public void onPolicyChanged_isNoop_whenCatalogIsNotPluginDriven() {
        InternalCatalog internal = Mockito.mock(InternalCatalog.class);
        registerCatalog(internal);

        // Must not throw.
        PluginDrivenPolicyBridge.onPolicyChanged(tn(), notif());
    }

    @Test
    public void onPolicyChanged_isNoop_whenNoHintCapabilityDeclared() {
        PolicyOps ops = Mockito.mock(PolicyOps.class);
        PluginDrivenExternalCatalog cat = buildPluginCatalogWith(
                EnumSet.noneOf(ConnectorCapability.class), Optional.of(ops));
        registerCatalog(cat);

        PluginDrivenPolicyBridge.onPolicyChanged(tn(), notif());
        Mockito.verify(ops, Mockito.never()).onPolicyChanged(Mockito.any());
    }

    @Test
    public void onPolicyChanged_invokesPlugin_whenAnyHintCapabilityPresent() {
        PolicyOps ops = Mockito.mock(PolicyOps.class);
        PluginDrivenExternalCatalog cat = buildPluginCatalogWith(
                EnumSet.of(ConnectorCapability.SUPPORTS_RLS_HINT), Optional.of(ops));
        registerCatalog(cat);

        PolicyChangeNotification n = notif();
        PluginDrivenPolicyBridge.onPolicyChanged(tn(), n);
        Mockito.verify(ops).onPolicyChanged(n);
    }

    @Test
    public void onPolicyChanged_swallowsPluginException() {
        PolicyOps ops = Mockito.mock(PolicyOps.class);
        Mockito.doThrow(new RuntimeException("plugin boom"))
                .when(ops).onPolicyChanged(Mockito.any());
        PluginDrivenExternalCatalog cat = buildPluginCatalogWith(
                EnumSet.of(ConnectorCapability.SUPPORTS_MASK_HINT), Optional.of(ops));
        registerCatalog(cat);

        // Must not throw.
        PluginDrivenPolicyBridge.onPolicyChanged(tn(), notif());
    }

    // ===== adaptMaskHint =====

    @Test
    public void adaptMaskHint_rendersAllKinds() {
        DataMaskPolicy nullify = PluginDrivenPolicyBridge.adaptMaskHint(
                CATALOG, DATABASE, TABLE, COLUMN, ColumnMaskHint.nullify());
        Assert.assertEquals("NULL", nullify.getMaskTypeDef());
        Assert.assertTrue(nullify.getPolicyIdent().contains("NULLIFY"));

        DataMaskPolicy redact = PluginDrivenPolicyBridge.adaptMaskHint(
                CATALOG, DATABASE, TABLE, COLUMN, ColumnMaskHint.redact());
        Assert.assertEquals("'***'", redact.getMaskTypeDef());

        DataMaskPolicy expr = PluginDrivenPolicyBridge.adaptMaskHint(
                CATALOG, DATABASE, TABLE, COLUMN, ColumnMaskHint.expression("upper(c1)"));
        Assert.assertEquals("upper(c1)", expr.getMaskTypeDef());
    }
}
