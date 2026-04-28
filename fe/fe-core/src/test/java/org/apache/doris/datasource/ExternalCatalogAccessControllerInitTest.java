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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Env;
import org.apache.doris.connector.spi.ConnectorProvider;
import org.apache.doris.mysql.privilege.AccessControllerManager;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Unit tests for {@link ExternalCatalog#initAccessController(boolean)} that
 * cover the M2-10 wiring of {@link ConnectorProvider#defaultAccessControllerFactoryName()}.
 *
 * <p>Resolution order under test:
 * <ol>
 *   <li>{@code access_controller.class} property wins.</li>
 *   <li>For {@link PluginDrivenExternalCatalog}, the SPI default is used when
 *       the property is absent or empty.</li>
 *   <li>Otherwise the engine falls back to its built-in controller
 *       (no {@code createAccessController} call).</li>
 * </ol>
 */
public class ExternalCatalogAccessControllerInitTest {

    private static final String CATALOG_NAME = "test_cat";

    private MockedStatic<Env> mockedEnv;
    private Env env;
    private AccessControllerManager accessManager;

    @Before
    public void setUp() {
        env = Mockito.mock(Env.class);
        accessManager = Mockito.mock(AccessControllerManager.class);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
    }

    @After
    public void tearDown() {
        mockedEnv.close();
    }

    private PluginDrivenExternalCatalog newPluginCatalog(Map<String, String> props,
                                                        Optional<ConnectorProvider> provider) {
        PluginDrivenExternalCatalog cat = Mockito.spy(new PluginDrivenExternalCatalog());
        cat.name = CATALOG_NAME;
        cat.id = 1L;
        cat.logType = InitCatalogLog.Type.PLUGIN;
        cat.catalogProperty = new CatalogProperty(null, props);
        // Stub SPI lookup so the test does not depend on ConnectorPluginManager state.
        Mockito.doReturn(provider).when(cat).getProvider();
        // Stub getType so we never touch getType() default lookup that may hit "type" prop.
        Mockito.doReturn("hms").when(cat).getType();
        return cat;
    }

    private LegacyTestCatalog newLegacyCatalog(Map<String, String> props) {
        return new LegacyTestCatalog(props);
    }

    // ---- case A: explicit className wins, SPI is not consulted ----

    @Test
    public void explicitPropertyWinsOverSpi() {
        Map<String, String> props = new HashMap<>();
        props.put(CatalogMgr.ACCESS_CONTROLLER_CLASS_PROP,
                "org.apache.doris.mysql.privilege.RangerHiveAccessControllerFactory");
        props.put(CatalogMgr.ACCESS_CONTROLLER_PROPERTY_PREFIX_PROP + "prop1", "v1");

        // SPI returns a different value; explicit property must win.
        ConnectorProvider provider = Mockito.mock(ConnectorProvider.class);
        Mockito.when(provider.defaultAccessControllerFactoryName())
                .thenReturn(Optional.of("ranger-hive"));
        PluginDrivenExternalCatalog cat = newPluginCatalog(props, Optional.of(provider));

        cat.initAccessController(false);

        ArgumentCaptor<String> classNameCap = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Map> acPropsCap = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(accessManager).createAccessController(
                Mockito.eq(CATALOG_NAME), classNameCap.capture(), acPropsCap.capture(),
                Mockito.eq(false));
        Assert.assertEquals(
                "org.apache.doris.mysql.privilege.RangerHiveAccessControllerFactory",
                classNameCap.getValue());
        Assert.assertEquals("v1", acPropsCap.getValue().get("prop1"));
        // SPI must never be queried when the property is set.
        Mockito.verify(provider, Mockito.never()).defaultAccessControllerFactoryName();
    }

    // ---- case B: property missing + SPI returns a name ----

    @Test
    public void spiDefaultUsedWhenPropertyMissing() {
        Map<String, String> props = new HashMap<>();
        props.put(CatalogMgr.ACCESS_CONTROLLER_PROPERTY_PREFIX_PROP + "prop1", "v1");

        ConnectorProvider provider = Mockito.mock(ConnectorProvider.class);
        Mockito.when(provider.defaultAccessControllerFactoryName())
                .thenReturn(Optional.of("ranger-hive"));
        PluginDrivenExternalCatalog cat = newPluginCatalog(props, Optional.of(provider));

        cat.initAccessController(false);

        ArgumentCaptor<Map> acPropsCap = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(accessManager).createAccessController(
                Mockito.eq(CATALOG_NAME), Mockito.eq("ranger-hive"), acPropsCap.capture(),
                Mockito.eq(false));
        Assert.assertEquals("v1", acPropsCap.getValue().get("prop1"));
    }

    // ---- case C: property missing + SPI returns empty -> fallback ----

    @Test
    public void spiEmptyFallsBackToInternal() {
        Map<String, String> props = new HashMap<>();
        ConnectorProvider provider = Mockito.mock(ConnectorProvider.class);
        Mockito.when(provider.defaultAccessControllerFactoryName())
                .thenReturn(Optional.empty());
        PluginDrivenExternalCatalog cat = newPluginCatalog(props, Optional.of(provider));

        cat.initAccessController(false);

        Mockito.verifyNoInteractions(accessManager);
    }

    // ---- case D: legacy (non-plugin) catalog never consults SPI ----

    @Test
    public void legacyCatalogWithoutPropertyFallsBack() {
        Map<String, String> props = new HashMap<>();
        LegacyTestCatalog cat = newLegacyCatalog(props);

        cat.initAccessController(false);

        Mockito.verifyNoInteractions(accessManager);
    }

    @Test
    public void legacyCatalogStillRespectsExplicitProperty() {
        Map<String, String> props = new HashMap<>();
        props.put(CatalogMgr.ACCESS_CONTROLLER_CLASS_PROP, "ranger-hive");
        LegacyTestCatalog cat = newLegacyCatalog(props);

        cat.initAccessController(true);

        Mockito.verify(accessManager).createAccessController(
                Mockito.eq(CATALOG_NAME), Mockito.eq("ranger-hive"), Mockito.anyMap(),
                Mockito.eq(true));
    }

    // ---- case E: dryRun preserves the same resolution ----

    @Test
    public void dryRunUsesSpiDefault() {
        Map<String, String> props = new HashMap<>();
        ConnectorProvider provider = Mockito.mock(ConnectorProvider.class);
        Mockito.when(provider.defaultAccessControllerFactoryName())
                .thenReturn(Optional.of("ranger-hive"));
        PluginDrivenExternalCatalog cat = newPluginCatalog(props, Optional.of(provider));

        cat.initAccessController(true);

        Mockito.verify(accessManager).createAccessController(
                Mockito.eq(CATALOG_NAME), Mockito.eq("ranger-hive"), Mockito.anyMap(),
                Mockito.eq(true));
    }

    // ---- case F: property present but empty == missing ----

    @Test
    public void emptyPropertyIsTreatedAsMissing() {
        Map<String, String> props = new HashMap<>();
        props.put(CatalogMgr.ACCESS_CONTROLLER_CLASS_PROP, "");

        ConnectorProvider provider = Mockito.mock(ConnectorProvider.class);
        Mockito.when(provider.defaultAccessControllerFactoryName())
                .thenReturn(Optional.of("ranger-hive"));
        PluginDrivenExternalCatalog cat = newPluginCatalog(props, Optional.of(provider));

        cat.initAccessController(false);

        Mockito.verify(accessManager).createAccessController(
                Mockito.eq(CATALOG_NAME), Mockito.eq("ranger-hive"), Mockito.anyMap(),
                Mockito.eq(false));
    }

    // ---- case G: plugin catalog with no registered provider falls back ----

    @Test
    public void pluginCatalogWithoutProviderFallsBack() {
        Map<String, String> props = new HashMap<>();
        PluginDrivenExternalCatalog cat = newPluginCatalog(props, Optional.empty());

        cat.initAccessController(false);

        Mockito.verifyNoInteractions(accessManager);
    }

    // ---- case H: ac properties prefix is stripped before being passed in ----

    @Test
    public void acPropertiesPrefixIsStripped() {
        Map<String, String> props = new HashMap<>();
        props.put(CatalogMgr.ACCESS_CONTROLLER_PROPERTY_PREFIX_PROP + "ranger.url", "http://x");
        props.put(CatalogMgr.ACCESS_CONTROLLER_PROPERTY_PREFIX_PROP + "ranger.service", "hive");
        props.put("unrelated.key", "ignored");

        ConnectorProvider provider = Mockito.mock(ConnectorProvider.class);
        Mockito.when(provider.defaultAccessControllerFactoryName())
                .thenReturn(Optional.of("ranger-hive"));
        PluginDrivenExternalCatalog cat = newPluginCatalog(props, Optional.of(provider));

        cat.initAccessController(false);

        ArgumentCaptor<Map> acPropsCap = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(accessManager).createAccessController(
                Mockito.eq(CATALOG_NAME), Mockito.eq("ranger-hive"), acPropsCap.capture(),
                Mockito.eq(false));
        Map captured = acPropsCap.getValue();
        Assert.assertEquals("http://x", captured.get("ranger.url"));
        Assert.assertEquals("hive", captured.get("ranger.service"));
        Assert.assertFalse(captured.containsKey("unrelated.key"));
    }

    /**
     * Minimal concrete {@link ExternalCatalog} subclass used to verify legacy
     * (non-plugin-driven) behaviour. Implements only the abstract methods with
     * no-op stubs; we never exercise metadata paths in this test.
     */
    private static final class LegacyTestCatalog extends ExternalCatalog {
        LegacyTestCatalog(Map<String, String> props) {
            super(2L, CATALOG_NAME, InitCatalogLog.Type.UNKNOWN, "");
            this.catalogProperty = new CatalogProperty(null, props);
        }

        @Override
        protected void initLocalObjectsImpl() {
        }

        @Override
        protected List<String> listTableNamesFromRemote(SessionContext ctx, String dbName) {
            return Collections.emptyList();
        }

        @Override
        public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
            return false;
        }
    }
}
