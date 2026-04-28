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

package org.apache.doris.connector;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.security.SecurityChecker;
import org.apache.doris.common.Config;
import org.apache.doris.common.EnvUtils;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorHttpSecurityHook;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.audit.ConnectorAuditEvent;
import org.apache.doris.connector.api.audit.ConnectorAuditOps;
import org.apache.doris.connector.api.audit.ConnectorAuditOps.AuditEventKind;
import org.apache.doris.connector.api.cache.ConnectorMetaCacheBinding;
import org.apache.doris.connector.api.cache.InvalidateRequest;
import org.apache.doris.connector.api.cache.MetaCacheHandle;
import org.apache.doris.connector.api.credential.CredentialBroker;
import org.apache.doris.connector.api.credential.UserContext;
import org.apache.doris.connector.api.event.ConnectorMetaChangeEvent;
import org.apache.doris.connector.audit.AuditEventChainListener;
import org.apache.doris.connector.audit.PluginAuditEventBridge;
import org.apache.doris.connector.cache.ConnectorMetaCacheRegistry;
import org.apache.doris.connector.credential.DefaultCredentialBroker;
import org.apache.doris.connector.credential.DefaultCredentialBrokerFactory;
import org.apache.doris.connector.event.ConnectorEventDispatcher;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.qe.ConnectContext;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

/**
 * Default implementation of {@link ConnectorContext}.
 *
 * <p>Provides the minimal catalog-level context that connector providers need
 * during creation. Additional context fields can be added here as the SPI evolves.
 */
public class DefaultConnectorContext implements ConnectorContext {

    private static final Logger LOG = LogManager.getLogger(DefaultConnectorContext.class);

    private static final ExecutionAuthenticator NOOP_AUTH = new ExecutionAuthenticator() {};

    /**
     * Engine seam used by {@link #publishExternalEvent} and
     * {@link #publishAuditEvent}. Production wires lambdas that look up the
     * dispatcher / catalog manager / processor through {@link Env}; tests
     * inject mocks via {@link #setEngineHooks} (always restore the previous
     * value in {@code @AfterEach}).
     */
    public interface EngineHooks {
        /** Resolve the audit-emit declaration for a catalog. */
        AuditDeclaration probeAudit(String catalogName);

        /** Forward an external metadata-change event into the engine dispatcher. */
        void dispatchExternal(String catalogName, ConnectorMetaChangeEvent event);
    }

    /**
     * Aggregated audit declaration returned by {@link EngineHooks#probeAudit}.
     * {@code emitsCapability} mirrors {@code ConnectorCapability.EMITS_AUDIT_EVENTS}
     * being declared by the connector; {@code kinds} mirrors the
     * {@code ConnectorAuditOps#emittedEventKinds()} contract.
     */
    public record AuditDeclaration(boolean emitsCapability, Set<AuditEventKind> kinds) {
        public AuditDeclaration {
            Objects.requireNonNull(kinds, "kinds");
            kinds = Set.copyOf(kinds);
        }

        public static AuditDeclaration empty() {
            return new AuditDeclaration(false, Set.of());
        }
    }

    private static final EngineHooks DEFAULT_HOOKS = new EngineHooks() {
        @Override
        public AuditDeclaration probeAudit(String catalogName) {
            Env env = Env.getCurrentEnv();
            if (env == null) {
                return AuditDeclaration.empty();
            }
            CatalogMgr mgr = env.getCatalogMgr();
            if (mgr == null) {
                return AuditDeclaration.empty();
            }
            CatalogIf<?> ci = mgr.getCatalog(catalogName);
            if (!(ci instanceof PluginDrivenExternalCatalog pc)) {
                return AuditDeclaration.empty();
            }
            Connector connector = pc.getConnector();
            if (connector == null) {
                return AuditDeclaration.empty();
            }
            boolean cap = connector.getCapabilities().contains(ConnectorCapability.EMITS_AUDIT_EVENTS);
            ConnectorSession session = pc.buildConnectorSession();
            ConnectorMetadata md = connector.getMetadata(session);
            Optional<ConnectorAuditOps> opsOpt = md.auditOps();
            Set<AuditEventKind> kinds = opsOpt.map(ConnectorAuditOps::emittedEventKinds)
                    .orElse(Set.of());
            return new AuditDeclaration(cap, kinds);
        }

        @Override
        public void dispatchExternal(String catalogName, ConnectorMetaChangeEvent event) {
            Env env = Env.getCurrentEnv();
            if (env == null) {
                LOG.debug("no current Env; dropping external event for catalog {}",
                        catalogName);
                return;
            }
            ConnectorEventDispatcher dispatcher = env.getConnectorEventDispatcher();
            if (dispatcher == null) {
                LOG.debug("no ConnectorEventDispatcher; dropping external event for catalog {}",
                        catalogName);
                return;
            }
            dispatcher.dispatchExternal(catalogName, event);
        }
    };

    private static volatile EngineHooks engineHooks = DEFAULT_HOOKS;

    /** Test seam — install custom engine hooks. */
    @VisibleForTesting
    public static void setEngineHooks(EngineHooks hooks) {
        engineHooks = hooks == null ? DEFAULT_HOOKS : hooks;
    }

    /** Test seam — current hooks (for save / restore). */
    @VisibleForTesting
    public static EngineHooks getEngineHooks() {
        return engineHooks;
    }

    private final String catalogName;
    private final long catalogId;
    private final Map<String, String> environment;
    private final Supplier<ExecutionAuthenticator> authSupplier;
    private final ConnectorMetaCacheRegistry cacheRegistry;
    private final DefaultCredentialBroker credentialBroker;

    private final ConnectorHttpSecurityHook httpSecurityHook = new ConnectorHttpSecurityHook() {
        @Override
        public void beforeRequest(String url) throws Exception {
            SecurityChecker.getInstance().startSSRFChecking(url);
        }

        @Override
        public void afterRequest() {
            SecurityChecker.getInstance().stopSSRFChecking();
        }
    };

    public DefaultConnectorContext(String catalogName, long catalogId) {
        this(catalogName, catalogId, () -> NOOP_AUTH);
    }

    public DefaultConnectorContext(String catalogName, long catalogId,
            Supplier<ExecutionAuthenticator> authSupplier) {
        this.catalogName = Objects.requireNonNull(catalogName, "catalogName");
        this.catalogId = catalogId;
        this.authSupplier = Objects.requireNonNull(authSupplier, "authSupplier");
        this.environment = buildEnvironment();
        this.cacheRegistry = new ConnectorMetaCacheRegistry(catalogName);
        this.credentialBroker = DefaultCredentialBrokerFactory.forCatalog(catalogName);
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public long getCatalogId() {
        return catalogId;
    }

    @Override
    public Map<String, String> getEnvironment() {
        return environment;
    }

    @Override
    public ConnectorHttpSecurityHook getHttpSecurityHook() {
        return httpSecurityHook;
    }

    @Override
    public String sanitizeJdbcUrl(String jdbcUrl) {
        try {
            return SecurityChecker.getInstance().getSafeJdbcUrl(jdbcUrl);
        } catch (Exception e) {
            throw new RuntimeException("JDBC URL security check failed: " + e.getMessage(), e);
        }
    }

    @Override
    public <T> T executeAuthenticated(Callable<T> task) throws Exception {
        return authSupplier.get().execute(task);
    }

    @Override
    public UserContext currentUserContext() {
        ConnectContext cc = ConnectContext.get();
        String username = cc == null ? null : cc.getQualifiedUser();
        if (username == null || username.isEmpty()) {
            username = "system";
        }
        return UserContext.builder().username(username).build();
    }

    @Override
    public <K, V> MetaCacheHandle<K, V> getOrCreateCache(ConnectorMetaCacheBinding<K, V> binding) {
        return cacheRegistry.getOrCreateCache(binding);
    }

    @Override
    public void invalidate(InvalidateRequest req) {
        cacheRegistry.invalidate(req);
    }

    @Override
    public void invalidateAll() {
        cacheRegistry.invalidateAll();
    }

    @Override
    public CredentialBroker getCredentialBroker() {
        return credentialBroker;
    }

    /**
     * D7 — forwards a plugin-emitted metadata change to the engine
     * dispatcher. Followers drop the event (master-gated). The call is
     * exception-isolated: a failing dispatcher cannot surface back to the
     * connector.
     */
    @Override
    public void publishExternalEvent(ConnectorMetaChangeEvent event) {
        Objects.requireNonNull(event, "event");
        try {
            engineHooks.dispatchExternal(catalogName, event);
        } catch (Throwable t) {
            LOG.warn("publishExternalEvent failed for catalog {}", catalogName, t);
        }
    }

    /**
     * D8 — forwards a plugin-emitted audit event into the engine audit
     * pipeline.
     *
     * <p>Three gating checks reject the event before forwarding:</p>
     * <ol>
     *   <li>Event's {@code catalog()} must equal this context's
     *       {@code catalogName} — a plugin must not publish on behalf of
     *       another catalog.</li>
     *   <li>Connector must declare
     *       {@link ConnectorCapability#EMITS_AUDIT_EVENTS}.</li>
     *   <li>Event's classified {@link AuditEventKind} must appear in
     *       {@code ConnectorAuditOps#emittedEventKinds()}.</li>
     * </ol>
     *
     * <p>Surviving events are first handed to
     * {@link AuditEventChainListener#fireBeforeForward} (so future plugins
     * can chain), then to {@link PluginAuditEventBridge#bridge} which
     * converts and queues the {@code AuditEvent} on the engine
     * {@code AuditEventProcessor}. The whole call is exception-isolated.</p>
     */
    @Override
    public void publishAuditEvent(ConnectorAuditEvent event) {
        Objects.requireNonNull(event, "event");
        try {
            if (!Objects.equals(event.catalog(), catalogName)) {
                LOG.warn("plugin {} emitted audit event for foreign catalog {}; dropped",
                        catalogName, event.catalog());
                return;
            }
            AuditDeclaration decl = engineHooks.probeAudit(catalogName);
            if (!decl.emitsCapability()) {
                LOG.warn("plugin {} attempted publishAuditEvent without EMITS_AUDIT_EVENTS capability; dropped",
                        catalogName);
                return;
            }
            AuditEventKind kind = PluginAuditEventBridge.kindOf(event);
            if (!decl.kinds().contains(kind)) {
                LOG.warn("plugin {} emitted audit event of kind {} not in declared set {}; dropped",
                        catalogName, kind, decl.kinds());
                return;
            }
            AuditEventChainListener.INSTANCE.fireBeforeForward(event);
            PluginAuditEventBridge.bridge(event);
        } catch (Throwable t) {
            LOG.warn("publishAuditEvent failed for catalog {}", catalogName, t);
        }
    }

    /**
     * Engine-side accessor for the cache registry. Intended for future
     * integration points (D7 event dispatch, D10 delegate fan-out) that need
     * to call {@link ConnectorMetaCacheRegistry#invalidate(InvalidateRequest)}
     * directly without going through the SPI surface.
     */
    public ConnectorMetaCacheRegistry getCacheRegistry() {
        return cacheRegistry;
    }

    private static Map<String, String> buildEnvironment() {
        Map<String, String> env = new HashMap<>();
        String dorisHome = EnvUtils.getDorisHome();
        if (dorisHome != null) {
            env.put("doris_home", dorisHome);
        }
        env.put("jdbc_drivers_dir", Config.jdbc_drivers_dir);
        env.put("force_sqlserver_jdbc_encrypt_false",
                String.valueOf(Config.force_sqlserver_jdbc_encrypt_false));
        env.put("jdbc_driver_secure_path", Config.jdbc_driver_secure_path);
        return Collections.unmodifiableMap(env);
    }
}
