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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.credential.UserContext;
import org.apache.doris.connector.api.policy.ColumnMaskHint;
import org.apache.doris.connector.api.policy.ConnectorPolicyContext;
import org.apache.doris.connector.api.policy.PolicyChangeNotification;
import org.apache.doris.connector.api.policy.PolicyOps;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.mysql.privilege.DataMaskPolicy;
import org.apache.doris.qe.ConnectContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * D8 §11 hint-only bridge between fe-core's row-filter / column-mask
 * pipelines and a plugin's {@link PolicyOps}.
 *
 * <p>This bridge never replaces the engine's authoritative
 * {@code AccessControllerManager.evalRowFilterPolicies} /
 * {@code evalDataMaskPolicy} decisions. It only:</p>
 * <ol>
 *   <li>asks the plugin whether the engine-decided row-filter <em>may</em> be
 *       inlined into the connector scan (later push-down rules consume the
 *       resulting hint via {@code StatementContext}); and</li>
 *   <li>volunteers a column-mask hint that the engine may apply when its own
 *       {@code DataMaskPolicy} pipeline is silent.</li>
 * </ol>
 *
 * <p>All methods are no-ops (return empty / do nothing) unless the resolved
 * catalog is a {@link PluginDrivenExternalCatalog}, the connector exposes a
 * non-empty {@link ConnectorMetadata#policyOps()}, and the relevant capability
 * ({@link ConnectorCapability#SUPPORTS_RLS_HINT} /
 * {@link ConnectorCapability#SUPPORTS_MASK_HINT}) is declared. Plugin
 * exceptions are caught, logged and swallowed so analysis never aborts on a
 * misbehaving connector.</p>
 */
public final class PluginDrivenPolicyBridge {

    private static final Logger LOG = LogManager.getLogger(PluginDrivenPolicyBridge.class);

    private PluginDrivenPolicyBridge() {
    }

    /**
     * Whether the given table accepts engine-driven RLS being pushed down to
     * the connector.
     *
     * @return {@link Optional#empty()} when the catalog is not plugin-driven,
     *     the connector lacks {@link ConnectorCapability#SUPPORTS_RLS_HINT},
     *     {@link ConnectorMetadata#policyOps()} is empty, or the plugin call
     *     throws. {@code Optional.of(true|false)} otherwise.
     */
    public static Optional<Boolean> supportsRlsAt(TableNameInfo table, ConnectorPolicyContext ctx) {
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(ctx, "ctx");
        Optional<PolicyOpsHandle> handle = resolveHandle(table.getCtl(),
                ConnectorCapability.SUPPORTS_RLS_HINT);
        if (handle.isEmpty()) {
            return Optional.empty();
        }
        try {
            boolean result = handle.get().ops.supportsRlsAt(table.getDb(), table.getTbl(), ctx);
            return Optional.of(result);
        } catch (Exception e) {
            LOG.warn("PolicyOps#supportsRlsAt threw for {}.{}.{}; treating as no-hint",
                    table.getCtl(), table.getDb(), table.getTbl(), e);
            return Optional.empty();
        }
    }

    /**
     * Plugin-volunteered mask hint for one column. Returned only when the
     * connector both declares {@link ConnectorCapability#SUPPORTS_MASK_HINT}
     * and surfaces a non-empty {@link ConnectorMetadata#policyOps()}.
     */
    public static Optional<ColumnMaskHint> hintForColumn(TableNameInfo table, String column,
            UserContext user) {
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(column, "column");
        Objects.requireNonNull(user, "user");
        Optional<PolicyOpsHandle> handle = resolveHandle(table.getCtl(),
                ConnectorCapability.SUPPORTS_MASK_HINT);
        if (handle.isEmpty()) {
            return Optional.empty();
        }
        try {
            return handle.get().ops.hintForColumn(table.getDb(), table.getTbl(), column, user);
        } catch (Exception e) {
            LOG.warn("PolicyOps#hintForColumn threw for {}.{}.{} column={}; treating as no-hint",
                    table.getCtl(), table.getDb(), table.getTbl(), column, e);
            return Optional.empty();
        }
    }

    /**
     * Forward an engine-side policy change to the plugin so it may invalidate
     * any plugin-side cache. No-op when the catalog is not plugin-driven, the
     * connector lacks both hint capabilities, or {@code policyOps()} is empty.
     */
    public static void onPolicyChanged(TableNameInfo table, PolicyChangeNotification n) {
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(n, "n");
        // Either capability flips the connector into a "policy-aware" plugin
        // for notification purposes; the engine should not require both.
        Optional<PolicyOpsHandle> handle = resolveHandleAnyCap(table.getCtl());
        if (handle.isEmpty()) {
            return;
        }
        try {
            handle.get().ops.onPolicyChanged(n);
        } catch (Exception e) {
            LOG.warn("PolicyOps#onPolicyChanged threw for catalog {}; ignoring",
                    table.getCtl(), e);
        }
    }

    /**
     * Builds a {@link ConnectorPolicyContext} from the current engine session.
     * Helper for callers in fe-core that already hold a {@link ConnectContext}.
     */
    public static ConnectorPolicyContext buildPolicyContext(ConnectContext ctx,
            String catalog, String database, String table) {
        Objects.requireNonNull(ctx, "ctx");
        UserIdentity user = ctx.getCurrentUserIdentity();
        String username = user != null ? user.getQualifiedUser() : "";
        Optional<String> queryId = ctx.queryId() != null
                ? Optional.of(DebugUtil.printId(ctx.queryId())) : Optional.empty();
        return new ConnectorPolicyContext(catalog, database, table, username, queryId);
    }

    /**
     * Builds an SPI {@link UserContext} from the current engine session. The
     * engine has no first-class group / attr surface today; the resulting
     * context only carries the qualified username.
     */
    public static UserContext buildUserContext(ConnectContext ctx) {
        Objects.requireNonNull(ctx, "ctx");
        UserIdentity user = ctx.getCurrentUserIdentity();
        String username = user != null ? user.getQualifiedUser() : "";
        return UserContext.builder().username(username).build();
    }

    /**
     * Adapts a plugin-supplied {@link ColumnMaskHint} into an engine-side
     * {@link DataMaskPolicy}. Used by {@code LogicalCheckPolicy} as a fallback
     * when the engine's own mask pipeline is silent for a column.
     */
    public static DataMaskPolicy adaptMaskHint(String catalog, String database, String table,
            String column, ColumnMaskHint hint) {
        return new PluginColumnMaskPolicy(catalog, database, table, column, hint);
    }

    private static Optional<PolicyOpsHandle> resolveHandle(String catalogName,
            ConnectorCapability requiredCapability) {
        if (Strings.isNullOrEmpty(catalogName)) {
            return Optional.empty();
        }
        Env env = currentEnv();
        if (env == null) {
            return Optional.empty();
        }
        CatalogIf<?> catalog = env.getCatalogMgr().getCatalog(catalogName);
        if (!(catalog instanceof PluginDrivenExternalCatalog)) {
            return Optional.empty();
        }
        try {
            PluginDrivenExternalCatalog plugin = (PluginDrivenExternalCatalog) catalog;
            Connector connector = plugin.getConnector();
            if (connector == null) {
                return Optional.empty();
            }
            Set<ConnectorCapability> caps = connector.getCapabilities();
            if (caps == null || !caps.contains(requiredCapability)) {
                return Optional.empty();
            }
            ConnectorSession session = plugin.buildConnectorSession();
            ConnectorMetadata metadata = connector.getMetadata(session);
            Optional<PolicyOps> ops = metadata.policyOps();
            return ops.map(o -> new PolicyOpsHandle(o, caps));
        } catch (Exception e) {
            LOG.warn("Failed to resolve PolicyOps for catalog {}; treating as no-hint",
                    catalogName, e);
            return Optional.empty();
        }
    }

    private static Optional<PolicyOpsHandle> resolveHandleAnyCap(String catalogName) {
        if (Strings.isNullOrEmpty(catalogName)) {
            return Optional.empty();
        }
        Env env = currentEnv();
        if (env == null) {
            return Optional.empty();
        }
        CatalogIf<?> catalog = env.getCatalogMgr().getCatalog(catalogName);
        if (!(catalog instanceof PluginDrivenExternalCatalog)) {
            return Optional.empty();
        }
        try {
            PluginDrivenExternalCatalog plugin = (PluginDrivenExternalCatalog) catalog;
            Connector connector = plugin.getConnector();
            if (connector == null) {
                return Optional.empty();
            }
            Set<ConnectorCapability> caps = connector.getCapabilities();
            if (caps == null
                    || (!caps.contains(ConnectorCapability.SUPPORTS_RLS_HINT)
                            && !caps.contains(ConnectorCapability.SUPPORTS_MASK_HINT))) {
                return Optional.empty();
            }
            ConnectorSession session = plugin.buildConnectorSession();
            ConnectorMetadata metadata = connector.getMetadata(session);
            Optional<PolicyOps> ops = metadata.policyOps();
            return ops.map(o -> new PolicyOpsHandle(o, caps));
        } catch (Exception e) {
            LOG.warn("Failed to resolve PolicyOps for catalog {}; treating as no-hint",
                    catalogName, e);
            return Optional.empty();
        }
    }

    /**
     * Indirection over {@link Env#getCurrentEnv()} so unit tests can swap in a
     * mocked environment without touching the global singleton lifecycle.
     */
    @VisibleForTesting
    static Env currentEnv() {
        return Env.getCurrentEnv();
    }

    private static final class PolicyOpsHandle {
        final PolicyOps ops;
        final Set<ConnectorCapability> capabilities;

        PolicyOpsHandle(PolicyOps ops, Set<ConnectorCapability> capabilities) {
            this.ops = ops;
            this.capabilities = capabilities != null ? capabilities : Collections.emptySet();
        }
    }
}
