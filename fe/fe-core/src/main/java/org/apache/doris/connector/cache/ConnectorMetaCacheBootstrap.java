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

package org.apache.doris.connector.cache;

import org.apache.doris.connector.DefaultConnectorContext;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.cache.ConnectorMetaCacheBinding;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;

/**
 * Engine-side glue that wires a plugin {@link Connector}'s declared
 * {@link Connector#getMetaCacheBindings() meta-cache bindings} into the
 * fe-core {@link ConnectorMetaCacheRegistry} owned by its
 * {@link DefaultConnectorContext}.
 *
 * <p>Called from {@code PluginDrivenExternalCatalog#initLocalObjectsImpl()}
 * after the {@link Connector} has been created, and again on re-initialization
 * (drop/recreate, ALTER CATALOG, FE restart). The contract is:</p>
 *
 * <ul>
 *   <li>{@link #bindAll(DefaultConnectorContext, Connector)} is idempotent
 *       per (context, binding) — re-binding the same binding to the same
 *       context returns the previously created handle.</li>
 *   <li>If the plugin returns {@code null} from {@code getMetaCacheBindings()},
 *       this is treated as an empty list (the SPI default already returns
 *       {@code List.of()}, but we defend against custom implementations).</li>
 *   <li>Bindings are NOT replicated through EditLog. After a FE restart the
 *       catalog is re-initialized which triggers another bootstrap pass —
 *       binding sets are always reconstructed from the live plugin code.</li>
 * </ul>
 *
 * <p>Cross-FE invalidation broadcast is NOT yet implemented (see M1-10/11
 * follow-up). Single-FE invalidation flows through
 * {@link DefaultConnectorContext#invalidate} →
 * {@link ConnectorMetaCacheRegistry#invalidate}.</p>
 */
public final class ConnectorMetaCacheBootstrap {

    private static final Logger LOG = LogManager.getLogger(ConnectorMetaCacheBootstrap.class);

    private ConnectorMetaCacheBootstrap() {
    }

    /**
     * Reads {@code connector.getMetaCacheBindings()} and binds each entry to
     * {@code context.getCacheRegistry()}.
     *
     * @return number of NEWLY-registered bindings (existing entries with a
     *         matching binding are not counted).
     */
    public static int bindAll(DefaultConnectorContext context, Connector connector) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(connector, "connector");
        List<ConnectorMetaCacheBinding<?, ?>> bindings = connector.getMetaCacheBindings();
        if (bindings == null || bindings.isEmpty()) {
            return 0;
        }
        int created = context.getCacheRegistry().bindAll(bindings);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Bound {} cache binding(s) (declared={}) for catalog '{}' (id={})",
                    created, bindings.size(), context.getCatalogName(), context.getCatalogId());
        }
        return created;
    }

    /**
     * Detach (invalidate + drop) every binding registered against
     * {@code context.getCacheRegistry()}. Called from the catalog's
     * {@code onClose} hook so that a stale registry never outlives its
     * connector.
     */
    public static void detachAll(DefaultConnectorContext context) {
        Objects.requireNonNull(context, "context");
        ConnectorMetaCacheRegistry registry = context.getCacheRegistry();
        int n = registry.size();
        registry.detach();
        if (LOG.isDebugEnabled() && n > 0) {
            LOG.debug("Detached {} cache binding(s) from catalog '{}' (id={})",
                    n, context.getCatalogName(), context.getCatalogId());
        }
    }
}
