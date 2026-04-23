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

package org.apache.doris.connector.spi;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.extension.spi.Plugin;
import org.apache.doris.extension.spi.PluginFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * SPI interface for connector provider discovery via Java ServiceLoader.
 *
 * <p>Extends {@link PluginFactory} to allow
 * {@link org.apache.doris.extension.loader.DirectoryPluginRuntimeManager}
 * to load connector providers from plugin directories at runtime.
 *
 * <p>Implementations must:
 * <ol>
 *   <li>Have a public no-arg constructor.</li>
 *   <li>Register in META-INF/services/org.apache.doris.connector.spi.ConnectorProvider.</li>
 *   <li>Have NO dependency on fe-core, fe-common, or fe-catalog.</li>
 * </ol>
 *
 * <p>Backend dispatch (selecting between e.g. Iceberg HMS / REST / Glue
 * variants under a single connector type) is opt-in via
 * {@link #getCatalogTypeProperty()} and {@link #getSupportedBackends()};
 * single-backend connectors can ignore both methods and inherit the defaults.
 */
public interface ConnectorProvider extends PluginFactory {

    /**
     * Returns the connector type name (e.g., "hive", "iceberg", "es").
     * Corresponds to the {@code type} property in CREATE CATALOG.
     */
    String getType();

    /**
     * Returns the user-facing property key whose value selects the backend
     * variant within this connector type, e.g. {@code "iceberg.catalog.type"}
     * for Iceberg or {@code "paimon.catalog.type"} for Paimon.
     *
     * <p>For single-backend connectors (jdbc, es, mc, trino, hms, hive, hudi)
     * this returns {@link Optional#empty()}.
     *
     * <p>The fe-core resolver uses this key to look up the backend value in
     * the raw property map and route the {@link #create(Map, ConnectorContext)}
     * call to the appropriate backend implementation. The key must be a
     * property declared by {@link Connector#getCatalogProperties()}.
     */
    default Optional<String> getCatalogTypeProperty() {
        return Optional.empty();
    }

    /**
     * Returns the set of backend variant identifiers this provider supports.
     * Used by the engine to:
     * <ul>
     *   <li>List the legal values for the property returned by
     *       {@link #getCatalogTypeProperty()} in error messages.</li>
     *   <li>Reject CREATE CATALOG with an unsupported backend at validation
     *       time (before plugin instantiation).</li>
     * </ul>
     *
     * <p>For single-backend connectors, the default returns a single-element
     * set containing {@link #getType()} so the contract holds uniformly.
     *
     * <p>Backend identifiers are case-insensitive but should be lowercase
     * canonical names (e.g., {@code "hms"}, {@code "rest"}, {@code "glue"},
     * {@code "dlf"}, {@code "filesystem"} for Iceberg). Aliases are out of
     * scope for M0-05 and will be added in a later PR (D2 §6 references D11).
     */
    default Set<String> getSupportedBackends() {
        return Collections.singleton(getType());
    }

    /**
     * Returns true if this provider can handle the given catalog type and properties.
     * Must be cheap (no network calls) and deterministic.
     */
    default boolean supports(String catalogType, Map<String, String> properties) {
        return getType().equalsIgnoreCase(catalogType);
    }

    /**
     * Creates a Connector instance for a catalog.
     * Called once per catalog lifecycle.
     *
     * @param properties catalog configuration properties
     * @param context runtime context provided by fe-core
     * @return a ready-to-use Connector
     */
    Connector create(Map<String, String> properties, ConnectorContext context);

    /**
     * Validates catalog properties before creation.
     * Called during CREATE CATALOG to fail fast on invalid configuration.
     * Default implementation does nothing (all properties accepted).
     *
     * @param properties catalog configuration properties
     * @throws IllegalArgumentException if required properties are missing or invalid
     */
    default void validateProperties(Map<String, String> properties) {
        // no-op by default
    }

    /** API version for compatibility checking. Major version change = incompatible. */
    default int apiVersion() {
        return 1;
    }

    @Override
    default String name() {
        return getType();
    }

    /**
     * Not used by DirectoryPluginRuntimeManager for connectors.
     * Provided to satisfy {@link PluginFactory} contract.
     */
    @Override
    default Plugin create() {
        throw new UnsupportedOperationException(
                "ConnectorProvider does not support no-arg create(). "
                + "Use create(Map, ConnectorContext) instead.");
    }
}
