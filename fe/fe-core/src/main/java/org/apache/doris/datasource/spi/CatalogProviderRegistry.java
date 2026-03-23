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

package org.apache.doris.datasource.spi;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for catalog providers discovered via SPI.
 *
 * <p>This registry maintains a mapping from catalog type string (e.g., "es", "iceberg")
 * to the corresponding {@link CatalogProvider} implementation. Providers are registered
 * during FE startup by {@link CatalogPluginLoader} before any EditLog replay occurs.</p>
 */
public class CatalogProviderRegistry {
    private static final Logger LOG = LogManager.getLogger(CatalogProviderRegistry.class);

    private static final Map<String, CatalogProvider> PROVIDERS = new ConcurrentHashMap<>();

    /**
     * Register a catalog provider.
     *
     * @param provider the provider to register
     * @throws IllegalArgumentException if a provider with the same type is already registered
     */
    public static void register(CatalogProvider provider) {
        String type = provider.getType();
        CatalogProvider existing = PROVIDERS.putIfAbsent(type, provider);
        if (existing != null) {
            throw new IllegalArgumentException(
                    "Duplicate CatalogProvider for type '" + type + "': "
                            + existing.getClass().getName() + " vs " + provider.getClass().getName());
        }
        LOG.info("Registered CatalogProvider for type '{}': {}", type, provider.getClass().getName());
    }

    /**
     * Get the catalog provider for the given type.
     *
     * @param type catalog type string
     * @return the provider, or null if no provider is registered for this type
     */
    public static CatalogProvider getProvider(String type) {
        return PROVIDERS.get(type);
    }

    /**
     * Check if a provider is registered for the given type.
     */
    public static boolean hasProvider(String type) {
        return PROVIDERS.containsKey(type);
    }

    /**
     * Get all registered providers (read-only view).
     */
    public static Map<String, CatalogProvider> getAllProviders() {
        return Collections.unmodifiableMap(PROVIDERS);
    }
}
