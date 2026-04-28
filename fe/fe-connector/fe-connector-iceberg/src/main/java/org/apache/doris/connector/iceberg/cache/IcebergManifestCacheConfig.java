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

package org.apache.doris.connector.iceberg.cache;

import java.util.Map;
import java.util.Objects;

/**
 * Catalog-property parser for {@link IcebergPluginManifestCache}
 * configuration. Mirrors the legacy fe-core
 * {@code IcebergUtils#isManifestCacheEnabled} predicate (default
 * {@code false}) and exposes the maximum-entries override.
 *
 * <p>The legacy property keys ({@code meta.cache.iceberg.manifest.*}) live
 * under fe-core; this plugin uses its own namespace
 * ({@code iceberg.manifest_cache.*}) so users opting into the plugin path
 * configure the cache explicitly. The property is intentionally
 * <b>opt-in</b> to match the legacy default
 * {@code DEFAULT_ICEBERG_MANIFEST_CACHE_ENABLE = false}.
 */
public final class IcebergManifestCacheConfig {

    public static final String PROP_ENABLED = "iceberg.manifest_cache.enabled";
    public static final String PROP_MAX_ENTRIES = "iceberg.manifest_cache.max_entries";

    public static final boolean DEFAULT_ENABLED = false;

    private final boolean enabled;
    private final long maxEntries;

    public IcebergManifestCacheConfig(boolean enabled, long maxEntries) {
        this.enabled = enabled;
        this.maxEntries = maxEntries > 0 ? maxEntries : IcebergPluginManifestCache.DEFAULT_MAX_ENTRIES;
    }

    public static IcebergManifestCacheConfig fromProperties(Map<String, String> properties) {
        Objects.requireNonNull(properties, "properties");
        boolean enabled = parseBoolean(properties.get(PROP_ENABLED), DEFAULT_ENABLED);
        long maxEntries = parseLong(properties.get(PROP_MAX_ENTRIES),
                IcebergPluginManifestCache.DEFAULT_MAX_ENTRIES);
        return new IcebergManifestCacheConfig(enabled, maxEntries);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public long getMaxEntries() {
        return maxEntries;
    }

    private static boolean parseBoolean(String raw, boolean defaultValue) {
        if (raw == null || raw.isEmpty()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(raw.trim());
    }

    private static long parseLong(String raw, long defaultValue) {
        if (raw == null || raw.isEmpty()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
