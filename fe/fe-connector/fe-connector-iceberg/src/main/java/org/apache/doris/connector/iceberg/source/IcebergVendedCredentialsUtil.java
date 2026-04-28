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

package org.apache.doris.connector.iceberg.source;

import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Plugin-private helper that merges iceberg-vended storage credentials with
 * the static catalog-level storage properties supplied at connector
 * configuration time.
 *
 * <p>Subset of the legacy fe-core {@code VendedCredentialsFactory} /
 * {@code IcebergVendedCredentialsProvider} that the plugin can implement
 * without depending on fe-core types ({@code MetastoreProperties},
 * {@code StorageProperties}). The legacy code returned a typed
 * {@code Map<StorageProperties.Type, StorageProperties>}; here we operate
 * directly on the flat {@code Map<String, String>} that ultimately reaches
 * BE through {@code TFileScanRangeParams.properties} (PlanNodes.thrift
 * field 9).</p>
 *
 * <p>Inputs gathered from the iceberg {@link Table}:</p>
 * <ul>
 *   <li>{@code table.io().properties()} — the FileIO properties iceberg
 *       attaches to the per-table {@code FileIO} when the catalog vends
 *       credentials per table (legacy
 *       {@code IcebergVendedCredentialsProvider.extractRawVendedCredentials}).</li>
 *   <li>If the {@code FileIO} implements
 *       {@link SupportsStorageCredentials}, every
 *       {@link StorageCredential#config()} entry is mixed in too.</li>
 * </ul>
 *
 * <p>Precedence: vended values take priority over the static catalog
 * defaults for the same key, mirroring legacy
 * {@code Maps.newHashMap(fileIO.properties())}-then-{@code putAll} order.
 * Non-credential keys leak through unchanged — iceberg's FileIO already
 * filters to its own scheme, and BE consumes the merged map by string
 * prefix ({@code s3.}, {@code oss.}, {@code gs.}, {@code adlfs.}, etc.) so
 * no additional prefix gating is performed here.</p>
 */
final class IcebergVendedCredentialsUtil {

    private IcebergVendedCredentialsUtil() {
    }

    /**
     * Returns a new map containing the {@code base} catalog storage
     * properties merged with any iceberg-vended values exposed by
     * {@code table}. Vended values supersede catalog defaults for the
     * same key. Returns the catalog defaults unchanged when iceberg
     * exposes no FileIO or no extra properties.
     *
     * @param table the iceberg table (loaded via the catalog so that
     *              vended creds, if any, are populated on its FileIO);
     *              must not be {@code null}
     * @param base  the catalog-level storage properties (may be empty);
     *              {@code null} is treated as empty
     * @return merged storage properties (mutable, ordering preserves base
     *         insertion order with vended keys appended for stable
     *         iteration in tests)
     */
    static Map<String, String> mergeStorageProperties(Table table, Map<String, String> base) {
        Map<String, String> merged = new LinkedHashMap<>();
        if (base != null) {
            merged.putAll(base);
        }

        FileIO io = table.io();
        if (io == null) {
            return merged;
        }

        Map<String, String> ioProps = io.properties();
        if (ioProps != null && !ioProps.isEmpty()) {
            merged.putAll(ioProps);
        }

        if (io instanceof SupportsStorageCredentials) {
            SupportsStorageCredentials ssc = (SupportsStorageCredentials) io;
            for (StorageCredential credential : ssc.credentials()) {
                Map<String, String> cfg = credential.config();
                if (cfg != null && !cfg.isEmpty()) {
                    merged.putAll(cfg);
                }
            }
        }

        return merged;
    }

    /**
     * Returns a copy of {@code merged} with each key prefixed by
     * {@code prefix}. Used when threading credentials through the
     * scan-node-properties map so they don't collide with other plugin
     * keys; {@link IcebergScanPlanProvider#populateScanLevelParams} strips
     * the prefix when copying into {@code TFileScanRangeParams.properties}.
     */
    static Map<String, String> withPrefix(Map<String, String> merged, String prefix) {
        Map<String, String> out = new HashMap<>(merged.size() * 2);
        for (Map.Entry<String, String> e : merged.entrySet()) {
            out.put(prefix + e.getKey(), e.getValue());
        }
        return out;
    }
}
