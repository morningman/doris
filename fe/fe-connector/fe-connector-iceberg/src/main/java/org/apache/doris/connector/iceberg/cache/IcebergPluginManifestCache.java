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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Table;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Plugin-private manifest cache. Owned by a single
 * {@code IcebergConnector} instance (per the D3 cache-ownership decision
 * — the plugin does not depend on fe-core's
 * {@code IcebergExternalMetaCache}).
 *
 * <p>Backed by Caffeine. Manifest files in iceberg are immutable, so the
 * cache uses no TTL by default — entries are evicted only when the
 * configured maximum size is exceeded.
 *
 * <p>The legacy fe-core algorithm in
 * {@code IcebergExternalMetaCache#loadManifestCacheValue} is mirrored
 * exactly: data manifests are decoded with {@link ManifestFiles#read} and
 * each {@link DataFile#copy()} is stored; delete manifests are decoded
 * with {@link ManifestFiles#readDeleteManifest} using the table's
 * partition specs and each {@link DeleteFile#copy()} is stored. Copies
 * are required because the iceberg ManifestReader returns flyweight
 * objects that are reused across the iterator.
 */
public final class IcebergPluginManifestCache {

    /** Default maximum number of cached manifests. Matches the BE-side
     *  manifest-cache default and is intentionally large because each
     *  entry's size is bounded by the original manifest file. */
    public static final long DEFAULT_MAX_ENTRIES = 100_000L;

    private final Cache<ManifestCacheKey, ManifestCacheValue> cache;

    public IcebergPluginManifestCache(long maxEntries) {
        long capacity = maxEntries > 0 ? maxEntries : DEFAULT_MAX_ENTRIES;
        this.cache = Caffeine.newBuilder()
                .maximumSize(capacity)
                .build();
    }

    /**
     * Look up data files for {@code manifest}; load and cache on miss.
     *
     * @param key          stable cache key — typically built via
     *                     {@link ManifestCacheKey#of(String, ManifestFile)}.
     * @param manifest     the iceberg manifest to read on miss.
     * @param table        owning iceberg table (provides {@code io()}).
     * @param hitRecorder  invoked exactly once with {@code true} on hit
     *                     and {@code false} on miss.
     */
    public ManifestCacheValue getOrLoadDataFiles(ManifestCacheKey key,
                                                 ManifestFile manifest,
                                                 Table table,
                                                 Consumer<Boolean> hitRecorder) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(manifest, "manifest");
        Objects.requireNonNull(table, "table");
        ManifestCacheValue cached = cache.getIfPresent(key);
        if (cached != null) {
            recordHit(hitRecorder, true);
            return cached;
        }
        ManifestCacheValue loaded = cache.get(key, ignored -> loadDataFiles(manifest, table));
        recordHit(hitRecorder, false);
        return loaded;
    }

    /**
     * Look up delete files for {@code manifest}; load and cache on miss.
     */
    public ManifestCacheValue getOrLoadDeleteFiles(ManifestCacheKey key,
                                                   ManifestFile manifest,
                                                   Table table,
                                                   Consumer<Boolean> hitRecorder) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(manifest, "manifest");
        Objects.requireNonNull(table, "table");
        ManifestCacheValue cached = cache.getIfPresent(key);
        if (cached != null) {
            recordHit(hitRecorder, true);
            return cached;
        }
        ManifestCacheValue loaded = cache.get(key, ignored -> loadDeleteFiles(manifest, table));
        recordHit(hitRecorder, false);
        return loaded;
    }

    /** Visible for tests / explain rendering: total cache entries. */
    public long size() {
        return cache.estimatedSize();
    }

    /** Visible for tests: drop everything (the cache is per-connector,
     *  so a connector close should invalidate it). */
    public void invalidateAll() {
        cache.invalidateAll();
    }

    private static void recordHit(Consumer<Boolean> recorder, boolean hit) {
        if (recorder != null) {
            recorder.accept(hit);
        }
    }

    private static ManifestCacheValue loadDataFiles(ManifestFile manifest, Table table) {
        List<DataFile> out = new ArrayList<>();
        try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, table.io())) {
            for (DataFile dataFile : reader) {
                out.add(dataFile.copy());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to read iceberg data manifest " + manifest.path(), e);
        }
        return ManifestCacheValue.forDataFiles(out);
    }

    private static ManifestCacheValue loadDeleteFiles(ManifestFile manifest, Table table) {
        List<DeleteFile> out = new ArrayList<>();
        try (ManifestReader<DeleteFile> reader = ManifestFiles.readDeleteManifest(
                manifest, table.io(), table.specs())) {
            for (DeleteFile deleteFile : reader) {
                out.add(deleteFile.copy());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to read iceberg delete manifest " + manifest.path(), e);
        }
        return ManifestCacheValue.forDeleteFiles(out);
    }
}
