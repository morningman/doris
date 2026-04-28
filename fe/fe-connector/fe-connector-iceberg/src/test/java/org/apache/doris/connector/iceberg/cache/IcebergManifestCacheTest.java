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

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.LongAdder;

class IcebergManifestCacheTest {

    private static final String CATALOG = "ctl";
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()));

    @TempDir
    Path tmp;

    private Table table;
    private Path tableDir;

    @BeforeEach
    void setUp() {
        tableDir = tmp.resolve("t-" + UUID.randomUUID());
        HadoopTables tables = new HadoopTables(new Configuration());
        table = tables.create(SCHEMA, PartitionSpec.unpartitioned(),
                Collections.singletonMap("format-version", "2"),
                tableDir.toString());
    }

    @AfterEach
    void tearDown() throws IOException {
        // best-effort table-dir cleanup; @TempDir handles the rest.
        if (tableDir != null && Files.exists(tableDir)) {
            try (java.util.stream.Stream<Path> walk = Files.walk(tableDir)) {
                walk.sorted((a, b) -> b.getNameCount() - a.getNameCount())
                        .forEach(p -> p.toFile().delete());
            }
        }
    }

    /** Append a synthetic data file (metadata-only — the actual file
     *  bytes never need to exist for manifest reads). Triggers a new
     *  manifest write under the table's metadata directory. */
    private DataFile appendOneDataFile(String name) {
        Path fakePath = tableDir.resolve("data").resolve(name + ".parquet");
        DataFile df = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(fakePath.toString())
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(1024L)
                .withRecordCount(10L)
                .build();
        table.newAppend().appendFile(df).commit();
        return df;
    }

    private List<ManifestFile> currentDataManifests() {
        Snapshot snap = table.currentSnapshot();
        Assertions.assertNotNull(snap, "table should have a snapshot after append");
        return snap.dataManifests(table.io());
    }

    // -------- key / config / value tests --------

    @Test
    void cacheKeyEqualsAndHashCodeCoversTriple() {
        ManifestCacheKey a = new ManifestCacheKey(CATALOG, "/m/1", ManifestContent.DATA);
        ManifestCacheKey b = new ManifestCacheKey(CATALOG, "/m/1", ManifestContent.DATA);
        ManifestCacheKey c = new ManifestCacheKey("other", "/m/1", ManifestContent.DATA);
        ManifestCacheKey d = new ManifestCacheKey(CATALOG, "/m/2", ManifestContent.DATA);
        ManifestCacheKey e = new ManifestCacheKey(CATALOG, "/m/1", ManifestContent.DELETES);
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        Assertions.assertNotEquals(a, c);
        Assertions.assertNotEquals(a, d);
        Assertions.assertNotEquals(a, e);
        Assertions.assertNotEquals(a, "not a key");
        Assertions.assertNotEquals(null, a);
    }

    @Test
    void cacheKeyOfManifestExtractsPathAndContent() {
        ManifestFile mf = Mockito.mock(ManifestFile.class);
        Mockito.when(mf.path()).thenReturn("/p/m1.avro");
        Mockito.when(mf.content()).thenReturn(ManifestContent.DELETES);
        ManifestCacheKey key = ManifestCacheKey.of(CATALOG, mf);
        Assertions.assertEquals("/p/m1.avro", key.getManifestPath());
        Assertions.assertEquals(ManifestContent.DELETES, key.getContent());
        Assertions.assertEquals(CATALOG, key.getCatalogName());
        Assertions.assertTrue(key.toString().contains("m1.avro"));
    }

    @Test
    void configFromPropertiesParsesEnabledAndCapacity() {
        Map<String, String> props = new HashMap<>();
        props.put(IcebergManifestCacheConfig.PROP_ENABLED, "true");
        props.put(IcebergManifestCacheConfig.PROP_MAX_ENTRIES, "42");
        IcebergManifestCacheConfig cfg = IcebergManifestCacheConfig.fromProperties(props);
        Assertions.assertTrue(cfg.isEnabled());
        Assertions.assertEquals(42L, cfg.getMaxEntries());
    }

    @Test
    void configDefaultsAreDisabledAndDefaultCapacity() {
        IcebergManifestCacheConfig cfg = IcebergManifestCacheConfig.fromProperties(Collections.emptyMap());
        Assertions.assertFalse(cfg.isEnabled());
        Assertions.assertEquals(IcebergPluginManifestCache.DEFAULT_MAX_ENTRIES, cfg.getMaxEntries());
    }

    @Test
    void configMaxEntriesNonNumericFallsBackToDefault() {
        Map<String, String> props = new HashMap<>();
        props.put(IcebergManifestCacheConfig.PROP_MAX_ENTRIES, "not-a-number");
        IcebergManifestCacheConfig cfg = IcebergManifestCacheConfig.fromProperties(props);
        Assertions.assertEquals(IcebergPluginManifestCache.DEFAULT_MAX_ENTRIES, cfg.getMaxEntries());
    }

    @Test
    void cacheValueFactoryEnforcesEmptyOpposite() {
        Assertions.assertEquals(0,
                ManifestCacheValue.forDataFiles(Collections.emptyList()).getDeleteFiles().size());
        Assertions.assertEquals(0,
                ManifestCacheValue.forDeleteFiles(Collections.emptyList()).getDataFiles().size());
        Assertions.assertEquals(0, ManifestCacheValue.forDataFiles(null).getDataFiles().size());
        Assertions.assertEquals(0, ManifestCacheValue.forDeleteFiles(null).getDeleteFiles().size());
    }

    // -------- real cache flow --------

    @Test
    void firstReadIsMissSecondReadIsHit() {
        appendOneDataFile("a");
        ManifestFile manifest = currentDataManifests().get(0);

        IcebergPluginManifestCache cache = new IcebergPluginManifestCache(8);
        LongAdder hits = new LongAdder();
        LongAdder misses = new LongAdder();
        java.util.function.Consumer<Boolean> rec = h -> {
            if (h) {
                hits.increment();
            } else {
                misses.increment();
            }
        };
        ManifestCacheKey key = ManifestCacheKey.of(CATALOG, manifest);

        ManifestCacheValue v1 = cache.getOrLoadDataFiles(key, manifest, table, rec);
        Assertions.assertEquals(1, v1.getDataFiles().size());
        Assertions.assertEquals(1, misses.sum());
        Assertions.assertEquals(0, hits.sum());

        ManifestCacheValue v2 = cache.getOrLoadDataFiles(key, manifest, table, rec);
        Assertions.assertSame(v1, v2, "second call must return the cached instance");
        Assertions.assertEquals(1, misses.sum());
        Assertions.assertEquals(1, hits.sum());
    }

    @Test
    void newManifestAfterCommitIsAnotherMiss() {
        appendOneDataFile("a");
        ManifestFile manifest1 = currentDataManifests().get(0);
        IcebergPluginManifestCache cache = new IcebergPluginManifestCache(8);
        LongAdder hits = new LongAdder();
        LongAdder misses = new LongAdder();
        java.util.function.Consumer<Boolean> rec = h -> {
            if (h) {
                hits.increment();
            } else {
                misses.increment();
            }
        };
        cache.getOrLoadDataFiles(ManifestCacheKey.of(CATALOG, manifest1), manifest1, table, rec);
        Assertions.assertEquals(1, misses.sum());

        appendOneDataFile("b");
        ManifestFile manifest2 = currentDataManifests().stream()
                .filter(m -> !m.path().equals(manifest1.path()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("expected new manifest after second commit"));
        cache.getOrLoadDataFiles(ManifestCacheKey.of(CATALOG, manifest2), manifest2, table, rec);
        Assertions.assertEquals(2, misses.sum(), "new manifest must miss");
        Assertions.assertEquals(0, hits.sum(), "first manifest was not re-read");
    }

    @Test
    void invalidateAllClearsEntries() {
        appendOneDataFile("a");
        ManifestFile manifest = currentDataManifests().get(0);
        IcebergPluginManifestCache cache = new IcebergPluginManifestCache(8);
        cache.getOrLoadDataFiles(ManifestCacheKey.of(CATALOG, manifest), manifest, table, null);
        Assertions.assertEquals(1L, cache.size());
        cache.invalidateAll();
        Assertions.assertEquals(0L, cache.size());
    }

    @Test
    void loadFailureWrapsAsUncheckedIOException() {
        // Manifest pointing at a non-existent path: the iceberg ManifestFiles.read
        // call surfaces IOException at iteration time; the cache wraps it.
        ManifestFile bogus = Mockito.mock(ManifestFile.class);
        Mockito.when(bogus.path()).thenReturn(tableDir.resolve("does-not-exist.avro").toString());
        Mockito.when(bogus.content()).thenReturn(ManifestContent.DATA);

        IcebergPluginManifestCache cache = new IcebergPluginManifestCache(8);
        Assertions.assertThrows(RuntimeException.class, () ->
                cache.getOrLoadDataFiles(ManifestCacheKey.of(CATALOG, bogus), bogus, table, null));
    }

    @Test
    void dataAndDeleteCacheUseDistinctPaths() {
        appendOneDataFile("a");
        ManifestFile manifest = currentDataManifests().get(0);
        IcebergPluginManifestCache cache = new IcebergPluginManifestCache(8);

        // Data path returns 1 file from this manifest.
        ManifestCacheValue dataVal = cache.getOrLoadDataFiles(
                ManifestCacheKey.of(CATALOG, manifest), manifest, table, null);
        Assertions.assertEquals(1, dataVal.getDataFiles().size());
        Assertions.assertEquals(0, dataVal.getDeleteFiles().size());
    }

    @Test
    void recorderNullIsTolerated() {
        appendOneDataFile("a");
        ManifestFile manifest = currentDataManifests().get(0);
        IcebergPluginManifestCache cache = new IcebergPluginManifestCache(8);
        // Both miss and hit paths must not NPE when recorder is null.
        cache.getOrLoadDataFiles(ManifestCacheKey.of(CATALOG, manifest), manifest, table, null);
        cache.getOrLoadDataFiles(ManifestCacheKey.of(CATALOG, manifest), manifest, table, null);
        Assertions.assertEquals(1L, cache.size());
    }
}
