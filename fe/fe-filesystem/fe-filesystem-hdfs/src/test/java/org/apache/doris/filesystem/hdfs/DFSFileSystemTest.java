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

package org.apache.doris.filesystem.hdfs;

import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.GlobListing;
import org.apache.doris.filesystem.Location;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Unit tests for {@link DFSFileSystem} constructor and lifecycle.
 * No real HDFS cluster required — tests focus on construction, close behavior,
 * and post-close error detection.
 */
class DFSFileSystemTest {

    // ------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------

    @Test
    void constructor_succeedsWithEmptyProperties() {
        Assertions.assertDoesNotThrow(() -> new DFSFileSystem(new HashMap<>()),
                "DFSFileSystem should accept empty properties (simple auth, no Kerberos)");
    }

    @Test
    void constructor_succeedsWithHadoopUsername() {
        Map<String, String> props = new HashMap<>();
        props.put("hadoop.username", "testuser");

        Assertions.assertDoesNotThrow(() -> new DFSFileSystem(props));
    }

    @Test
    void constructor_succeedsWithHdfsProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("dfs.nameservices", "ns1");
        props.put("hadoop.username", "doris");
        props.put("dfs.ha.namenodes.ns1", "nn1,nn2");

        Assertions.assertDoesNotThrow(() -> new DFSFileSystem(props));
    }

    // ------------------------------------------------------------------
    // close() and post-close behavior
    // ------------------------------------------------------------------

    @Test
    void close_isIdempotent() throws IOException {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        fs.close();
        Assertions.assertDoesNotThrow(fs::close, "Second close should not throw");
    }

    @Test
    void exists_throwsAfterClose() throws IOException {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        fs.close();

        IOException ex = Assertions.assertThrows(IOException.class,
                () -> fs.exists(Location.of("hdfs://namenode/test")));
        Assertions.assertTrue(ex.getMessage().contains("closed"),
                "Error message should indicate the filesystem is closed");
    }

    @Test
    void mkdirs_throwsAfterClose() throws IOException {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        fs.close();

        IOException ex = Assertions.assertThrows(IOException.class,
                () -> fs.mkdirs(Location.of("hdfs://namenode/dir")));
        Assertions.assertTrue(ex.getMessage().contains("closed"));
    }

    @Test
    void delete_throwsAfterClose() throws IOException {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        fs.close();

        IOException ex = Assertions.assertThrows(IOException.class,
                () -> fs.delete(Location.of("hdfs://namenode/file"), false));
        Assertions.assertTrue(ex.getMessage().contains("closed"));
    }

    @Test
    void rename_throwsAfterClose() throws IOException {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        fs.close();

        IOException ex = Assertions.assertThrows(IOException.class,
                () -> fs.rename(Location.of("hdfs://nn/src"), Location.of("hdfs://nn/dst")));
        Assertions.assertTrue(ex.getMessage().contains("closed"));
    }

    @Test
    void list_throwsAfterClose() throws IOException {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        fs.close();

        IOException ex = Assertions.assertThrows(IOException.class,
                () -> fs.list(Location.of("hdfs://namenode/dir")));
        Assertions.assertTrue(ex.getMessage().contains("closed"));
    }

    // ------------------------------------------------------------------
    // globListWithLimit — maxFile pagination cursor semantics
    // ------------------------------------------------------------------

    /**
     * Injects a pre-created Hadoop {@link org.apache.hadoop.fs.FileSystem} into the
     * {@link DFSFileSystem#fsByAuthority fsByAuthority} cache for {@code authority},
     * so {@code getHadoopFs(Path)} returns it without touching the real HDFS.
     */
    @SuppressWarnings("unchecked")
    private static void injectHadoopFs(DFSFileSystem fs, String authority,
            org.apache.hadoop.fs.FileSystem hadoopFs) throws Exception {
        Field f = DFSFileSystem.class.getDeclaredField("fsByAuthority");
        f.setAccessible(true);
        ConcurrentHashMap<String, org.apache.hadoop.fs.FileSystem> map =
                (ConcurrentHashMap<String, org.apache.hadoop.fs.FileSystem>) f.get(fs);
        map.put(authority, hadoopFs);
    }

    private static FileStatus fileStatus(String uri, long len) {
        return new FileStatus(len, false, 1, 1024L, 0L, new Path(uri));
    }

    @Test
    void globListWithLimit_maxFileIsNextCursorWhenPageLimitHit() throws Exception {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        org.apache.hadoop.fs.FileSystem hadoopFs = Mockito.mock(org.apache.hadoop.fs.FileSystem.class);
        FileStatus[] statuses = new FileStatus[10];
        for (int i = 0; i < 10; i++) {
            statuses[i] = fileStatus("hdfs://nn/glob/f" + i + ".csv", 100L);
        }
        Mockito.when(hadoopFs.globStatus(ArgumentMatchers.any(Path.class))).thenReturn(statuses);
        injectHadoopFs(fs, "nn", hadoopFs);

        GlobListing listing = fs.globListWithLimit(
                Location.of("hdfs://nn/glob/*.csv"), null, 0L, 3L);

        Assertions.assertEquals(3, listing.getFiles().size());
        Assertions.assertEquals("hdfs://nn/glob/f0.csv", listing.getFiles().get(0).location().uri());
        Assertions.assertEquals("hdfs://nn/glob/f2.csv", listing.getFiles().get(2).location().uri());
        // Page limit hit, next matching key past the page is f3.csv — that is the cursor.
        Assertions.assertEquals("hdfs://nn/glob/f3.csv", listing.getMaxFile());
        fs.close();
    }

    @Test
    void globListWithLimit_maxFileIsLastKeyWhenListingExhausted() throws Exception {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        org.apache.hadoop.fs.FileSystem hadoopFs = Mockito.mock(org.apache.hadoop.fs.FileSystem.class);
        FileStatus[] statuses = new FileStatus[] {
                fileStatus("hdfs://nn/glob/a.csv", 10L),
                fileStatus("hdfs://nn/glob/b.csv", 10L),
                fileStatus("hdfs://nn/glob/c.csv", 10L),
        };
        Mockito.when(hadoopFs.globStatus(ArgumentMatchers.any(Path.class))).thenReturn(statuses);
        injectHadoopFs(fs, "nn", hadoopFs);

        GlobListing listing = fs.globListWithLimit(
                Location.of("hdfs://nn/glob/*.csv"), null, 0L, 10L);

        Assertions.assertEquals(3, listing.getFiles().size());
        FileEntry last = listing.getFiles().get(listing.getFiles().size() - 1);
        Assertions.assertEquals("hdfs://nn/glob/c.csv", last.location().uri());
        // Listing exhausted before any page limit — maxFile equals last entry on the page.
        Assertions.assertEquals(last.location().uri(), listing.getMaxFile());
        fs.close();
    }

    @Test
    void globListWithLimit_maxFileIsEmptyWhenNoMatches() throws Exception {
        DFSFileSystem fs = new DFSFileSystem(new HashMap<>());
        org.apache.hadoop.fs.FileSystem hadoopFs = Mockito.mock(org.apache.hadoop.fs.FileSystem.class);
        Mockito.when(hadoopFs.globStatus(ArgumentMatchers.any(Path.class)))
                .thenReturn(new FileStatus[0]);
        injectHadoopFs(fs, "nn", hadoopFs);

        GlobListing listing = fs.globListWithLimit(
                Location.of("hdfs://nn/glob/*.csv"), null, 0L, 10L);

        Assertions.assertTrue(listing.getFiles().isEmpty());
        Assertions.assertEquals("", listing.getMaxFile());
        fs.close();
    }
}
