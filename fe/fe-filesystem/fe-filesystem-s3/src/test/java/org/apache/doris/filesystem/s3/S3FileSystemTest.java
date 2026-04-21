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

package org.apache.doris.filesystem.s3;

import org.apache.doris.filesystem.DorisOutputFile;
import org.apache.doris.filesystem.Location;
import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;
import org.apache.doris.filesystem.spi.RequestBody;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.List;

/**
 * Unit tests for {@link S3FileSystem} using a mock {@link S3ObjStorage}.
 * No real AWS credentials or S3 connectivity required.
 */
class S3FileSystemTest {

    private S3ObjStorage mockStorage;
    private S3FileSystem fs;

    @BeforeEach
    void setUp() {
        mockStorage = Mockito.mock(S3ObjStorage.class);
        fs = new S3FileSystem(mockStorage);
    }

    // ------------------------------------------------------------------
    // exists() (inherited from ObjFileSystem)
    // ------------------------------------------------------------------

    @Test
    void exists_returnsTrueWhenHeadObjectSucceeds() throws IOException {
        Mockito.when(mockStorage.headObject("s3://bucket/key"))
                .thenReturn(new RemoteObject("key", "key", null, 100L, 0L));

        Assertions.assertTrue(fs.exists(Location.of("s3://bucket/key")));
    }

    @Test
    void exists_returnsFalseForFileNotFoundException() throws IOException {
        Mockito.when(mockStorage.headObject("s3://bucket/missing"))
                .thenThrow(new FileNotFoundException("not found"));
        // exists() falls back to a 1-key prefix probe for marker-less directories.
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("s3://bucket/missing/"),
                ArgumentMatchers.isNull(), ArgumentMatchers.eq(1)))
                .thenReturn(new RemoteObjects(List.of(), false, null));

        Assertions.assertFalse(fs.exists(Location.of("s3://bucket/missing")));
    }

    @Test
    void exists_rethrowsPlain404IOException() throws IOException {
        IOException io404 = new IOException("HTTP 404 Not Found");
        Mockito.doThrow(io404).when(mockStorage).headObject("s3://bucket/gone");

        IOException thrown = Assertions.assertThrows(IOException.class,
                () -> fs.exists(Location.of("s3://bucket/gone")));
        Assertions.assertEquals(io404, thrown);
    }

    // ------------------------------------------------------------------
    // mkdirs()
    // ------------------------------------------------------------------

    @Test
    void mkdirs_putsZeroByteMarkerWithTrailingSlashAndParentMarkers() throws IOException {
        // Nothing exists: HEAD on every probed URI returns 404 → FileNotFoundException.
        Mockito.when(mockStorage.headObject(ArgumentMatchers.anyString()))
                .thenThrow(new FileNotFoundException("missing"));

        fs.mkdirs(Location.of("s3://bucket/dir/subdir"));

        // Both the leaf marker and the missing parent marker must be PUT (top-down).
        org.mockito.InOrder inOrder = Mockito.inOrder(mockStorage);
        inOrder.verify(mockStorage).putObject(ArgumentMatchers.eq("s3://bucket/dir/"),
                ArgumentMatchers.any(RequestBody.class));
        inOrder.verify(mockStorage).putObject(ArgumentMatchers.eq("s3://bucket/dir/subdir/"),
                ArgumentMatchers.any(RequestBody.class));
        Mockito.verify(mockStorage, Mockito.times(2)).putObject(
                ArgumentMatchers.anyString(), ArgumentMatchers.any(RequestBody.class));
    }

    @Test
    void mkdirs_doesNotDoubleSlashIfAlreadyPresent() throws IOException {
        Mockito.when(mockStorage.headObject(ArgumentMatchers.anyString()))
                .thenThrow(new FileNotFoundException("missing"));

        fs.mkdirs(Location.of("s3://bucket/dir/"));

        // Single-level dir under bucket root has no parent ancestor to create.
        Mockito.verify(mockStorage).putObject(ArgumentMatchers.eq("s3://bucket/dir/"),
                ArgumentMatchers.any(RequestBody.class));
        Mockito.verify(mockStorage, Mockito.times(1)).putObject(
                ArgumentMatchers.anyString(), ArgumentMatchers.any(RequestBody.class));
    }

    @Test
    void mkdirs_isIdempotentWhenMarkerAlreadyExists() throws IOException {
        // HEAD on the marker succeeds → return without PUT.
        Mockito.when(mockStorage.headObject("s3://bucket/dir/sub/"))
                .thenReturn(new RemoteObject("dir/sub/", "", null, 0L, 0L));

        fs.mkdirs(Location.of("s3://bucket/dir/sub"));

        Mockito.verify(mockStorage, Mockito.never()).putObject(
                ArgumentMatchers.anyString(), ArgumentMatchers.any(RequestBody.class));
    }

    @Test
    void mkdirs_skipsParentMarkerWhenAlreadyPresent() throws IOException {
        // Default: missing. Specific URI: present. Use doThrow/doReturn to allow override.
        Mockito.doThrow(new FileNotFoundException("missing"))
                .when(mockStorage).headObject(ArgumentMatchers.anyString());
        Mockito.doReturn(new RemoteObject("dir/", "", null, 0L, 0L))
                .when(mockStorage).headObject("s3://bucket/dir/");

        fs.mkdirs(Location.of("s3://bucket/dir/sub"));

        Mockito.verify(mockStorage).putObject(ArgumentMatchers.eq("s3://bucket/dir/sub/"),
                ArgumentMatchers.any(RequestBody.class));
        Mockito.verify(mockStorage, Mockito.never()).putObject(ArgumentMatchers.eq("s3://bucket/dir/"),
                ArgumentMatchers.any(RequestBody.class));
    }

    @Test
    void mkdirs_rejectsWhenRealFileExistsAtSamePath() throws IOException {
        Mockito.doThrow(new FileNotFoundException("missing"))
                .when(mockStorage).headObject(ArgumentMatchers.anyString());
        // A real (non-marker) file already lives at the bare path.
        Mockito.doReturn(new RemoteObject("dir/file", "file", null, 42L, 0L))
                .when(mockStorage).headObject("s3://bucket/dir/file");

        IOException ex = Assertions.assertThrows(IOException.class,
                () -> fs.mkdirs(Location.of("s3://bucket/dir/file")));
        Assertions.assertTrue(ex.getMessage().contains("non-directory"),
                "expected refusal message, got: " + ex.getMessage());
        Mockito.verify(mockStorage, Mockito.never()).putObject(
                ArgumentMatchers.anyString(), ArgumentMatchers.any(RequestBody.class));
    }

    // ------------------------------------------------------------------
    // delete()
    // ------------------------------------------------------------------

    @Test
    void delete_nonRecursiveDeletesExactObject() throws IOException {
        // Probe must return empty so non-empty check passes.
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("s3://bucket/file.txt/"),
                ArgumentMatchers.isNull(), ArgumentMatchers.eq(2)))
                .thenReturn(new RemoteObjects(List.of(), false, null));

        fs.delete(Location.of("s3://bucket/file.txt"), false);

        Mockito.verify(mockStorage).deleteObject("s3://bucket/file.txt");
    }

    @Test
    void delete_nonRecursiveSwallowsNotFoundError() throws IOException {
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("s3://bucket/gone/"),
                ArgumentMatchers.isNull(), ArgumentMatchers.eq(2)))
                .thenReturn(new RemoteObjects(List.of(), false, null));
        Mockito.doThrow(new FileNotFoundException("not found"))
                .when(mockStorage).deleteObject("s3://bucket/gone");

        // Should not throw
        fs.delete(Location.of("s3://bucket/gone"), false);
    }

    @Test
    void delete_nonRecursiveOnEmptyDirSucceeds() throws IOException {
        // Only the directory marker exists; treat as empty.
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("s3://bucket/emptydir/"),
                ArgumentMatchers.isNull(), ArgumentMatchers.eq(2)))
                .thenReturn(new RemoteObjects(
                        List.of(new RemoteObject("emptydir/", "", null, 0L, 0L)),
                        false, null));

        fs.delete(Location.of("s3://bucket/emptydir"), false);

        Mockito.verify(mockStorage).deleteObject("s3://bucket/emptydir");
    }

    @Test
    void delete_nonRecursiveOnNonEmptyDirThrows() throws IOException {
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("s3://bucket/dir/"),
                ArgumentMatchers.isNull(), ArgumentMatchers.eq(2)))
                .thenReturn(new RemoteObjects(
                        List.of(new RemoteObject("dir/child", "child", null, 1L, 0L)),
                        false, null));

        IOException ex = Assertions.assertThrows(IOException.class,
                () -> fs.delete(Location.of("s3://bucket/dir"), false));
        Assertions.assertTrue(ex.getMessage().contains("Directory not empty"),
                "expected non-empty error, got: " + ex.getMessage());
        Mockito.verify(mockStorage, Mockito.never()).deleteObject(ArgumentMatchers.anyString());
    }

    @Test
    void delete_recursiveBatchDeletesAllObjectsUnderPrefix() throws IOException {
        RemoteObjects page = new RemoteObjects(
                List.of(
                        new RemoteObject("dir/a.txt", "a.txt", null, 10L, 0L),
                        new RemoteObject("dir/b.txt", "b.txt", null, 20L, 0L)),
                false, null);
        Mockito.when(mockStorage.listObjects(ArgumentMatchers.eq("s3://bucket/dir/"), ArgumentMatchers.any())).thenReturn(page);

        fs.delete(Location.of("s3://bucket/dir"), true);

        // Single batched DeleteObjects (one HTTP call per page) instead of N DeleteObject calls.
        Mockito.verify(mockStorage).deleteObjectsByKeys(
                ArgumentMatchers.eq("bucket"),
                ArgumentMatchers.eq(List.of("dir/a.txt", "dir/b.txt")));
        Mockito.verify(mockStorage, Mockito.never()).deleteObject(ArgumentMatchers.eq("s3://bucket/dir/a.txt"));
        Mockito.verify(mockStorage, Mockito.never()).deleteObject(ArgumentMatchers.eq("s3://bucket/dir/b.txt"));
        // The exact-key delete still runs (no-op when missing).
        Mockito.verify(mockStorage).deleteObject("s3://bucket/dir");
    }

    // ------------------------------------------------------------------
    // rename()
    // ------------------------------------------------------------------

    @Test
    void rename_copyThenDelete() throws IOException {
        // dst HEAD must report not-found.
        Mockito.when(mockStorage.headObject("s3://bucket/new"))
                .thenThrow(new FileNotFoundException("missing"));
        // src HEAD reports the existing object.
        Mockito.when(mockStorage.headObject("s3://bucket/old"))
                .thenReturn(new RemoteObject("old", "old", null, 1L, 0L));

        fs.rename(Location.of("s3://bucket/old"), Location.of("s3://bucket/new"));

        Mockito.verify(mockStorage).copyObject("s3://bucket/old", "s3://bucket/new");
        Mockito.verify(mockStorage).deleteObject("s3://bucket/old");
    }

    @Test
    void rename_throwsFileAlreadyExistsWhenDstExists() throws IOException {
        Mockito.when(mockStorage.headObject("s3://bucket/dst"))
                .thenReturn(new RemoteObject("dst", "dst", null, 5L, 0L));

        Assertions.assertThrows(FileAlreadyExistsException.class,
                () -> fs.rename(Location.of("s3://bucket/src"), Location.of("s3://bucket/dst")));
        Mockito.verify(mockStorage, Mockito.never()).copyObject(
                ArgumentMatchers.anyString(), ArgumentMatchers.anyString());
        Mockito.verify(mockStorage, Mockito.never()).deleteObject(ArgumentMatchers.anyString());
    }

    @Test
    void rename_rejectsDirectoryPrefixSrc() throws IOException {
        // dst not found, src not a key but prefix has children.
        Mockito.when(mockStorage.headObject("s3://bucket/dst"))
                .thenThrow(new FileNotFoundException("missing"));
        Mockito.when(mockStorage.headObject("s3://bucket/srcdir"))
                .thenThrow(new FileNotFoundException("missing"));
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("s3://bucket/srcdir/"),
                ArgumentMatchers.isNull(), ArgumentMatchers.eq(1)))
                .thenReturn(new RemoteObjects(
                        List.of(new RemoteObject("srcdir/a", "a", null, 1L, 0L)),
                        false, null));

        IOException ex = Assertions.assertThrows(IOException.class,
                () -> fs.rename(Location.of("s3://bucket/srcdir"), Location.of("s3://bucket/dst")));
        Assertions.assertTrue(ex.getMessage().contains("renameDirectory"),
                "expected hint to use renameDirectory, got: " + ex.getMessage());
        Mockito.verify(mockStorage, Mockito.never()).copyObject(
                ArgumentMatchers.anyString(), ArgumentMatchers.anyString());
    }

    // ------------------------------------------------------------------
    // exists() — marker-less prefix fallback (#4)
    // ------------------------------------------------------------------

    @Test
    void exists_returnsTrueForMarkerlessPrefixWithChildren() throws IOException {
        // HEAD on the bare key returns 404 (no marker), but the prefix has children.
        Mockito.when(mockStorage.headObject("s3://bucket/hivedir"))
                .thenThrow(new FileNotFoundException("no marker"));
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("s3://bucket/hivedir/"),
                ArgumentMatchers.isNull(), ArgumentMatchers.eq(1)))
                .thenReturn(new RemoteObjects(
                        List.of(new RemoteObject("hivedir/part-0", "part-0", null, 100L, 0L)),
                        false, null));

        Assertions.assertTrue(fs.exists(Location.of("s3://bucket/hivedir")));
    }

    @Test
    void exists_returnsFalseWhenNoKeyAndNoChildren() throws IOException {
        Mockito.when(mockStorage.headObject("s3://bucket/missing"))
                .thenThrow(new FileNotFoundException("missing"));
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("s3://bucket/missing/"),
                ArgumentMatchers.isNull(), ArgumentMatchers.eq(1)))
                .thenReturn(new RemoteObjects(List.of(), false, null));

        Assertions.assertFalse(fs.exists(Location.of("s3://bucket/missing")));
    }

    // ------------------------------------------------------------------
    // renameDirectory() (#4)
    // ------------------------------------------------------------------

    @Test
    void renameDirectory_copiesEachChildAndBatchDeletes() throws IOException {
        // Source has two children, dst is empty.
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("s3://bucket/src/"), ArgumentMatchers.isNull()))
                .thenReturn(new RemoteObjects(
                        List.of(
                                new RemoteObject("src/a.txt", "a.txt", null, 1L, 0L),
                                new RemoteObject("src/sub/b.txt", "sub/b.txt", null, 2L, 0L)),
                        false, null));
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("s3://bucket/dst/"),
                ArgumentMatchers.isNull(), ArgumentMatchers.eq(1)))
                .thenReturn(new RemoteObjects(List.of(), false, null));

        Runnable notExists = Mockito.mock(Runnable.class);
        fs.renameDirectory(Location.of("s3://bucket/src"), Location.of("s3://bucket/dst"), notExists);

        Mockito.verify(notExists, Mockito.never()).run();
        Mockito.verify(mockStorage).copyObject("s3://bucket/src/a.txt", "s3://bucket/dst/a.txt");
        Mockito.verify(mockStorage).copyObject(
                "s3://bucket/src/sub/b.txt", "s3://bucket/dst/sub/b.txt");
        Mockito.verify(mockStorage).deleteObjectsByKeys(
                ArgumentMatchers.eq("bucket"),
                ArgumentMatchers.eq(List.of("src/a.txt", "src/sub/b.txt")));
    }

    @Test
    void renameDirectory_runsWhenSrcNotExistsCallback() throws IOException {
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("s3://bucket/missing/"), ArgumentMatchers.isNull()))
                .thenReturn(new RemoteObjects(List.of(), false, null));

        Runnable notExists = Mockito.mock(Runnable.class);
        fs.renameDirectory(
                Location.of("s3://bucket/missing"), Location.of("s3://bucket/dst"), notExists);

        Mockito.verify(notExists).run();
        Mockito.verify(mockStorage, Mockito.never()).copyObject(
                ArgumentMatchers.anyString(), ArgumentMatchers.anyString());
        Mockito.verify(mockStorage, Mockito.never()).deleteObjectsByKeys(
                ArgumentMatchers.anyString(), ArgumentMatchers.anyList());
    }

    @Test
    void renameDirectory_abortsWhenDstHasObjects() throws IOException {
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("s3://bucket/src/"), ArgumentMatchers.isNull()))
                .thenReturn(new RemoteObjects(
                        List.of(new RemoteObject("src/a.txt", "a.txt", null, 1L, 0L)),
                        false, null));
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("s3://bucket/dst/"),
                ArgumentMatchers.isNull(), ArgumentMatchers.eq(1)))
                .thenReturn(new RemoteObjects(
                        List.of(new RemoteObject("dst/existing", "existing", null, 1L, 0L)),
                        false, null));

        Assertions.assertThrows(FileAlreadyExistsException.class,
                () -> fs.renameDirectory(Location.of("s3://bucket/src"),
                        Location.of("s3://bucket/dst"), () -> { }));
        Mockito.verify(mockStorage, Mockito.never()).copyObject(
                ArgumentMatchers.anyString(), ArgumentMatchers.anyString());
        Mockito.verify(mockStorage, Mockito.never()).deleteObjectsByKeys(
                ArgumentMatchers.anyString(), ArgumentMatchers.anyList());
    }

    // ------------------------------------------------------------------
    // S3FileIterator phantom marker filter (#5)
    // ------------------------------------------------------------------

    @Test
    void list_iteratorSkipsDirectoryMarkerEntries() throws IOException {
        // The mkdirs marker has key "dir/" which equals the listing prefix; iterator must skip it.
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("s3://bucket/dir/"), ArgumentMatchers.any()))
                .thenReturn(new RemoteObjects(
                        List.of(
                                new RemoteObject("dir/", "", null, 0L, 0L),
                                new RemoteObject("dir/file.txt", "file.txt", null, 7L, 0L),
                                new RemoteObject("dir/sub/", "sub/", null, 0L, 0L)),
                        false, null));

        List<org.apache.doris.filesystem.FileEntry> emitted = new java.util.ArrayList<>();
        try (org.apache.doris.filesystem.FileIterator it =
                fs.list(Location.of("s3://bucket/dir"))) {
            while (it.hasNext()) {
                emitted.add(it.next());
            }
        }
        Assertions.assertEquals(1, emitted.size(), "expected only the real file");
        Assertions.assertEquals("s3://bucket/dir/file.txt", emitted.get(0).location().uri());
    }

    // ------------------------------------------------------------------
    // listFiles() — direct-children only (strategy a, #6)
    // ------------------------------------------------------------------

    @Test
    void listFiles_returnsOnlyDirectChildrenAndSkipsMarker() throws IOException {
        // listObjectsNonRecursive returns Contents: marker + one direct file.
        // (Sub-directories would be in CommonPrefixes; the helper drops them.)
        Mockito.when(mockStorage.listObjectsNonRecursive(
                ArgumentMatchers.eq("s3://bucket/dir/"), ArgumentMatchers.isNull()))
                .thenReturn(new RemoteObjects(
                        List.of(
                                new RemoteObject("dir/", "", null, 0L, 0L),
                                new RemoteObject("dir/file.txt", "file.txt", null, 12L, 0L)),
                        false, null));

        List<org.apache.doris.filesystem.FileEntry> files =
                fs.listFiles(Location.of("s3://bucket/dir"));

        Assertions.assertEquals(1, files.size());
        Assertions.assertEquals("s3://bucket/dir/file.txt", files.get(0).location().uri());
        // Recursive flat list MUST NOT have been used.
        Mockito.verify(mockStorage, Mockito.never()).listObjects(
                ArgumentMatchers.anyString(), ArgumentMatchers.any());
    }

    // ------------------------------------------------------------------
    // list() - directory boundary enforcement
    // ------------------------------------------------------------------

    @Test
    void list_appendsTrailingSlashToAvoidSiblingPrefixPollution() throws IOException {
        // Simulate object storage where "tpcds1000/store" shares a prefix
        // with sibling directories "tpcds1000/store_sales", "tpcds1000/store_returns".
        // The implementation must list with prefix "tpcds1000/store/" so that
        // sibling objects are not pulled in.
        RemoteObjects page = new RemoteObjects(
                List.of(
                        new RemoteObject("tpcds1000/store/data.orc", "data.orc", null, 1234L, 0L)),
                false, null);
        Mockito.when(mockStorage.listObjectsNonRecursive(
                ArgumentMatchers.eq("oss://bucket/tpcds1000/store/"),
                ArgumentMatchers.any())).thenReturn(page);

        List<org.apache.doris.filesystem.FileEntry> files = fs.listFiles(Location.of("oss://bucket/tpcds1000/store"));

        Mockito.verify(mockStorage).listObjectsNonRecursive(
                ArgumentMatchers.eq("oss://bucket/tpcds1000/store/"),
                ArgumentMatchers.any());
        Mockito.verify(mockStorage, Mockito.never()).listObjectsNonRecursive(
                ArgumentMatchers.eq("oss://bucket/tpcds1000/store"),
                ArgumentMatchers.any());
        Assertions.assertEquals(1, files.size());
    }

    @Test
    void list_doesNotDoubleSlashWhenLocationAlreadyEndsWithSlash() throws IOException {
        RemoteObjects page = new RemoteObjects(List.of(), false, null);
        Mockito.when(mockStorage.listObjectsNonRecursive(ArgumentMatchers.anyString(), ArgumentMatchers.any()))
                .thenReturn(page);

        fs.listFiles(Location.of("s3://bucket/dir/"));

        Mockito.verify(mockStorage).listObjectsNonRecursive(
                ArgumentMatchers.eq("s3://bucket/dir/"),
                ArgumentMatchers.any());
    }

    // ------------------------------------------------------------------
    // longestNonGlobPrefix() - package-visible static
    // ------------------------------------------------------------------

    @Test
    void longestNonGlobPrefix_noGlobReturnsFullPattern() {
        Assertions.assertEquals("data/2024/file.csv", S3FileSystem.longestNonGlobPrefix("data/2024/file.csv"));
    }

    @Test
    void longestNonGlobPrefix_starTruncatesAtStar() {
        Assertions.assertEquals("data/2024/", S3FileSystem.longestNonGlobPrefix("data/2024/*.csv"));
    }

    @Test
    void longestNonGlobPrefix_questionMarkTruncates() {
        Assertions.assertEquals("data/file", S3FileSystem.longestNonGlobPrefix("data/file?.csv"));
    }

    @Test
    void longestNonGlobPrefix_bracketTruncates() {
        Assertions.assertEquals("data/", S3FileSystem.longestNonGlobPrefix("data/[abc].csv"));
    }

    @Test
    void longestNonGlobPrefix_braceTruncates() {
        Assertions.assertEquals("data/", S3FileSystem.longestNonGlobPrefix("data/{1..3}.csv"));
    }

    @Test
    void longestNonGlobPrefix_backslashTruncates() {
        Assertions.assertEquals("data/", S3FileSystem.longestNonGlobPrefix("data/\\*.csv"));
    }

    @Test
    void longestNonGlobPrefix_emptyForLeadingStar() {
        Assertions.assertEquals("", S3FileSystem.longestNonGlobPrefix("*.csv"));
    }

    // ------------------------------------------------------------------
    // newOutputFile().create() / createOrOverwrite()
    // ------------------------------------------------------------------

    @Test
    void create_throwsFileAlreadyExistsWhenObjectExists() throws IOException {
        Mockito.when(mockStorage.headObject("s3://bucket/existing"))
                .thenReturn(new RemoteObject("existing", "existing", null, 100L, 0L));

        DorisOutputFile out = fs.newOutputFile(Location.of("s3://bucket/existing"));
        Assertions.assertThrows(FileAlreadyExistsException.class, out::create);
    }

    @Test
    void create_succeedsWhenObjectDoesNotExist() throws IOException {
        Mockito.when(mockStorage.headObject("s3://bucket/new"))
                .thenThrow(new FileNotFoundException("missing"));

        DorisOutputFile out = fs.newOutputFile(Location.of("s3://bucket/new"));
        // Should not throw; underlying stream constructor does no I/O.
        Assertions.assertNotNull(out.create());
    }

    @Test
    void create_propagatesNonNotFoundIOException() throws IOException {
        IOException io500 = new IOException("server error");
        Mockito.when(mockStorage.headObject("s3://bucket/err")).thenThrow(io500);

        DorisOutputFile out = fs.newOutputFile(Location.of("s3://bucket/err"));
        IOException thrown = Assertions.assertThrows(IOException.class, out::create);
        Assertions.assertEquals(io500, thrown);
    }

    @Test
    void createOrOverwrite_doesNotProbeForExistence() throws IOException {
        DorisOutputFile out = fs.newOutputFile(Location.of("s3://bucket/anything"));
        Assertions.assertNotNull(out.createOrOverwrite());
        Mockito.verify(mockStorage, Mockito.never()).headObject(ArgumentMatchers.anyString());
    }

    // ------------------------------------------------------------------
    // close()
    // ------------------------------------------------------------------

    @Test
    void close_delegatesToObjStorage() throws IOException {
        fs.close();
        Mockito.verify(mockStorage).close();
    }

    // ------------------------------------------------------------------
    // deleteFiles() override (#12)
    // ------------------------------------------------------------------

    @Test
    void deleteFiles_groupsByBucketAndCallsBatchDeleteOncePerBucket() throws IOException {
        fs.deleteFiles(List.of(
                Location.of("s3://b1/a.txt"),
                Location.of("s3://b1/b.txt"),
                Location.of("s3://b2/c.txt")));

        Mockito.verify(mockStorage).deleteObjectsByKeys(
                ArgumentMatchers.eq("b1"),
                ArgumentMatchers.eq(List.of("a.txt", "b.txt")));
        Mockito.verify(mockStorage).deleteObjectsByKeys(
                ArgumentMatchers.eq("b2"),
                ArgumentMatchers.eq(List.of("c.txt")));
        // No per-key DeleteObject calls.
        Mockito.verify(mockStorage, Mockito.never()).deleteObject(ArgumentMatchers.anyString());
    }

    @Test
    void deleteFiles_emptyInputIsNoOp() throws IOException {
        fs.deleteFiles(List.of());
        Mockito.verify(mockStorage, Mockito.never()).deleteObjectsByKeys(
                ArgumentMatchers.anyString(), ArgumentMatchers.anyList());
    }

    // ------------------------------------------------------------------
    // globToRegex() — glob → regex conversion (#15)
    // ------------------------------------------------------------------

    @Test
    void globToRegex_starDoesNotCrossSlash() {
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(
                S3FileSystem.globToRegex("*.csv"));
        Assertions.assertTrue(p.matcher("foo.csv").matches());
        Assertions.assertFalse(p.matcher("dir/foo.csv").matches(),
                "single * must not cross /");
    }

    @Test
    void globToRegex_singleStarBoundedToOneLevel() {
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(
                S3FileSystem.globToRegex("dir/*.csv"));
        Assertions.assertTrue(p.matcher("dir/a.csv").matches());
        Assertions.assertFalse(p.matcher("dir/sub/x.csv").matches());
    }

    @Test
    void globToRegex_doubleStarCrossesSlash() {
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(
                S3FileSystem.globToRegex("**/*.csv"));
        Assertions.assertTrue(p.matcher("dir/a.csv").matches());
        Assertions.assertTrue(p.matcher("dir/sub/a.csv").matches());
        Assertions.assertTrue(p.matcher("a/b/c/d.csv").matches());
        // Sanity: a non-csv name is rejected.
        Assertions.assertFalse(p.matcher("dir/a.txt").matches());
    }

    @Test
    void globToRegex_characterClass() {
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(
                S3FileSystem.globToRegex("file[abc].csv"));
        Assertions.assertTrue(p.matcher("filea.csv").matches());
        Assertions.assertTrue(p.matcher("filec.csv").matches());
        Assertions.assertFalse(p.matcher("filed.csv").matches());
    }

    @Test
    void globToRegex_braceAlternation() {
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(
                S3FileSystem.globToRegex("file_{x,y}.csv"));
        Assertions.assertTrue(p.matcher("file_x.csv").matches());
        Assertions.assertTrue(p.matcher("file_y.csv").matches());
        Assertions.assertFalse(p.matcher("file_z.csv").matches());
    }

    @Test
    void globToRegex_keysWithSpaceAreLiteral() {
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(
                S3FileSystem.globToRegex("file with space.csv"));
        Assertions.assertTrue(p.matcher("file with space.csv").matches());
    }

    @Test
    void globToRegex_keysWithColonAndBackslash() {
        // Glob "foo:bar/*" must accept S3 keys that contain ':' (illegal in Windows paths).
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(
                S3FileSystem.globToRegex("foo:bar/*"));
        Assertions.assertTrue(p.matcher("foo:bar/qux").matches());
        // Backslash in the key is not special; literal match still works.
        java.util.regex.Pattern p2 = java.util.regex.Pattern.compile(
                S3FileSystem.globToRegex("data/*.csv"));
        Assertions.assertTrue(p2.matcher("data/has\\backslash.csv").matches());
    }

    @Test
    void globToRegex_questionMarkMatchesSingleChar() {
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(
                S3FileSystem.globToRegex("file?.csv"));
        Assertions.assertTrue(p.matcher("fileA.csv").matches());
        Assertions.assertFalse(p.matcher("file.csv").matches());
        Assertions.assertFalse(p.matcher("fileAB.csv").matches());
    }

    @Test
    void globToRegex_escapedSpecialChars() {
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(
                S3FileSystem.globToRegex("file\\*.csv"));
        Assertions.assertTrue(p.matcher("file*.csv").matches());
        Assertions.assertFalse(p.matcher("filea.csv").matches());
    }
}
