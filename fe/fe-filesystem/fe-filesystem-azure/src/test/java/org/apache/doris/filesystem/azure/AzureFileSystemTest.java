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

package org.apache.doris.filesystem.azure;

import org.apache.doris.filesystem.DorisOutputFile;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.GlobListing;
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
import java.io.OutputStream;
import java.util.List;

/**
 * Unit tests for {@link AzureFileSystem} using a mock {@link AzureObjStorage}.
 */
class AzureFileSystemTest {

    private AzureObjStorage mockStorage;
    private AzureFileSystem fs;

    @BeforeEach
    void setUp() {
        mockStorage = Mockito.mock(AzureObjStorage.class);
        fs = new AzureFileSystem(mockStorage);
    }

    @Test
    void list_appendsTrailingSlashToAvoidSiblingPrefixPollution() throws IOException {
        RemoteObjects page = new RemoteObjects(
                List.of(new RemoteObject("tpcds1000/store/data.orc",
                        "data.orc", null, 1234L, 0L)),
                false, null);
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("wasbs://c@a.host/tpcds1000/store/"),
                ArgumentMatchers.any())).thenReturn(page);

        List<FileEntry> files = fs.listFiles(Location.of("wasbs://c@a.host/tpcds1000/store"));

        Mockito.verify(mockStorage).listObjects(
                ArgumentMatchers.eq("wasbs://c@a.host/tpcds1000/store/"),
                ArgumentMatchers.any());
        Mockito.verify(mockStorage, Mockito.never()).listObjects(
                ArgumentMatchers.eq("wasbs://c@a.host/tpcds1000/store"),
                ArgumentMatchers.any());
        Assertions.assertEquals(1, files.size());
    }

    @Test
    void list_doesNotDoubleSlashWhenLocationAlreadyEndsWithSlash() throws IOException {
        RemoteObjects page = new RemoteObjects(List.of(), false, null);
        Mockito.when(mockStorage.listObjects(ArgumentMatchers.anyString(), ArgumentMatchers.any()))
                .thenReturn(page);

        fs.listFiles(Location.of("wasbs://c@a.host/dir/"));

        Mockito.verify(mockStorage).listObjects(
                ArgumentMatchers.eq("wasbs://c@a.host/dir/"),
                ArgumentMatchers.any());
    }

    @Test
    void delete_recursive_doesNotTouchSiblingBlobWithSameName() throws IOException {
        RemoteObjects page = new RemoteObjects(
                List.of(
                        new RemoteObject("foo/a.csv", "a.csv", null, 1L, 0L),
                        new RemoteObject("foo/b.csv", "b.csv", null, 1L, 0L)),
                false, null);
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("wasbs://c@a.host/foo/"),
                ArgumentMatchers.any())).thenReturn(page);

        fs.delete(Location.of("wasbs://c@a.host/foo"), true);

        Mockito.verify(mockStorage).deleteObject("wasbs://c@a.host/foo/a.csv");
        Mockito.verify(mockStorage).deleteObject("wasbs://c@a.host/foo/b.csv");
        Mockito.verify(mockStorage, Mockito.never()).deleteObject("wasbs://c@a.host/foo");
    }

    @Test
    void delete_nonRecursive_throwsIfDirectoryNotEmpty() throws IOException {
        RemoteObjects page = new RemoteObjects(
                List.of(new RemoteObject("foo/x.csv", "x.csv", null, 1L, 0L)),
                false, null);
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("wasbs://c@a.host/foo/"),
                ArgumentMatchers.any())).thenReturn(page);

        IOException ex = Assertions.assertThrows(IOException.class,
                () -> fs.delete(Location.of("wasbs://c@a.host/foo"), false));
        Assertions.assertTrue(ex.getMessage().contains("Directory not empty"),
                "expected 'Directory not empty' message, got: " + ex.getMessage());
        Mockito.verify(mockStorage, Mockito.never()).deleteObject(ArgumentMatchers.anyString());
    }

    @Test
    void delete_nonRecursive_silentlyAcceptsMissingTarget() throws IOException {
        RemoteObjects empty = new RemoteObjects(List.of(), false, null);
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("wasbs://c@a.host/foo/"),
                ArgumentMatchers.any())).thenReturn(empty);
        Mockito.doThrow(new IOException("Azure 404 not found"))
                .when(mockStorage).deleteObject("wasbs://c@a.host/foo");

        Assertions.assertDoesNotThrow(
                () -> fs.delete(Location.of("wasbs://c@a.host/foo"), false));
    }

    @Test
    void globListWithLimit_returnsMatchingBlobs() throws IOException {
        RemoteObjects page = new RemoteObjects(
                List.of(
                        new RemoteObject("data/a.csv", "a.csv", null, 10L, 0L),
                        new RemoteObject("data/b.csv", "b.csv", null, 20L, 0L),
                        new RemoteObject("data/c.csv", "c.csv", null, 30L, 0L),
                        new RemoteObject("data/d.json", "d.json", null, 40L, 0L),
                        new RemoteObject("data/e.txt", "e.txt", null, 50L, 0L)),
                false, null);
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("wasbs://c@a.host/data/"),
                ArgumentMatchers.any())).thenReturn(page);

        GlobListing listing = fs.globListWithLimit(
                Location.of("wasbs://c@a.host/data/*.csv"), null, 0L, 0L);

        Assertions.assertEquals(3, listing.getFiles().size());
        Assertions.assertEquals("c", listing.getBucket());
        Assertions.assertEquals("data/", listing.getPrefix());
        Assertions.assertEquals("data/c.csv", listing.getMaxFile());
    }

    @Test
    void globListWithLimit_skipsDirectoryMarkers() throws IOException {
        RemoteObjects page = new RemoteObjects(
                List.of(
                        new RemoteObject("data/", "", null, 0L, 0L),
                        new RemoteObject("data/sub/", "sub/", null, 0L, 0L),
                        new RemoteObject("data/a.csv", "a.csv", null, 10L, 0L)),
                false, null);
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("wasbs://c@a.host/data/"),
                ArgumentMatchers.any())).thenReturn(page);

        GlobListing listing = fs.globListWithLimit(
                Location.of("wasbs://c@a.host/data/*.csv"), null, 0L, 0L);

        Assertions.assertEquals(1, listing.getFiles().size());
        Assertions.assertEquals("wasbs://c@a.host/data/a.csv",
                listing.getFiles().get(0).location().uri());
    }

    @Test
    void globListWithLimit_maxFileIsCursorWhenLimitHit() throws IOException {
        RemoteObjects page = new RemoteObjects(
                List.of(
                        new RemoteObject("data/a.csv", "a.csv", null, 10L, 0L),
                        new RemoteObject("data/b.csv", "b.csv", null, 10L, 0L),
                        new RemoteObject("data/c.csv", "c.csv", null, 10L, 0L),
                        new RemoteObject("data/d.csv", "d.csv", null, 10L, 0L),
                        new RemoteObject("data/e.csv", "e.csv", null, 10L, 0L)),
                false, null);
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("wasbs://c@a.host/data/"),
                ArgumentMatchers.any())).thenReturn(page);

        GlobListing listing = fs.globListWithLimit(
                Location.of("wasbs://c@a.host/data/*.csv"), null, 0L, 3L);

        Assertions.assertEquals(3, listing.getFiles().size());
        Assertions.assertEquals("data/d.csv", listing.getMaxFile());
    }

    @Test
    void globListWithLimit_maxFileIsLastKeyWhenExhausted() throws IOException {
        RemoteObjects page = new RemoteObjects(
                List.of(
                        new RemoteObject("data/a.csv", "a.csv", null, 10L, 0L),
                        new RemoteObject("data/b.csv", "b.csv", null, 10L, 0L)),
                false, null);
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("wasbs://c@a.host/data/"),
                ArgumentMatchers.any())).thenReturn(page);

        GlobListing listing = fs.globListWithLimit(
                Location.of("wasbs://c@a.host/data/*.csv"), null, 0L, 0L);

        Assertions.assertEquals(2, listing.getFiles().size());
        Assertions.assertEquals("data/b.csv", listing.getMaxFile());
    }

    @Test
    void globListWithLimit_startAfterAppliedBeforeLimit() throws IOException {
        RemoteObjects page = new RemoteObjects(
                List.of(
                        new RemoteObject("data/a.csv", "a.csv", null, 10L, 0L),
                        new RemoteObject("data/b.csv", "b.csv", null, 10L, 0L),
                        new RemoteObject("data/c.csv", "c.csv", null, 10L, 0L),
                        new RemoteObject("data/d.csv", "d.csv", null, 10L, 0L)),
                false, null);
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("wasbs://c@a.host/data/"),
                ArgumentMatchers.any())).thenReturn(page);

        GlobListing listing = fs.globListWithLimit(
                Location.of("wasbs://c@a.host/data/*.csv"), "data/b.csv", 0L, 2L);

        Assertions.assertEquals(2, listing.getFiles().size());
        Assertions.assertEquals("wasbs://c@a.host/data/c.csv", listing.getFiles().get(0).location().uri());
        Assertions.assertEquals("wasbs://c@a.host/data/d.csv", listing.getFiles().get(1).location().uri());
        // Limit not yet reached after consuming both, listing exhausted → maxFile = last returned key.
        Assertions.assertEquals("data/d.csv", listing.getMaxFile());
    }

    // ---------------------------------------------------------------------
    // F04 — listFiles(Location) glob-aware dispatch
    // ---------------------------------------------------------------------

    @Test
    void listFiles_noGlob_delegatesToDefault() throws IOException {
        // No glob → falls through to the default impl which iterates list().
        // list() appends a '/' and uses listObjects with that exact prefix.
        RemoteObjects page = new RemoteObjects(
                List.of(
                        new RemoteObject("dir/a.csv", "a.csv", null, 1L, 0L),
                        new RemoteObject("dir/b.csv", "b.csv", null, 2L, 0L)),
                false, null);
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("wasbs://c@a.host/dir/"),
                ArgumentMatchers.any())).thenReturn(page);

        List<FileEntry> result = fs.listFiles(Location.of("wasbs://c@a.host/dir"));

        Assertions.assertEquals(2, result.size());
        // Confirm the glob path was NOT taken: globListWithLimit also calls listObjects
        // with this exact prefix, so we additionally verify no regex filtering occurred
        // by checking both files are present (a glob like "dir" matches nothing literally).
        Mockito.verify(mockStorage).listObjects(
                ArgumentMatchers.eq("wasbs://c@a.host/dir/"),
                ArgumentMatchers.any());
    }

    @Test
    void listFiles_singleLevelGlob_filtersBasenamesAndSkipsMarkers() throws IOException {
        RemoteObjects page = new RemoteObjects(
                List.of(
                        new RemoteObject("data/", "", null, 0L, 0L),
                        new RemoteObject("data/sub/", "sub/", null, 0L, 0L),
                        new RemoteObject("data/sub/deep.csv", "deep.csv", null, 99L, 0L),
                        new RemoteObject("data/a.csv", "a.csv", null, 10L, 0L),
                        new RemoteObject("data/b.json", "b.json", null, 20L, 0L),
                        new RemoteObject("data/c.csv", "c.csv", null, 30L, 0L)),
                false, null);
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("wasbs://c@a.host/data/"),
                ArgumentMatchers.any())).thenReturn(page);

        List<FileEntry> result = fs.listFiles(Location.of("wasbs://c@a.host/data/*.csv"));

        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals("wasbs://c@a.host/data/a.csv", result.get(0).location().uri());
        Assertions.assertEquals("wasbs://c@a.host/data/c.csv", result.get(1).location().uri());
    }

    @Test
    void listFiles_multiSegmentGlob_usesGlobListWithLimit() throws IOException {
        // Cross-segment glob: parent "data/*" has a wildcard, so we fall into
        // globListWithLimit, which lists at the longest non-glob prefix ("wasbs://.../").
        RemoteObjects page = new RemoteObjects(
                List.of(
                        new RemoteObject("data/2024/file.csv", "file.csv", null, 1L, 0L),
                        new RemoteObject("data/2025/file.csv", "file.csv", null, 2L, 0L),
                        new RemoteObject("data/2024/other.json", "other.json", null, 3L, 0L)),
                false, null);
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("wasbs://c@a.host/data/"),
                ArgumentMatchers.any())).thenReturn(page);

        List<FileEntry> result = fs.listFiles(Location.of("wasbs://c@a.host/data/*/file.csv"));

        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals("wasbs://c@a.host/data/2024/file.csv",
                result.get(0).location().uri());
        Assertions.assertEquals("wasbs://c@a.host/data/2025/file.csv",
                result.get(1).location().uri());
    }

    // ---------------------------------------------------------------------
    // F05 — rename refuses virtual directories
    // ---------------------------------------------------------------------

    @Test
    void rename_virtualDirectory_throws() throws IOException {
        // src "wasbs://c@a.host/foo" has children at foo/x.csv → virtual directory.
        RemoteObjects page = new RemoteObjects(
                List.of(new RemoteObject("foo/x.csv", "x.csv", null, 1L, 0L)),
                false, null);
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("wasbs://c@a.host/foo/"),
                ArgumentMatchers.any())).thenReturn(page);

        IOException ex = Assertions.assertThrows(IOException.class,
                () -> fs.rename(Location.of("wasbs://c@a.host/foo"),
                        Location.of("wasbs://c@a.host/bar")));
        Assertions.assertTrue(ex.getMessage().contains("Renaming directories is not supported"),
                "expected directory-refusal message, got: " + ex.getMessage());
        Mockito.verify(mockStorage, Mockito.never())
                .copyObject(ArgumentMatchers.anyString(), ArgumentMatchers.anyString());
    }

    // ---------------------------------------------------------------------
    // F07 — rename compensates when the source delete fails
    // ---------------------------------------------------------------------

    @Test
    void rename_compensatesWhenDeleteFails() throws IOException {
        // No children → not a virtual directory; rename proceeds with copy+delete.
        RemoteObjects empty = new RemoteObjects(List.of(), false, null);
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("wasbs://c@a.host/src.csv/"),
                ArgumentMatchers.any())).thenReturn(empty);
        // Copy succeeds, source delete fails.
        Mockito.doThrow(new IOException("source delete blew up"))
                .when(mockStorage).deleteObject("wasbs://c@a.host/src.csv");

        IOException ex = Assertions.assertThrows(IOException.class,
                () -> fs.rename(Location.of("wasbs://c@a.host/src.csv"),
                        Location.of("wasbs://c@a.host/dst.csv")));

        Assertions.assertTrue(ex.getMessage().contains("compensation"),
                "expected compensation message, got: " + ex.getMessage());
        // Compensating delete on dst was attempted.
        Mockito.verify(mockStorage).deleteObject("wasbs://c@a.host/dst.csv");
        Mockito.verify(mockStorage).copyObject(
                "wasbs://c@a.host/src.csv", "wasbs://c@a.host/dst.csv");
    }

    // ---------------------------------------------------------------------
    // F06 — renameDirectory branches on directory presence
    // ---------------------------------------------------------------------

    @Test
    void renameDirectory_runsWhenSrcNotExistsCallback_whenNoMarkerAndNoChildren()
            throws IOException {
        RemoteObjects empty = new RemoteObjects(List.of(), false, null);
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("wasbs://c@a.host/missing/"),
                ArgumentMatchers.any())).thenReturn(empty);
        Mockito.when(mockStorage.headObject("wasbs://c@a.host/missing/"))
                .thenThrow(new FileNotFoundException("404"));

        boolean[] called = {false};
        fs.renameDirectory(
                Location.of("wasbs://c@a.host/missing"),
                Location.of("wasbs://c@a.host/dst"),
                () -> called[0] = true);

        Assertions.assertTrue(called[0], "whenSrcNotExists callback must be run");
    }

    @Test
    void renameDirectory_throwsUnsupported_whenChildrenPresent() throws IOException {
        RemoteObjects page = new RemoteObjects(
                List.of(new RemoteObject("dir/a.csv", "a.csv", null, 1L, 0L)),
                false, null);
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("wasbs://c@a.host/dir/"),
                ArgumentMatchers.any())).thenReturn(page);

        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> fs.renameDirectory(
                        Location.of("wasbs://c@a.host/dir"),
                        Location.of("wasbs://c@a.host/dst"),
                        () -> Assertions.fail("callback must not run when src exists")));
    }

    // ---------------------------------------------------------------------
    // F08 — mkdirs is idempotent and never overwrites
    // ---------------------------------------------------------------------

    @Test
    void mkdirs_idempotent_doesNotOverwriteExistingMarker() throws IOException {
        Mockito.when(mockStorage.headObject("wasbs://c@a.host/dir/"))
                .thenReturn(new RemoteObject("dir/", "", null, 0L, 0L));

        fs.mkdirs(Location.of("wasbs://c@a.host/dir"));

        Mockito.verify(mockStorage, Mockito.never())
                .putObject(ArgumentMatchers.anyString(), ArgumentMatchers.any(RequestBody.class));
    }

    @Test
    void mkdirs_putsWhenMarkerMissing() throws IOException {
        Mockito.when(mockStorage.headObject("wasbs://c@a.host/dir/"))
                .thenThrow(new FileNotFoundException("404"));

        fs.mkdirs(Location.of("wasbs://c@a.host/dir"));

        Mockito.verify(mockStorage).putObject(
                ArgumentMatchers.eq("wasbs://c@a.host/dir/"),
                ArgumentMatchers.any(RequestBody.class));
    }

    // ---------------------------------------------------------------------
    // F09 — newOutputFile.create() vs createOrOverwrite()
    // ---------------------------------------------------------------------

    @Test
    void create_throwsIfDestinationExists() throws IOException {
        Mockito.when(mockStorage.headObject("wasbs://c@a.host/file.csv"))
                .thenReturn(new RemoteObject("file.csv", "", null, 1L, 0L));

        DorisOutputFile out = fs.newOutputFile(Location.of("wasbs://c@a.host/file.csv"));

        IOException ex = Assertions.assertThrows(IOException.class, out::create);
        Assertions.assertTrue(ex.getMessage().contains("File already exists"),
                "expected 'File already exists', got: " + ex.getMessage());
    }

    @Test
    void create_writesWhenDestinationMissing() throws IOException {
        Mockito.when(mockStorage.headObject("wasbs://c@a.host/new.csv"))
                .thenThrow(new FileNotFoundException("404"));

        DorisOutputFile out = fs.newOutputFile(Location.of("wasbs://c@a.host/new.csv"));
        try (OutputStream stream = out.create()) {
            stream.write(new byte[]{1, 2, 3});
        }

        Mockito.verify(mockStorage).putObject(
                ArgumentMatchers.eq("wasbs://c@a.host/new.csv"),
                ArgumentMatchers.any(RequestBody.class));
    }

    // ---------------------------------------------------------------------
    // F10 — exists() recognises virtual directories
    // ---------------------------------------------------------------------

    @Test
    void exists_returnsTrueWhenExactKeyExists() throws IOException {
        Mockito.when(mockStorage.headObject("wasbs://c@a.host/file.csv"))
                .thenReturn(new RemoteObject("file.csv", "", null, 1L, 0L));

        Assertions.assertTrue(fs.exists(Location.of("wasbs://c@a.host/file.csv")));
    }

    @Test
    void exists_returnsTrueForVirtualDirectoryWithChildren() throws IOException {
        Mockito.when(mockStorage.headObject("wasbs://c@a.host/dir"))
                .thenThrow(new FileNotFoundException("404"));
        RemoteObjects page = new RemoteObjects(
                List.of(new RemoteObject("dir/a.csv", "a.csv", null, 1L, 0L)),
                false, null);
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("wasbs://c@a.host/dir/"),
                ArgumentMatchers.any())).thenReturn(page);

        Assertions.assertTrue(fs.exists(Location.of("wasbs://c@a.host/dir")));
    }

    @Test
    void exists_returnsFalseWhenNeitherKeyNorChildren() throws IOException {
        Mockito.when(mockStorage.headObject("wasbs://c@a.host/nope"))
                .thenThrow(new FileNotFoundException("404"));
        RemoteObjects empty = new RemoteObjects(List.of(), false, null);
        Mockito.when(mockStorage.listObjects(
                ArgumentMatchers.eq("wasbs://c@a.host/nope/"),
                ArgumentMatchers.any())).thenReturn(empty);

        Assertions.assertFalse(fs.exists(Location.of("wasbs://c@a.host/nope")));
    }
}
