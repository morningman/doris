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

import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.GlobListing;
import org.apache.doris.filesystem.Location;
import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
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
}
