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
}
