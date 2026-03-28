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

package org.apache.doris.fs;

import org.apache.doris.fs.remote.RemoteFile;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class FileEntryTest {

    @Test
    public void testBasicFileEntry() {
        Location loc = Location.of("s3://bucket/path/file.parquet");
        FileEntry entry = new FileEntry(loc, false, 1024, 1000L);

        Assert.assertEquals(loc, entry.location());
        Assert.assertFalse(entry.isDirectory());
        Assert.assertTrue(entry.isFile());
        Assert.assertEquals(1024, entry.length());
        Assert.assertEquals(1000L, entry.lastModified());
        Assert.assertEquals("file.parquet", entry.fileName());
    }

    @Test
    public void testFileNameExtraction() {
        FileEntry entry = new FileEntry(
                Location.of("hdfs://nn/warehouse/table/data.orc"),
                false, 0, 0);
        Assert.assertEquals("data.orc", entry.fileName());
    }

    @Test
    public void testBlockInfo() {
        List<FileEntry.BlockInfo> blocks = Arrays.asList(
                new FileEntry.BlockInfo(Arrays.asList("host1:50010", "host2:50010"), 0, 1024),
                new FileEntry.BlockInfo(Arrays.asList("host3:50010"), 1024, 2048)
        );

        FileEntry entry = new FileEntry(
                Location.of("hdfs://nn/data/file.orc"),
                false, 3072, 0, blocks);

        Assert.assertTrue(entry.blocks().isPresent());
        List<FileEntry.BlockInfo> resultBlocks = entry.blocks().get();
        Assert.assertEquals(2, resultBlocks.size());
        Assert.assertEquals(0, resultBlocks.get(0).offset());
        Assert.assertEquals(1024, resultBlocks.get(0).length());
        Assert.assertEquals(2, resultBlocks.get(0).hosts().size());
    }

    @Test
    public void testFromRemoteFile() {
        RemoteFile rf = new RemoteFile(new Path("hdfs://nn/path/file.dat"), false, 2048L, 512, 999L, null);
        FileEntry entry = FileEntry.fromRemoteFile(rf, "hdfs://nn/path");

        Assert.assertEquals("hdfs://nn/path/file.dat", entry.location().toString());
        Assert.assertFalse(entry.isDirectory());
        Assert.assertEquals(2048, entry.length());
    }

    @Test
    public void testToRemoteFile() {
        FileEntry entry = new FileEntry(
                Location.of("s3://bucket/prefix/file.csv"),
                false, 4096, 5000L);

        RemoteFile rf = entry.toRemoteFile();
        Assert.assertEquals("file.csv", rf.getName());
        Assert.assertFalse(rf.isDirectory());
        Assert.assertEquals(4096, rf.getSize());
    }

    @Test
    public void testDirectoryEntry() {
        FileEntry dir = new FileEntry(
                Location.of("s3://bucket/prefix/subdir"),
                true, -1, 0);

        Assert.assertTrue(dir.isDirectory());
        Assert.assertFalse(dir.isFile());
        Assert.assertEquals("subdir", dir.fileName());
    }

    @Test
    public void testNoBlockInfo() {
        FileEntry entry = new FileEntry(
                Location.of("s3://bucket/file.dat"),
                false, 100, 0);
        Assert.assertFalse(entry.blocks().isPresent());
    }

    @Test
    public void testEquality() {
        FileEntry a = new FileEntry(Location.of("s3://b/k"), false, 100, 200);
        FileEntry b = new FileEntry(Location.of("s3://b/k"), false, 100, 200);
        FileEntry c = new FileEntry(Location.of("s3://b/other"), false, 100, 200);

        Assert.assertEquals(a, b);
        Assert.assertEquals(a.hashCode(), b.hashCode());
        Assert.assertNotEquals(a, c);
    }
}
