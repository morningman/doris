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

import org.apache.doris.fs.io.DorisInput;
import org.apache.doris.fs.io.DorisInputFile;
import org.apache.doris.fs.io.DorisOutputFile;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;

public class MemoryFileSystemTest {

    private MemoryFileSystem fs;

    @Before
    public void setUp() {
        fs = new MemoryFileSystem();
    }

    // ====== Basic IO ======

    @Test
    public void testWriteAndReadFile() throws IOException {
        Location loc = Location.of("mem://bucket/test/file.txt");
        byte[] data = "hello world".getBytes(StandardCharsets.UTF_8);

        // Write
        DorisOutputFile outputFile = fs.newOutputFile(loc);
        try (OutputStream out = outputFile.createOrOverwrite()) {
            out.write(data);
        }

        // Read
        DorisInputFile inputFile = fs.newInputFile(loc);
        Assert.assertTrue(inputFile.exists());
        Assert.assertEquals(data.length, inputFile.length());

        // Read via DorisInput
        DorisInput input = inputFile.newInput();
        byte[] buf = new byte[data.length];
        input.readFully(0, buf, 0, buf.length);
        Assert.assertArrayEquals(data, buf);
        input.close();
    }

    @Test
    public void testCreateFailsIfExists() throws IOException {
        Location loc = Location.of("mem://bucket/existing.txt");
        fs.putFile(loc, "existing".getBytes(StandardCharsets.UTF_8));

        DorisOutputFile outputFile = fs.newOutputFile(loc);
        Assert.assertThrows(IOException.class, outputFile::create);
    }

    @Test
    public void testCreateOrOverwrite() throws IOException {
        Location loc = Location.of("mem://bucket/overwrite.txt");
        fs.putFile(loc, "old".getBytes(StandardCharsets.UTF_8));

        try (OutputStream out = fs.newOutputFile(loc).createOrOverwrite()) {
            out.write("new".getBytes(StandardCharsets.UTF_8));
        }

        byte[] result = fs.getFileData(loc);
        Assert.assertArrayEquals("new".getBytes(StandardCharsets.UTF_8), result);
    }

    // ====== Existence ======

    @Test
    public void testExists() {
        Location loc = Location.of("mem://bucket/exists.txt");
        Assert.assertFalse(fs.exists(loc));

        fs.putFile(loc, new byte[0]);
        Assert.assertTrue(fs.exists(loc));
    }

    @Test
    public void testExistsDirectory() {
        Location dir = Location.of("mem://bucket/dir");
        Assert.assertFalse(fs.exists(dir));

        fs.putFile(Location.of("mem://bucket/dir/file.txt"), new byte[0]);
        Assert.assertTrue(fs.exists(dir));
    }

    // ====== Delete ======

    @Test
    public void testDeleteFile() throws IOException {
        Location loc = Location.of("mem://bucket/to_delete.txt");
        fs.putFile(loc, new byte[0]);
        Assert.assertTrue(fs.exists(loc));

        fs.deleteFile(loc);
        Assert.assertFalse(fs.exists(loc));
    }

    @Test
    public void testDeleteFileNotFound() {
        Location loc = Location.of("mem://bucket/not_exist.txt");
        Assert.assertThrows(FileNotFoundException.class, () -> fs.deleteFile(loc));
    }

    @Test
    public void testDeleteDirectory() throws IOException {
        fs.putFile(Location.of("mem://bucket/dir/a.txt"), "a".getBytes());
        fs.putFile(Location.of("mem://bucket/dir/b.txt"), "b".getBytes());
        fs.putFile(Location.of("mem://bucket/dir/sub/c.txt"), "c".getBytes());
        fs.putFile(Location.of("mem://bucket/other/d.txt"), "d".getBytes());

        fs.deleteDirectory(Location.of("mem://bucket/dir"));

        Assert.assertEquals(1, fs.fileCount());
        Assert.assertTrue(fs.exists(Location.of("mem://bucket/other/d.txt")));
    }

    // ====== Rename ======

    @Test
    public void testRenameFile() throws IOException {
        Location src = Location.of("mem://bucket/src.txt");
        Location dst = Location.of("mem://bucket/dst.txt");
        fs.putFile(src, "data".getBytes());

        fs.renameFile(src, dst);

        Assert.assertFalse(fs.exists(src));
        Assert.assertTrue(fs.exists(dst));
        Assert.assertArrayEquals("data".getBytes(), fs.getFileData(dst));
    }

    @Test
    public void testRenameDirectory() throws IOException {
        fs.putFile(Location.of("mem://bucket/old/a.txt"), "a".getBytes());
        fs.putFile(Location.of("mem://bucket/old/sub/b.txt"), "b".getBytes());

        fs.renameDirectory(
                Location.of("mem://bucket/old"),
                Location.of("mem://bucket/new"));

        Assert.assertFalse(fs.exists(Location.of("mem://bucket/old/a.txt")));
        Assert.assertTrue(fs.exists(Location.of("mem://bucket/new/a.txt")));
        Assert.assertTrue(fs.exists(Location.of("mem://bucket/new/sub/b.txt")));
    }

    // ====== Listing ======

    @Test
    public void testListFilesNonRecursive() throws IOException {
        fs.putFile(Location.of("mem://bucket/dir/a.txt"), "a".getBytes());
        fs.putFile(Location.of("mem://bucket/dir/b.txt"), "b".getBytes());
        fs.putFile(Location.of("mem://bucket/dir/sub/c.txt"), "c".getBytes());

        FileIterator iter = fs.listFiles(Location.of("mem://bucket/dir"), false);
        int count = 0;
        while (iter.hasNext()) {
            FileEntry entry = iter.next();
            Assert.assertFalse(entry.isDirectory());
            count++;
        }
        Assert.assertEquals(2, count);
    }

    @Test
    public void testListFilesRecursive() throws IOException {
        fs.putFile(Location.of("mem://bucket/dir/a.txt"), "a".getBytes());
        fs.putFile(Location.of("mem://bucket/dir/sub/b.txt"), "b".getBytes());
        fs.putFile(Location.of("mem://bucket/dir/sub/deep/c.txt"), "c".getBytes());

        FileIterator iter = fs.listFiles(Location.of("mem://bucket/dir"), true);
        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            count++;
        }
        Assert.assertEquals(3, count);
    }

    @Test
    public void testListDirectories() throws IOException {
        fs.putFile(Location.of("mem://bucket/dir/a.txt"), "a".getBytes());
        fs.putFile(Location.of("mem://bucket/dir/sub1/b.txt"), "b".getBytes());
        fs.putFile(Location.of("mem://bucket/dir/sub2/c.txt"), "c".getBytes());

        Set<Location> dirs = fs.listDirectories(Location.of("mem://bucket/dir"));
        Assert.assertEquals(2, dirs.size());
        Assert.assertTrue(dirs.contains(Location.of("mem://bucket/dir/sub1")));
        Assert.assertTrue(dirs.contains(Location.of("mem://bucket/dir/sub2")));
    }

    // ====== Location on InputFile ======

    @Test
    public void testInputFileLocation() {
        Location loc = Location.of("mem://bucket/file.dat");
        DorisInputFile inputFile = fs.newInputFile(loc);
        Assert.assertEquals(loc, inputFile.location());
    }

    @Test
    public void testOutputFileLocation() {
        Location loc = Location.of("mem://bucket/out.dat");
        DorisOutputFile outputFile = fs.newOutputFile(loc);
        Assert.assertEquals(loc, outputFile.location());
    }

}
