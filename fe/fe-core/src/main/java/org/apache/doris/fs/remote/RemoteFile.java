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

package org.apache.doris.fs.remote;

import org.apache.doris.fs.FileEntry;
import org.apache.doris.fs.Location;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Represents a file or directory in remote storage.
 *
 * @deprecated Use {@link FileEntry} instead. This class remains for backward compatibility
 *             during the migration period. Use {@link #toFileEntry()} to convert to the new type,
 *             or {@link #fromFileEntry(FileEntry)} to convert back.
 */
@Deprecated
public class RemoteFile {
    // Only file name, not full path
    private final String name;
    private final boolean isFile;
    private final boolean isDirectory;
    private final long size;
    // Block size of underlying file system. e.g. HDFS and S3.
    // A large file will split into multiple blocks. The blocks are transparent to the user.
    // Default block size for HDFS 2.x is 128M.
    private final long blockSize;
    private long modificationTime;
    private Path path;
    BlockLocation[] blockLocations;

    public RemoteFile(String name, boolean isFile, long size, long blockSize) {
        this(name, null, isFile, !isFile, size, blockSize, 0, null);
    }

    public RemoteFile(String name, boolean isFile, long size, long blockSize, long modificationTime) {
        this(name, null, isFile, !isFile, size, blockSize, modificationTime, null);
    }

    public RemoteFile(Path path, boolean isDirectory, long size, long blockSize, long modificationTime,
            BlockLocation[] blockLocations) {
        this(path.getName(), path, !isDirectory, isDirectory, size, blockSize, modificationTime, blockLocations);
    }

    public RemoteFile(String name, Path path, boolean isFile, boolean isDirectory,
            long size, long blockSize, long modificationTime, BlockLocation[] blockLocations) {
        Preconditions.checkState(!Strings.isNullOrEmpty(name));
        this.name = name;
        this.isFile = isFile;
        this.isDirectory = isDirectory;
        this.size = size;
        this.blockSize = blockSize;
        this.modificationTime = modificationTime;
        this.path = path;
        this.blockLocations = blockLocations;
    }

    public String getName() {
        return name;
    }

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    public boolean isFile() {
        return isFile;
    }

    public boolean isDirectory() {
        return isDirectory;
    }

    public long getSize() {
        return size;
    }

    public long getBlockSize() {
        return blockSize;
    }

    public long getModificationTime() {
        return modificationTime;
    }

    public BlockLocation[] getBlockLocations() {
        return blockLocations;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("RemoteFile [");
        sb.append("name: ").append(name);
        sb.append(", isFile: ").append(isFile);
        sb.append(", isDirectory: ").append(isDirectory);
        sb.append(", size: ").append(size);
        sb.append(", blockSize: ").append(blockSize);
        sb.append(", modificationTime: ").append(modificationTime);
        sb.append(", path: ").append(path);
        sb.append("]");

        return sb.toString();
    }

    // ====== FileEntry conversion ======

    /**
     * Converts this RemoteFile to a {@link FileEntry}.
     *
     * @return a new FileEntry representing the same file metadata
     */
    public FileEntry toFileEntry() {
        String locationStr;
        if (path != null) {
            locationStr = path.toString();
        } else {
            locationStr = name;
        }

        List<FileEntry.BlockInfo> blocks = null;
        if (blockLocations != null && blockLocations.length > 0) {
            blocks = new ArrayList<>();
            for (BlockLocation bl : blockLocations) {
                try {
                    blocks.add(new FileEntry.BlockInfo(
                            Arrays.asList(bl.getHosts()),
                            bl.getOffset(),
                            bl.getLength()));
                } catch (IOException e) {
                    throw new RuntimeException("Failed to get block hosts", e);
                }
            }
        }

        return new FileEntry(
                Location.of(locationStr),
                isDirectory,
                size,
                modificationTime,
                blocks);
    }

    /**
     * Creates a RemoteFile from a {@link FileEntry}.
     *
     * @param entry the FileEntry to convert
     * @return a new RemoteFile
     */
    public static RemoteFile fromFileEntry(FileEntry entry) {
        BlockLocation[] blockLocs = null;
        if (entry.blocks().isPresent()) {
            List<FileEntry.BlockInfo> blocks = entry.blocks().get();
            blockLocs = new BlockLocation[blocks.size()];
            for (int i = 0; i < blocks.size(); i++) {
                FileEntry.BlockInfo bi = blocks.get(i);
                blockLocs[i] = new BlockLocation(
                        null,
                        bi.hosts().toArray(new String[0]),
                        bi.offset(),
                        bi.length());
            }
        }

        return new RemoteFile(
                entry.fileName(),
                new Path(entry.location().toString()),
                entry.isFile(),
                entry.isDirectory(),
                entry.length(),
                0,
                entry.lastModified(),
                blockLocs);
    }
}
