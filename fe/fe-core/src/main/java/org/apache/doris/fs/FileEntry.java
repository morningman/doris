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

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable file/directory metadata. Zero Hadoop dependency in its public API.
 * Replaces {@link RemoteFile}.
 *
 * <p>For HDFS files, optional {@link BlockInfo} records are available for data locality
 * optimization. Blob-storage files do not need block information.
 */
public final class FileEntry {

    private final Location location;
    private final boolean isDirectory;
    private final long length;        // file size in bytes; -1 for directories
    private final long lastModified;  // epoch millis
    private final List<BlockInfo> blocks; // nullable, only for HDFS

    public FileEntry(Location location, boolean isDirectory, long length, long lastModified) {
        this(location, isDirectory, length, lastModified, null);
    }

    public FileEntry(Location location, boolean isDirectory, long length,
                     long lastModified, List<BlockInfo> blocks) {
        this.location = Objects.requireNonNull(location, "location is null");
        this.isDirectory = isDirectory;
        this.length = length;
        this.lastModified = lastModified;
        this.blocks = blocks == null ? null : Collections.unmodifiableList(new ArrayList<>(blocks));
    }

    // ====== Getters ======

    public Location location() {
        return location;
    }

    public boolean isFile() {
        return !isDirectory;
    }

    public boolean isDirectory() {
        return isDirectory;
    }

    public long length() {
        return length;
    }

    public long lastModified() {
        return lastModified;
    }

    /**
     * Returns the file name — shortcut for {@code location().fileName()}.
     */
    public String fileName() {
        return location.fileName();
    }

    /**
     * Returns the optional block information. Only HDFS files typically have blocks.
     */
    public Optional<List<BlockInfo>> blocks() {
        return Optional.ofNullable(blocks);
    }

    // ====== HDFS block information ======

    /**
     * Block information for data locality optimization.
     * Blob storage does not need this.
     */
    public static final class BlockInfo {
        private final List<String> hosts;
        private final long offset;
        private final long length;

        public BlockInfo(List<String> hosts, long offset, long length) {
            this.hosts = hosts == null ? Collections.emptyList()
                    : Collections.unmodifiableList(new ArrayList<>(hosts));
            this.offset = offset;
            this.length = length;
        }

        public List<String> hosts() {
            return hosts;
        }

        public long offset() {
            return offset;
        }

        public long length() {
            return length;
        }

        @Override
        public String toString() {
            return "BlockInfo{hosts=" + hosts + ", offset=" + offset + ", length=" + length + "}";
        }
    }

    // ====== Backward-compatible conversions ======

    /**
     * Converts this FileEntry to a legacy {@link RemoteFile}.
     *
     * @return a RemoteFile
     * @deprecated Use FileEntry instead of RemoteFile
     */
    @Deprecated
    public RemoteFile toRemoteFile() {
        BlockLocation[] blockLocations = null;
        if (blocks != null && !blocks.isEmpty()) {
            blockLocations = new BlockLocation[blocks.size()];
            for (int i = 0; i < blocks.size(); i++) {
                BlockInfo bi = blocks.get(i);
                blockLocations[i] = new BlockLocation(
                        null, // names
                        bi.hosts().toArray(new String[0]),
                        bi.offset(),
                        bi.length());
            }
        }
        return new RemoteFile(
                location.fileName(),
                new Path(location.toString()),
                !isDirectory,
                isDirectory,
                length,
                0, // blockSize — not tracked in FileEntry
                lastModified,
                blockLocations);
    }

    /**
     * Creates a FileEntry from a legacy {@link RemoteFile}.
     *
     * <p>The basePath is used to reconstruct the full location when RemoteFile only
     * has the file name without the full path.
     *
     * @param rf the RemoteFile to convert
     * @param basePath the base path to prepend if RemoteFile has no full path
     * @return a new FileEntry
     * @deprecated Use FileEntry directly
     */
    @Deprecated
    public static FileEntry fromRemoteFile(RemoteFile rf, String basePath) {
        // Determine the full location string
        String locationStr;
        if (rf.getPath() != null) {
            locationStr = rf.getPath().toString();
        } else {
            // RemoteFile only has a name, combine with basePath
            String base = basePath.endsWith("/") ? basePath : basePath + "/";
            locationStr = base + rf.getName();
        }

        // Convert block locations if present
        List<BlockInfo> blocks = null;
        if (rf.getBlockLocations() != null && rf.getBlockLocations().length > 0) {
            blocks = new ArrayList<>();
            for (BlockLocation bl : rf.getBlockLocations()) {
                try {
                    blocks.add(new BlockInfo(
                            Arrays.asList(bl.getHosts()),
                            bl.getOffset(),
                            bl.getLength()));
                } catch (IOException e) {
                    // BlockLocation.getHosts() should not throw for standard Hadoop implementation
                    throw new RuntimeException("Failed to get block hosts", e);
                }
            }
        }

        return new FileEntry(
                Location.of(locationStr),
                rf.isDirectory(),
                rf.getSize(),
                rf.getModificationTime(),
                blocks);
    }

    @Override
    public String toString() {
        return "FileEntry{"
                + "location=" + location
                + ", isDirectory=" + isDirectory
                + ", length=" + length
                + ", lastModified=" + lastModified
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileEntry that = (FileEntry) o;
        return isDirectory == that.isDirectory
                && length == that.length
                && lastModified == that.lastModified
                && Objects.equals(location, that.location);
    }

    @Override
    public int hashCode() {
        return Objects.hash(location, isDirectory, length, lastModified);
    }
}
