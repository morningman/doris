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
import org.apache.doris.fs.io.DorisInputStream;
import org.apache.doris.fs.io.DorisOutputFile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * In-memory file system for unit testing.
 *
 * <p>All data is stored in a {@link ConcurrentHashMap}, making this implementation
 * thread-safe and suitable for concurrent test scenarios.
 *
 * <p>This directly implements the new {@link FileSystem} interface (IOException-based,
 * Location-typed) without going through the legacy adapter.
 */
public class MemoryFileSystem implements FileSystem {

    private final Map<String, byte[]> files = new ConcurrentHashMap<>();

    // ====== IO Entry Points ======

    @Override
    public DorisInputFile newInputFile(Location location) {
        return new MemoryInputFile(location, -1);
    }

    @Override
    public DorisInputFile newInputFile(Location location, long length) {
        return new MemoryInputFile(location, length);
    }

    @Override
    public DorisOutputFile newOutputFile(Location location) {
        return new MemoryOutputFile(location);
    }

    // ====== File Operations ======

    @Override
    public boolean exists(Location location) {
        String key = normalize(location);
        return files.containsKey(key) || isDirectory(key);
    }

    @Override
    public void deleteFile(Location location) throws IOException {
        String key = normalize(location);
        if (files.remove(key) == null) {
            throw new FileNotFoundException("File not found: " + location);
        }
    }

    @Override
    public void renameFile(Location source, Location target) throws IOException {
        String srcKey = normalize(source);
        byte[] data = files.remove(srcKey);
        if (data == null) {
            throw new FileNotFoundException("Source not found: " + source);
        }
        files.put(normalize(target), data);
    }

    // ====== Directory Operations ======

    @Override
    public void deleteDirectory(Location location) throws IOException {
        String prefix = normalize(location);
        if (!prefix.endsWith("/")) {
            prefix = prefix + "/";
        }
        String dirPrefix = prefix;
        files.keySet().removeIf(key -> key.startsWith(dirPrefix));
    }

    @Override
    public void createDirectory(Location location) {
        // No-op: directories are implicit in a flat key-value store
    }

    @Override
    public void renameDirectory(Location source, Location target) throws IOException {
        String srcPrefix = normalize(source);
        if (!srcPrefix.endsWith("/")) {
            srcPrefix = srcPrefix + "/";
        }
        String dstPrefix = normalize(target);
        if (!dstPrefix.endsWith("/")) {
            dstPrefix = dstPrefix + "/";
        }
        String finalSrcPrefix = srcPrefix;
        String finalDstPrefix = dstPrefix;
        // Collect keys first to avoid ConcurrentModificationException
        Set<String> matchingKeys = files.keySet().stream()
                .filter(key -> key.startsWith(finalSrcPrefix))
                .collect(Collectors.toSet());
        for (String key : matchingKeys) {
            byte[] data = files.remove(key);
            if (data != null) {
                String newKey = finalDstPrefix + key.substring(finalSrcPrefix.length());
                files.put(newKey, data);
            }
        }
    }

    // ====== Listing ======

    @Override
    public FileIterator listFiles(Location location, boolean recursive) {
        String prefix = normalize(location);
        if (!prefix.endsWith("/")) {
            prefix = prefix + "/";
        }
        String dirPrefix = prefix;
        java.util.List<FileEntry> entries = files.entrySet().stream()
                .filter(e -> e.getKey().startsWith(dirPrefix))
                .filter(e -> {
                    if (recursive) {
                        return true;
                    }
                    // Non-recursive: only direct children
                    String rest = e.getKey().substring(dirPrefix.length());
                    return !rest.contains("/");
                })
                .map(e -> new FileEntry(
                        Location.of(e.getKey()),
                        false,
                        e.getValue().length,
                        System.currentTimeMillis()))
                .collect(Collectors.toList());
        return FileIterator.fromList(entries);
    }

    @Override
    public Set<Location> listDirectories(Location location) {
        String prefix = normalize(location);
        if (!prefix.endsWith("/")) {
            prefix = prefix + "/";
        }
        String dirPrefix = prefix;
        return files.keySet().stream()
                .filter(key -> key.startsWith(dirPrefix))
                .map(key -> {
                    String rest = key.substring(dirPrefix.length());
                    int slash = rest.indexOf('/');
                    if (slash > 0) {
                        return dirPrefix + rest.substring(0, slash);
                    }
                    return null;
                })
                .filter(java.util.Objects::nonNull)
                .distinct()
                .map(Location::of)
                .collect(Collectors.toSet());
    }

    // ====== Test Utilities ======

    /**
     * Writes data directly to the in-memory store.
     */
    public void putFile(Location location, byte[] data) {
        files.put(normalize(location), data);
    }

    /**
     * Reads data directly from the in-memory store.
     */
    public byte[] getFileData(Location location) {
        return files.get(normalize(location));
    }

    /**
     * Returns the number of files stored.
     */
    public int fileCount() {
        return files.size();
    }

    // ====== Internal ======

    private String normalize(Location location) {
        return location.toString();
    }

    private boolean isDirectory(String key) {
        String prefix = key.endsWith("/") ? key : key + "/";
        return files.keySet().stream().anyMatch(k -> k.startsWith(prefix));
    }

    // ====== Inner Classes ======

    private class MemoryInputFile implements DorisInputFile {
        private final Location location;
        private final long knownLength;

        MemoryInputFile(Location location, long knownLength) {
            this.location = location;
            this.knownLength = knownLength;
        }

        @Override
        public Location location() {
            return location;
        }

        @Override
        public long length() throws IOException {
            if (knownLength >= 0) {
                return knownLength;
            }
            byte[] data = files.get(normalize(location));
            if (data == null) {
                throw new FileNotFoundException("File not found: " + location);
            }
            return data.length;
        }

        @Override
        public long lastModifiedTime() {
            return System.currentTimeMillis();
        }

        @Override
        public boolean exists() {
            return files.containsKey(normalize(location));
        }

        @Override
        public DorisInput newInput() throws IOException {
            byte[] data = files.get(normalize(location));
            if (data == null) {
                throw new FileNotFoundException("File not found: " + location);
            }
            return new MemoryDorisInput(data);
        }

        @Override
        public DorisInputStream newStream() throws IOException {
            byte[] data = files.get(normalize(location));
            if (data == null) {
                throw new FileNotFoundException("File not found: " + location);
            }
            return new MemoryDorisInputStream(data);
        }

        @Override
        public String toString() {
            return "MemoryInputFile{" + location + "}";
        }
    }

    private class MemoryOutputFile implements DorisOutputFile {
        private final Location location;

        MemoryOutputFile(Location location) {
            this.location = location;
        }

        @Override
        public OutputStream create() throws IOException {
            if (files.containsKey(normalize(location))) {
                throw new IOException("File already exists: " + location);
            }
            return new MemoryOutputStream(location);
        }

        @Override
        public OutputStream createOrOverwrite() {
            return new MemoryOutputStream(location);
        }

        @Override
        public Location location() {
            return location;
        }

        @Override
        public String toString() {
            return "MemoryOutputFile{" + location + "}";
        }
    }

    private class MemoryOutputStream extends ByteArrayOutputStream {
        private final Location location;

        MemoryOutputStream(Location location) {
            this.location = location;
        }

        @Override
        public void close() throws IOException {
            super.close();
            files.put(normalize(location), toByteArray());
        }
    }

    private static class MemoryDorisInput implements DorisInput {
        private final byte[] data;

        MemoryDorisInput(byte[] data) {
            this.data = data;
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
            if (position < 0 || position + length > data.length) {
                throw new IOException("Read beyond end of data: pos=" + position
                        + ", len=" + length + ", dataLen=" + data.length);
            }
            System.arraycopy(data, (int) position, buffer, offset, length);
        }

        @Override
        public void close() {
            // no-op
        }
    }

    private static class MemoryDorisInputStream extends DorisInputStream {
        private final byte[] data;
        private int pos = 0;

        MemoryDorisInputStream(byte[] data) {
            this.data = data;
        }

        @Override
        public int read() {
            if (pos >= data.length) {
                return -1;
            }
            return data[pos++] & 0xFF;
        }

        @Override
        public int read(byte[] buf, int off, int len) {
            if (pos >= data.length) {
                return -1;
            }
            int toRead = Math.min(len, data.length - pos);
            System.arraycopy(data, pos, buf, off, toRead);
            pos += toRead;
            return toRead;
        }

        @Override
        public long getPosition() {
            return pos;
        }

        @Override
        public void seek(long position) {
            this.pos = (int) position;
        }
    }
}
