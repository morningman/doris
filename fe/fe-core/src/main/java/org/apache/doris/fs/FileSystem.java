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

import org.apache.doris.fs.io.DorisInputFile;
import org.apache.doris.fs.io.DorisOutputFile;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

/**
 * Doris file system core interface.
 *
 * <p>Design principles (referencing Trino TrinoFileSystem):
 * <ol>
 *   <li>Minimal API: only expose operations Doris actually needs</li>
 *   <li>IO via {@link DorisInputFile}/{@link DorisOutputFile}</li>
 *   <li>Errors propagated via {@link IOException} (no more {@link org.apache.doris.backup.Status})</li>
 *   <li>Paths use {@link Location} strong-typed value objects</li>
 * </ol>
 *
 * <p>Backward compatibility: The old Status-based interface is renamed to
 * {@link LegacyFileSystem}, with {@link LegacyFileSystemAdapter} bridging them.
 *
 * @see LegacyFileSystem
 * @see LegacyFileSystemAdapter
 */
public interface FileSystem extends Closeable {

    // ====== IO Entry Points ======

    /**
     * Creates an input file handle for the given location.
     */
    DorisInputFile newInputFile(Location location);

    /**
     * Creates an input file handle with a known length (skips HEAD request).
     */
    DorisInputFile newInputFile(Location location, long length);

    /**
     * Creates an output file handle for the given location.
     */
    DorisOutputFile newOutputFile(Location location);

    // ====== File Operations ======

    /**
     * Checks if a file or directory exists.
     *
     * @throws IOException if an I/O error occurs
     */
    boolean exists(Location location) throws IOException;

    /**
     * Deletes a single file.
     *
     * @throws IOException if an I/O error occurs
     */
    void deleteFile(Location location) throws IOException;

    /**
     * Deletes multiple files. Default implementation loops over {@link #deleteFile}.
     *
     * @throws IOException if an I/O error occurs
     */
    default void deleteFiles(Collection<Location> locations) throws IOException {
        for (Location location : locations) {
            deleteFile(location);
        }
    }

    /**
     * Renames a file.
     *
     * @throws IOException if an I/O error occurs
     */
    void renameFile(Location source, Location target) throws IOException;

    // ====== Directory Operations ======

    /**
     * Deletes a directory and all its contents.
     *
     * @throws IOException if an I/O error occurs
     */
    void deleteDirectory(Location location) throws IOException;

    /**
     * Creates a directory (and any necessary parent directories).
     *
     * @throws IOException if an I/O error occurs
     */
    void createDirectory(Location location) throws IOException;

    /**
     * Renames a directory.
     *
     * @throws IOException if an I/O error occurs
     */
    void renameDirectory(Location source, Location target) throws IOException;

    // ====== Listing ======

    /**
     * Lists files under the given location.
     *
     * @param location the directory to list
     * @param recursive whether to list files recursively
     * @return a lazy iterator over file entries
     * @throws IOException if an I/O error occurs
     */
    FileIterator listFiles(Location location, boolean recursive) throws IOException;

    /**
     * Lists direct subdirectories of the given location.
     *
     * @param location the directory to list
     * @return a set of subdirectory locations
     * @throws IOException if an I/O error occurs
     */
    Set<Location> listDirectories(Location location) throws IOException;

    /**
     * Lists files matching a glob pattern.
     * Default implementation throws {@link UnsupportedOperationException}.
     *
     * @param pattern a glob pattern (e.g. {@code s3://bucket/prefix/*.parquet})
     * @return a lazy iterator over matching file entries
     * @throws IOException if an I/O error occurs
     */
    default FileIterator globFiles(Location pattern) throws IOException {
        throw new UnsupportedOperationException("glob not supported by " + getClass().getSimpleName());
    }

    // ====== Default close ======

    @Override
    default void close() throws IOException {
    }
}
