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

import org.apache.doris.backup.Status;
import org.apache.doris.fs.io.DorisInputFile;
import org.apache.doris.fs.io.DorisOutputFile;
import org.apache.doris.fs.io.ParsedPath;
import org.apache.doris.fs.remote.RemoteFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Adapter that bridges the new {@link FileSystem} interface with the legacy
 * {@link LegacyFileSystem} interface.
 *
 * <p>Subclasses implement the legacy Status-based methods. This adapter provides
 * default implementations of the new IOException-based {@link FileSystem} methods
 * by delegating to the legacy methods and converting Status to exceptions.
 *
 * <p>All existing {@link org.apache.doris.fs.remote.RemoteFileSystem} subclasses
 * transition through this adapter, so both old and new callers can use them.
 *
 * @see FileSystem
 * @see LegacyFileSystem
 */
public abstract class LegacyFileSystemAdapter implements FileSystem, LegacyFileSystem {

    // ====== FileSystem → LegacyFileSystem bridging ======

    @Override
    public DorisInputFile newInputFile(Location location) {
        return newInputFile(new ParsedPath(location.toString()));
    }

    @Override
    public DorisInputFile newInputFile(Location location, long length) {
        return newInputFile(new ParsedPath(location.toString()), length);
    }

    @Override
    public DorisOutputFile newOutputFile(Location location) {
        return newOutputFile(new ParsedPath(location.toString()));
    }

    @Override
    public boolean exists(Location location) throws IOException {
        Status status = exists(location.toString());
        if (status.ok()) {
            return true;
        }
        if (status.getErrCode() == Status.ErrCode.NOT_FOUND) {
            return false;
        }
        throw new IOException("Failed to check existence of " + location + ": " + status.getErrMsg());
    }

    @Override
    public void deleteFile(Location location) throws IOException {
        Status status = delete(location.toString());
        if (!status.ok()) {
            throw new IOException("Failed to delete " + location + ": " + status.getErrMsg());
        }
    }

    @Override
    public void deleteDirectory(Location location) throws IOException {
        Status status = deleteDirectory(location.toString());
        if (!status.ok()) {
            throw new IOException("Failed to delete directory " + location + ": " + status.getErrMsg());
        }
    }

    @Override
    public void createDirectory(Location location) throws IOException {
        Status status = makeDir(location.toString());
        if (!status.ok()) {
            throw new IOException("Failed to create directory " + location + ": " + status.getErrMsg());
        }
    }

    @Override
    public void renameFile(Location source, Location target) throws IOException {
        Status status = rename(source.toString(), target.toString());
        if (!status.ok()) {
            throw new IOException("Failed to rename " + source + " to " + target + ": " + status.getErrMsg());
        }
    }

    @Override
    public void renameDirectory(Location source, Location target) throws IOException {
        Status status = renameDir(source.toString(), target.toString());
        if (!status.ok()) {
            throw new IOException("Failed to rename dir " + source + " to " + target + ": " + status.getErrMsg());
        }
    }

    @Override
    public FileIterator listFiles(Location location, boolean recursive) throws IOException {
        List<RemoteFile> result = new ArrayList<>();
        Status status = listFiles(location.toString(), recursive, result);
        if (!status.ok()) {
            throw new IOException("Failed to list files at " + location + ": " + status.getErrMsg());
        }
        List<FileEntry> entries = result.stream()
                .map(rf -> FileEntry.fromRemoteFile(rf, location.toString()))
                .collect(Collectors.toList());
        return FileIterator.fromList(entries);
    }

    @Override
    public Set<Location> listDirectories(Location location) throws IOException {
        Set<String> result = new HashSet<>();
        Status status = listDirectories(location.toString(), result);
        if (!status.ok()) {
            throw new IOException("Failed to list directories at " + location + ": " + status.getErrMsg());
        }
        return result.stream().map(Location::of).collect(Collectors.toSet());
    }

    @Override
    public FileIterator globFiles(Location pattern) throws IOException {
        List<RemoteFile> result = new ArrayList<>();
        Status status = globList(pattern.toString(), result, false);
        if (!status.ok()) {
            throw new IOException("Glob failed for " + pattern + ": " + status.getErrMsg());
        }
        List<FileEntry> entries = result.stream()
                .map(rf -> FileEntry.fromRemoteFile(rf, pattern.toString()))
                .collect(Collectors.toList());
        return FileIterator.fromList(entries);
    }
}
