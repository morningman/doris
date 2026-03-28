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

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Legacy file system interface using {@link Status} return values.
 *
 * <p>This interface is the renamed version of the original {@code FileSystem} interface.
 * It is retained for backward compatibility during the gradual migration to
 * the new {@link FileSystem} interface which uses {@code IOException}-based error handling
 * and {@link Location}-based path types.
 *
 * @deprecated Use {@link FileSystem} instead. This interface will be removed once all
 *     callers are migrated to the new API.
 * @see FileSystem
 * @see LegacyFileSystemAdapter
 */
@Deprecated
public interface LegacyFileSystem {
    Map<String, String> getProperties();

    Status exists(String remotePath);

    default Status directoryExists(String dir) {
        return exists(dir);
    }

    Status downloadWithFileSize(String remoteFilePath, String localFilePath, long fileSize);

    Status upload(String localPath, String remotePath);

    Status directUpload(String content, String remoteFile);

    Status rename(String origFilePath, String destFilePath);

    default Status renameDir(String origFilePath, String destFilePath) {
        return renameDir(origFilePath, destFilePath, () -> {});
    }

    default Status renameDir(String origFilePath,
                             String destFilePath,
                             Runnable runWhenPathNotExist) {
        throw new UnsupportedOperationException("Unsupported operation rename dir on current file system.");
    }

    default Status deleteAll(List<String> remotePaths) {
        for (String remotePath : remotePaths) {
            Status deleteStatus = delete(remotePath);
            if (!deleteStatus.ok()) {
                return deleteStatus;
            }
        }
        return Status.OK;
    }

    Status delete(String remotePath);

    default Status deleteDirectory(String dir) {
        return delete(dir);
    }

    Status makeDir(String remotePath);

    Status listFiles(String remotePath, boolean recursive, List<RemoteFile> result);

    default Status globList(String remotePath, List<RemoteFile> result) {
        return globList(remotePath, result, true);
    }

    Status globList(String remotePath, List<RemoteFile> result, boolean fileNameOnly);

    default GlobListResult globListWithLimit(String remotePath, List<RemoteFile> result,
            String startFile, long fileSizeLimit, long fileNumLimit) {
        throw new UnsupportedOperationException("Unsupported operation glob list with limit on current file system.");
    }

    default Status listDirectories(String remotePath, Set<String> result) {
        throw new UnsupportedOperationException("Unsupported operation list directories on current file system.");
    }

    default DorisOutputFile newOutputFile(ParsedPath path) {
        throw new UnsupportedOperationException("Unsupported operation new output file on current file system.");
    }

    default DorisInputFile newInputFile(ParsedPath path) {
        return newInputFile(path, -1);
    }

    default DorisInputFile newInputFile(ParsedPath path, long length) {
        throw new UnsupportedOperationException("Unsupported operation new input file on current file system.");
    }
}
