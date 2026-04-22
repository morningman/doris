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

package org.apache.doris.filesystem.broker;

import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.Location;
import org.apache.doris.thrift.TBrokerCheckPathExistRequest;
import org.apache.doris.thrift.TBrokerCheckPathExistResponse;
import org.apache.doris.thrift.TBrokerDeletePathRequest;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TBrokerListPathRequest;
import org.apache.doris.thrift.TBrokerListResponse;
import org.apache.doris.thrift.TBrokerOperationStatus;
import org.apache.doris.thrift.TBrokerOperationStatusCode;
import org.apache.doris.thrift.TBrokerRenamePathRequest;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link BrokerSpiFileSystem} using a mock {@link BrokerClientPool}
 * and mock Thrift client. No real Broker service required.
 */
class BrokerSpiFileSystemTest {

    private BrokerClientPool mockPool;
    private TPaloBrokerService.Client mockClient;
    private BrokerSpiFileSystem fs;
    private TNetworkAddress endpoint;

    @BeforeEach
    void setUp() throws IOException {
        mockPool = Mockito.mock(BrokerClientPool.class);
        mockClient = Mockito.mock(TPaloBrokerService.Client.class);
        endpoint = new TNetworkAddress("broker-host", 9999);
        Mockito.when(mockPool.borrow(endpoint)).thenReturn(mockClient);

        Map<String, String> params = new HashMap<>();
        params.put("username", "testuser");
        fs = new BrokerSpiFileSystem(endpoint, "fe-client", params, mockPool);
    }

    // ------------------------------------------------------------------
    // exists()
    // ------------------------------------------------------------------

    @Test
    void exists_returnsTrueWhenPathExists() throws Exception {
        TBrokerOperationStatus status = new TBrokerOperationStatus(TBrokerOperationStatusCode.OK);
        TBrokerCheckPathExistResponse resp = new TBrokerCheckPathExistResponse(status, true);
        Mockito.when(mockClient.checkPathExist(ArgumentMatchers.any(TBrokerCheckPathExistRequest.class))).thenReturn(resp);

        Assertions.assertTrue(fs.exists(Location.of("hdfs:///test/file.txt")));
        Mockito.verify(mockPool).returnGood(endpoint, mockClient);
    }

    @Test
    void exists_returnsFalseWhenPathDoesNotExist() throws Exception {
        TBrokerOperationStatus status = new TBrokerOperationStatus(TBrokerOperationStatusCode.OK);
        TBrokerCheckPathExistResponse resp = new TBrokerCheckPathExistResponse(status, false);
        Mockito.when(mockClient.checkPathExist(ArgumentMatchers.any(TBrokerCheckPathExistRequest.class))).thenReturn(resp);

        Assertions.assertFalse(fs.exists(Location.of("hdfs:///test/missing")));
    }

    @Test
    void exists_returnsFalseForFileNotFoundStatus() throws Exception {
        TBrokerOperationStatus status = new TBrokerOperationStatus(TBrokerOperationStatusCode.FILE_NOT_FOUND);
        TBrokerCheckPathExistResponse resp = new TBrokerCheckPathExistResponse(status, false);
        Mockito.when(mockClient.checkPathExist(ArgumentMatchers.any(TBrokerCheckPathExistRequest.class))).thenReturn(resp);

        Assertions.assertFalse(fs.exists(Location.of("hdfs:///test/gone")));
    }

    @Test
    void exists_throwsIOExceptionOnBrokerError() throws Exception {
        TBrokerOperationStatus status = new TBrokerOperationStatus(TBrokerOperationStatusCode.INVALID_INPUT_FILE_PATH);
        status.setMessage("invalid path");
        TBrokerCheckPathExistResponse resp = new TBrokerCheckPathExistResponse(status, false);
        Mockito.when(mockClient.checkPathExist(ArgumentMatchers.any(TBrokerCheckPathExistRequest.class))).thenReturn(resp);

        Assertions.assertThrows(IOException.class, () -> fs.exists(Location.of("hdfs:///bad/path")));
    }

    @Test
    void exists_throwsIOExceptionAndInvalidatesClientOnThriftError() throws Exception {
        Mockito.when(mockClient.checkPathExist(ArgumentMatchers.any(TBrokerCheckPathExistRequest.class)))
                .thenThrow(new org.apache.thrift.TException("connection lost"));

        Assertions.assertThrows(IOException.class, () -> fs.exists(Location.of("hdfs:///test")));
        Mockito.verify(mockPool).invalidate(endpoint, mockClient);
    }

    // ------------------------------------------------------------------
    // mkdirs() — no-op for broker
    // ------------------------------------------------------------------

    @Test
    void mkdirs_isNoOp() throws IOException {
        // Should not throw, does not call any broker RPC
        fs.mkdirs(Location.of("hdfs:///new/dir"));
    }

    // ------------------------------------------------------------------
    // delete()
    // ------------------------------------------------------------------

    @Test
    void delete_delegatesToBrokerDeletePath() throws Exception {
        TBrokerOperationStatus status = new TBrokerOperationStatus(TBrokerOperationStatusCode.OK);
        Mockito.when(mockClient.deletePath(ArgumentMatchers.any(TBrokerDeletePathRequest.class))).thenReturn(status);
        // recursive=true skips the listPath probe
        fs.delete(Location.of("hdfs:///test/file.txt"), true);

        ArgumentCaptor<TBrokerDeletePathRequest> captor =
                ArgumentCaptor.forClass(TBrokerDeletePathRequest.class);
        Mockito.verify(mockClient).deletePath(captor.capture());
        Assertions.assertEquals("hdfs:///test/file.txt", captor.getValue().getPath());
        Mockito.verify(mockPool).returnGood(endpoint, mockClient);
    }

    @Test
    void delete_swallowsFileNotFoundError() throws Exception {
        TBrokerOperationStatus status = new TBrokerOperationStatus(TBrokerOperationStatusCode.FILE_NOT_FOUND);
        Mockito.when(mockClient.deletePath(ArgumentMatchers.any(TBrokerDeletePathRequest.class))).thenReturn(status);
        // listPath of a missing path returns empty → probe passes; deletePath then sees FILE_NOT_FOUND
        TBrokerOperationStatus listStatus = new TBrokerOperationStatus(TBrokerOperationStatusCode.FILE_NOT_FOUND);
        TBrokerListResponse listResp = new TBrokerListResponse(listStatus);
        Mockito.when(mockClient.listPath(ArgumentMatchers.any(TBrokerListPathRequest.class))).thenReturn(listResp);

        // Should not throw
        fs.delete(Location.of("hdfs:///test/gone"), false);
    }

    @Test
    void delete_throwsIOExceptionOnBrokerError() throws Exception {
        TBrokerOperationStatus status = new TBrokerOperationStatus(TBrokerOperationStatusCode.INVALID_INPUT_FILE_PATH);
        status.setMessage("error");
        Mockito.when(mockClient.deletePath(ArgumentMatchers.any(TBrokerDeletePathRequest.class))).thenReturn(status);
        // recursive=true skips listPath probe so deletePath is reached
        Assertions.assertThrows(IOException.class, () -> fs.delete(Location.of("hdfs:///bad"), true));
    }

    @Test
    void delete_nonRecursive_throwsWhenDirectoryNotEmpty() throws Exception {
        TBrokerOperationStatus listStatus = new TBrokerOperationStatus(TBrokerOperationStatusCode.OK);
        TBrokerListResponse listResp = new TBrokerListResponse(listStatus);
        List<TBrokerFileStatus> children = new ArrayList<>();
        children.add(new TBrokerFileStatus("hdfs:///test/dir/child.txt", false, 10L, false));
        listResp.setFiles(children);
        Mockito.when(mockClient.listPath(ArgumentMatchers.any(TBrokerListPathRequest.class))).thenReturn(listResp);

        Assertions.assertThrows(IOException.class,
                () -> fs.delete(Location.of("hdfs:///test/dir"), false));
        Mockito.verify(mockClient, Mockito.never()).deletePath(ArgumentMatchers.any());
    }

    @Test
    void delete_nonRecursive_succeedsWhenDirectoryEmpty() throws Exception {
        TBrokerOperationStatus listStatus = new TBrokerOperationStatus(TBrokerOperationStatusCode.OK);
        TBrokerListResponse listResp = new TBrokerListResponse(listStatus);
        listResp.setFiles(new ArrayList<>());
        Mockito.when(mockClient.listPath(ArgumentMatchers.any(TBrokerListPathRequest.class))).thenReturn(listResp);
        TBrokerOperationStatus delStatus = new TBrokerOperationStatus(TBrokerOperationStatusCode.OK);
        Mockito.when(mockClient.deletePath(ArgumentMatchers.any(TBrokerDeletePathRequest.class))).thenReturn(delStatus);

        fs.delete(Location.of("hdfs:///test/empty-dir"), false);
        Mockito.verify(mockClient).deletePath(ArgumentMatchers.any());
    }

    @Test
    void delete_nonRecursive_succeedsWhenLocationIsFile() throws Exception {
        TBrokerOperationStatus listStatus = new TBrokerOperationStatus(TBrokerOperationStatusCode.OK);
        TBrokerListResponse listResp = new TBrokerListResponse(listStatus);
        List<TBrokerFileStatus> entries = new ArrayList<>();
        // For a file path, listPath returns a single entry whose path equals the location.
        entries.add(new TBrokerFileStatus("hdfs:///test/file.txt", false, 42L, false));
        listResp.setFiles(entries);
        Mockito.when(mockClient.listPath(ArgumentMatchers.any(TBrokerListPathRequest.class))).thenReturn(listResp);
        TBrokerOperationStatus delStatus = new TBrokerOperationStatus(TBrokerOperationStatusCode.OK);
        Mockito.when(mockClient.deletePath(ArgumentMatchers.any(TBrokerDeletePathRequest.class))).thenReturn(delStatus);

        fs.delete(Location.of("hdfs:///test/file.txt"), false);
        Mockito.verify(mockClient).deletePath(ArgumentMatchers.any());
    }

    @Test
    void delete_recursive_alwaysCallsDeletePath() throws Exception {
        TBrokerOperationStatus delStatus = new TBrokerOperationStatus(TBrokerOperationStatusCode.OK);
        Mockito.when(mockClient.deletePath(ArgumentMatchers.any(TBrokerDeletePathRequest.class))).thenReturn(delStatus);

        fs.delete(Location.of("hdfs:///test/non-empty-dir"), true);

        Mockito.verify(mockClient).deletePath(ArgumentMatchers.any());
        Mockito.verify(mockClient, Mockito.never()).listPath(ArgumentMatchers.any());
    }

    // ------------------------------------------------------------------
    // rename()
    // ------------------------------------------------------------------

    @Test
    void rename_delegatesToBrokerRenamePath() throws Exception {
        TBrokerOperationStatus status = new TBrokerOperationStatus(TBrokerOperationStatusCode.OK);
        Mockito.when(mockClient.renamePath(ArgumentMatchers.any(TBrokerRenamePathRequest.class))).thenReturn(status);

        fs.rename(Location.of("hdfs:///old/path"), Location.of("hdfs:///new/path"));

        ArgumentCaptor<TBrokerRenamePathRequest> captor =
                ArgumentCaptor.forClass(TBrokerRenamePathRequest.class);
        Mockito.verify(mockClient).renamePath(captor.capture());
        Assertions.assertEquals("hdfs:///old/path", captor.getValue().getSrcPath());
        Assertions.assertEquals("hdfs:///new/path", captor.getValue().getDestPath());
    }

    @Test
    void rename_throwsIOExceptionOnBrokerError() throws Exception {
        TBrokerOperationStatus status = new TBrokerOperationStatus(TBrokerOperationStatusCode.INVALID_ARGUMENT);
        status.setMessage("target exists");
        Mockito.when(mockClient.renamePath(ArgumentMatchers.any(TBrokerRenamePathRequest.class))).thenReturn(status);

        Assertions.assertThrows(IOException.class, () ->
                fs.rename(Location.of("hdfs:///src"), Location.of("hdfs:///dst")));
    }

    // ------------------------------------------------------------------
    // list()
    // ------------------------------------------------------------------

    @Test
    void list_returnsFileIteratorFromBrokerListPath() throws Exception {
        TBrokerOperationStatus status = new TBrokerOperationStatus(TBrokerOperationStatusCode.OK);
        TBrokerListResponse listResp = new TBrokerListResponse(status);
        List<TBrokerFileStatus> files = new ArrayList<>();
        TBrokerFileStatus f1 = new TBrokerFileStatus("hdfs:///dir/a.txt", false, 100L, false);
        f1.setModificationTime(1000L);
        TBrokerFileStatus f2 = new TBrokerFileStatus("hdfs:///dir/b.txt", false, 200L, false);
        f2.setModificationTime(2000L);
        files.add(f1);
        files.add(f2);
        listResp.setFiles(files);
        Mockito.when(mockClient.listPath(ArgumentMatchers.any(TBrokerListPathRequest.class))).thenReturn(listResp);

        FileIterator iter = fs.list(Location.of("hdfs:///dir"));

        Assertions.assertTrue(iter.hasNext());
        FileEntry entry1 = iter.next();
        Assertions.assertEquals("hdfs:///dir/a.txt", entry1.location().uri());
        Assertions.assertEquals(100L, entry1.length());

        Assertions.assertTrue(iter.hasNext());
        FileEntry entry2 = iter.next();
        Assertions.assertEquals("hdfs:///dir/b.txt", entry2.location().uri());
        Assertions.assertEquals(200L, entry2.length());

        Assertions.assertFalse(iter.hasNext());
        iter.close();
    }

    @Test
    void list_returnsEmptyIteratorForFileNotFound() throws Exception {
        TBrokerOperationStatus status = new TBrokerOperationStatus(TBrokerOperationStatusCode.FILE_NOT_FOUND);
        TBrokerListResponse listResp = new TBrokerListResponse(status);
        Mockito.when(mockClient.listPath(ArgumentMatchers.any(TBrokerListPathRequest.class))).thenReturn(listResp);

        FileIterator iter = fs.list(Location.of("hdfs:///missing"));

        Assertions.assertFalse(iter.hasNext());
    }

    // ------------------------------------------------------------------
    // close()
    // ------------------------------------------------------------------

    @Test
    void close_closesClientPool() throws IOException {
        fs.close();
        Mockito.verify(mockPool).close();
    }

    // ------------------------------------------------------------------
    // Accessor methods
    // ------------------------------------------------------------------

    @Test
    void accessors_returnConstructorValues() {
        Assertions.assertEquals("broker-host", fs.endpoint().getHostname());
        Assertions.assertEquals(9999, fs.endpoint().getPort());
        Assertions.assertEquals("fe-client", fs.clientId());
        Assertions.assertEquals("testuser", fs.brokerParams().get("username"));
    }

    @Test
    void brokerParams_areImmutable() {
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> fs.brokerParams().put("new", "val"));
    }
}
