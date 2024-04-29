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

package org.apache.doris.fs.obj;

import org.apache.doris.analysis.StorageBackend.StorageType;
import org.apache.doris.backup.Status;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.fs.remote.RemoteFile;
import org.apache.doris.fs.remote.RemoteFileSystem;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestS3 {
    @Test
    public void test() {
        List<RemoteFile> rfiles = new ArrayList<>();
        Map<String, String> properties = Maps.newHashMap();
        properties.put("s3.endpoint", "http://xx:9080");
        properties.put("s3.region", "us-east-1");
        properties.put("s3.access_key", "004oxo02jHNMH4zxNr7n");
        properties.put("s3.secret_key", "nSlnBAciuSA0UMaMfvAYjBxO2NKIHPIPnaZEsv81");
        properties.put("use_path_style", "true");
        RemoteFileSystem fileSystem = FileSystemFactory.get(
                "broker", StorageType.S3, properties);
        Status st = fileSystem.globList("s3://yybucket/*", rfiles, false);
        System.out.println(st);
        for (RemoteFile rf : rfiles) {
            System.out.println(rf);
        }
    }
}
