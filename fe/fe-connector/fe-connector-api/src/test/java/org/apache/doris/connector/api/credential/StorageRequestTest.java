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

package org.apache.doris.connector.api.credential;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;

public class StorageRequestTest {

    @Test
    public void roundTripAndEquality() {
        URI p = URI.create("s3://bucket/key");
        StorageRequest r = StorageRequest.builder()
                .path(p).catalog("cat").database("db").table("tbl").build();
        Assertions.assertEquals(p, r.path());
        Assertions.assertEquals("cat", r.catalog());
        Assertions.assertEquals("db", r.database().get());
        Assertions.assertEquals("tbl", r.table().get());

        StorageRequest r2 = StorageRequest.builder()
                .path(p).catalog("cat").database("db").table("tbl").build();
        Assertions.assertEquals(r, r2);
        Assertions.assertEquals(r.hashCode(), r2.hashCode());

        Assertions.assertNotNull(r.toString());
    }

    @Test
    public void rejectsMissingRequired() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> StorageRequest.builder().catalog("c").build());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> StorageRequest.builder().path(URI.create("s3://a")).build());
    }
}
