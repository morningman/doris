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

public class HttpEndpointTest {

    @Test
    public void roundTripAndEquality() {
        URI u = URI.create("https://rest/iceberg");
        HttpEndpoint e = HttpEndpoint.builder().url(u).catalog("c").purpose("iceberg-rest").build();
        Assertions.assertEquals(u, e.url());
        Assertions.assertEquals("c", e.catalog());
        Assertions.assertEquals("iceberg-rest", e.purpose());
        HttpEndpoint e2 = HttpEndpoint.builder().url(u).catalog("c").purpose("iceberg-rest").build();
        Assertions.assertEquals(e, e2);
        Assertions.assertEquals(e.hashCode(), e2.hashCode());
        Assertions.assertNotNull(e.toString());
    }

    @Test
    public void rejectsMissingRequired() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> HttpEndpoint.builder().catalog("c").purpose("p").build());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> HttpEndpoint.builder().url(URI.create("https://x")).purpose("p").build());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> HttpEndpoint.builder().url(URI.create("https://x")).catalog("c").build());
    }
}
