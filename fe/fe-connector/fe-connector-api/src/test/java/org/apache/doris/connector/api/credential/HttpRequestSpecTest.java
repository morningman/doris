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

public class HttpRequestSpecTest {

    @Test
    public void defaultMethodIsGet() {
        HttpRequestSpec spec = new HttpRequestSpec(URI.create("https://x"));
        Assertions.assertEquals("GET", spec.getMethod());
        Assertions.assertTrue(spec.getHeaders().isEmpty());
        Assertions.assertFalse(spec.getBody().isPresent());
    }

    @Test
    public void mutableHeadersAndBody() {
        HttpRequestSpec spec = new HttpRequestSpec(URI.create("https://x"), "POST");
        spec.getHeaders().put("Authorization", "Bearer x");
        byte[] body = new byte[]{1, 2, 3};
        spec.setBody(body);
        spec.setMethod("PUT");
        spec.setUrl(URI.create("https://y"));

        Assertions.assertEquals("Bearer x", spec.getHeaders().get("Authorization"));
        Assertions.assertSame(body, spec.getBody().get());
        Assertions.assertEquals("PUT", spec.getMethod());
        Assertions.assertEquals(URI.create("https://y"), spec.getUrl());
    }
}
