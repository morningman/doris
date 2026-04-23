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

import java.util.Collections;

public class JdbcRequestTest {

    @Test
    public void roundTripAndMaskedToString() {
        JdbcRequest r = JdbcRequest.builder()
                .catalog("c")
                .url("jdbc:mysql://h:3306/db")
                .username("alice")
                .attrs(Collections.singletonMap("password", "p@ss"))
                .build();
        Assertions.assertEquals("c", r.catalog());
        Assertions.assertEquals("alice", r.username());
        Assertions.assertEquals("p@ss", r.attrs().get("password"));

        String s = r.toString();
        Assertions.assertFalse(s.contains("alice"), s);
        Assertions.assertFalse(s.contains("p@ss"), s);
        Assertions.assertTrue(s.contains("password=***"), s);
        Assertions.assertTrue(s.contains("username=***"), s);
    }

    @Test
    public void nullUsernameRendersAsNull() {
        JdbcRequest r = JdbcRequest.builder()
                .catalog("c").url("jdbc:x").attrs(Collections.emptyMap()).build();
        Assertions.assertNull(r.username());
        Assertions.assertTrue(r.toString().contains("username=null"));
    }

    @Test
    public void rejectsMissingRequired() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> JdbcRequest.builder().url("u").build());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> JdbcRequest.builder().catalog("c").build());
    }

    @Test
    public void equality() {
        JdbcRequest a = JdbcRequest.builder().catalog("c").url("u").build();
        JdbcRequest b = JdbcRequest.builder().catalog("c").url("u").build();
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
    }
}
