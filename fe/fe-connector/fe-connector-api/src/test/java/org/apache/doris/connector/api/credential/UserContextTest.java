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
import java.util.HashSet;
import java.util.Set;

public class UserContextTest {

    @Test
    public void roundTripAndEquality() {
        Set<String> g = new HashSet<>();
        g.add("readers");
        UserContext u = UserContext.builder()
                .username("alice").currentRole("admin").groups(g)
                .attrs(Collections.singletonMap("dept", "eng")).build();
        Assertions.assertEquals("alice", u.username());
        Assertions.assertEquals("admin", u.currentRole());
        Assertions.assertTrue(u.groups().contains("readers"));
        Assertions.assertEquals("eng", u.attrs().get("dept"));

        UserContext u2 = UserContext.builder()
                .username("alice").currentRole("admin").groups(new HashSet<>(g))
                .attrs(Collections.singletonMap("dept", "eng")).build();
        Assertions.assertEquals(u, u2);
        Assertions.assertEquals(u.hashCode(), u2.hashCode());
    }

    @Test
    public void rejectsMissingUsername() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> UserContext.builder()
                        .groups(Collections.emptySet()).attrs(Collections.emptyMap()).build());
    }
}
