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

package org.apache.doris.connector.api.timetravel;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;

public class ConnectorRefTest {

    @Test
    public void roundTrip() {
        Instant t = Instant.parse("2024-05-01T12:00:00Z");
        ConnectorRef a = ConnectorRef.builder()
                .name("main")
                .kind(RefKind.BRANCH)
                .snapshotId(99L)
                .createdAt(t)
                .build();
        Assertions.assertEquals("main", a.name());
        Assertions.assertEquals(RefKind.BRANCH, a.kind());
        Assertions.assertEquals(99L, a.snapshotId());
        Assertions.assertEquals(t, a.createdAt().orElseThrow());

        ConnectorRef b = ConnectorRef.builder()
                .name("main").kind(RefKind.BRANCH).snapshotId(99L).createdAt(t).build();
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        Assertions.assertTrue(a.toString().contains("main"));
    }

    @Test
    public void defaultsSnapshotIdMinusOneAndCreatedAtEmpty() {
        ConnectorRef r = ConnectorRef.builder().name("x").kind(RefKind.TAG).build();
        Assertions.assertEquals(-1L, r.snapshotId());
        Assertions.assertTrue(r.createdAt().isEmpty());
    }

    @Test
    public void requiredFields() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorRef.builder().kind(RefKind.BRANCH).build());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorRef.builder().name("x").build());
    }
}
