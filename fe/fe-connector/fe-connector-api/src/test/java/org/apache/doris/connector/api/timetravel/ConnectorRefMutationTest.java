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

public class ConnectorRefMutationTest {

    @Test
    public void roundTrip() {
        ConnectorRefMutation a = ConnectorRefMutation.builder()
                .name("main")
                .kind(RefKind.BRANCH)
                .fromSnapshot(7L)
                .retentionMs(1000L)
                .replaceIfExists(true)
                .build();
        Assertions.assertEquals("main", a.name());
        Assertions.assertEquals(RefKind.BRANCH, a.kind());
        Assertions.assertEquals(7L, a.fromSnapshot().orElseThrow());
        Assertions.assertEquals(1000L, a.retentionMs().orElseThrow());
        Assertions.assertTrue(a.replaceIfExists());

        ConnectorRefMutation b = ConnectorRefMutation.builder()
                .name("main")
                .kind(RefKind.BRANCH)
                .fromSnapshot(7L)
                .retentionMs(1000L)
                .replaceIfExists(true)
                .build();
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        Assertions.assertTrue(a.toString().contains("replaceIfExists=true"));
    }

    @Test
    public void defaultReplaceIfExistsIsFalse() {
        ConnectorRefMutation m = ConnectorRefMutation.builder()
                .name("rel-1")
                .kind(RefKind.TAG)
                .build();
        Assertions.assertFalse(m.replaceIfExists());
        Assertions.assertTrue(m.fromSnapshot().isEmpty());
        Assertions.assertTrue(m.retentionMs().isEmpty());
    }

    @Test
    public void rejectsUnknownAndMissing() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorRefMutation.builder().name("x").kind(RefKind.UNKNOWN).build());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorRefMutation.builder().kind(RefKind.BRANCH).build());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorRefMutation.builder().name("x").build());
    }
}
