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

public class ConnectorRefSpecTest {

    @Test
    public void builderHappyPathAndRoundTrip() {
        ConnectorRefSpec a = ConnectorRefSpec.builder()
                .name("main")
                .kind(RefKind.BRANCH)
                .snapshotId(123L)
                .retentionMs(86_400_000L)
                .build();
        Assertions.assertEquals("main", a.name());
        Assertions.assertEquals(RefKind.BRANCH, a.kind());
        Assertions.assertEquals(123L, a.snapshotId().orElseThrow());
        Assertions.assertEquals(86_400_000L, a.retentionMs().orElseThrow());

        ConnectorRefSpec b = ConnectorRefSpec.builder()
                .name("main")
                .kind(RefKind.BRANCH)
                .snapshotId(123L)
                .retentionMs(86_400_000L)
                .build();
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        Assertions.assertTrue(a.toString().contains("main"));
    }

    @Test
    public void rejectsUnknownKind() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorRefSpec.builder().name("x").kind(RefKind.UNKNOWN).build());
    }

    @Test
    public void requiredFields() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorRefSpec.builder().kind(RefKind.TAG).build());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorRefSpec.builder().name("x").build());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorRefSpec.builder().name("").kind(RefKind.TAG).build());
    }

    @Test
    public void optionalFieldsDefaultEmpty() {
        ConnectorRefSpec s = ConnectorRefSpec.builder().name("t").kind(RefKind.TAG).build();
        Assertions.assertTrue(s.snapshotId().isEmpty());
        Assertions.assertTrue(s.retentionMs().isEmpty());
    }
}
