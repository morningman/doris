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

package org.apache.doris.connector.api.write;

import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class CommitOutcomeTest {

    @Test
    public void happyPath() {
        ConnectorTableVersion v = new ConnectorTableVersion.BySnapshotId(7L);
        CommitOutcome out = CommitOutcome.builder()
                .newVersion(v)
                .writtenRows(100L)
                .writtenBytes(2048L)
                .build();
        Assertions.assertEquals(Optional.of(v), out.newVersion());
        Assertions.assertEquals(100L, out.writtenRows());
        Assertions.assertEquals(2048L, out.writtenBytes());
        Assertions.assertTrue(out.toString().contains("CommitOutcome"));
    }

    @Test
    public void defaultsNewVersionEmpty() {
        CommitOutcome out = CommitOutcome.builder().build();
        Assertions.assertEquals(Optional.empty(), out.newVersion());
        Assertions.assertEquals(0L, out.writtenRows());
        Assertions.assertEquals(0L, out.writtenBytes());
    }

    @Test
    public void rejectsNegativeRows() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> CommitOutcome.builder().writtenRows(-1L).build());
    }

    @Test
    public void rejectsNegativeBytes() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> CommitOutcome.builder().writtenBytes(-1L).build());
    }

    @Test
    public void equalsAndHash() {
        CommitOutcome a = CommitOutcome.builder().writtenRows(3L).writtenBytes(9L).build();
        CommitOutcome b = CommitOutcome.builder().writtenRows(3L).writtenBytes(9L).build();
        CommitOutcome c = CommitOutcome.builder().writtenRows(4L).writtenBytes(9L).build();
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        Assertions.assertNotEquals(a, c);
    }
}
