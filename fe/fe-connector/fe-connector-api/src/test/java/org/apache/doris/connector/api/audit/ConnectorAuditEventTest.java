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

package org.apache.doris.connector.api.audit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class ConnectorAuditEventTest {

    @Test
    public void planCompletedHonoursContract() {
        ConnectorAuditEvent ev = new ConnectorAuditEvent.PlanCompletedAuditEvent(
                "hive_cat", 1L, Optional.of("q1"), "db", "t", 50L, 7L);
        Assertions.assertEquals("hive_cat", ev.catalog());
        Assertions.assertEquals(1L, ev.eventTimeMillis());
        Assertions.assertEquals("q1", ev.queryId().orElse(""));
        Assertions.assertTrue(ev instanceof ConnectorAuditEvent.PlanCompletedAuditEvent);
    }

    @Test
    public void commitCompletedRetainsCommitId() {
        ConnectorAuditEvent.CommitCompletedAuditEvent ev = new ConnectorAuditEvent.CommitCompletedAuditEvent(
                "c", 2L, Optional.empty(), "d", "t", "snap-1", 10L);
        Assertions.assertEquals("snap-1", ev.commitId());
        Assertions.assertEquals(10L, ev.rowsWritten());
    }

    @Test
    public void mtmvRefreshRetainsSnapshotMarker() {
        ConnectorAuditEvent.MtmvRefreshAuditEvent ev = new ConnectorAuditEvent.MtmvRefreshAuditEvent(
                "c", 3L, Optional.empty(), "d", "t", Optional.of("p1"), 99L);
        Assertions.assertEquals("p1", ev.partitionName().orElse(""));
        Assertions.assertEquals(99L, ev.snapshotMarker());
    }

    @Test
    public void policyEvalRetainsApplied() {
        ConnectorAuditEvent.PolicyEvalAuditEvent ev = new ConnectorAuditEvent.PolicyEvalAuditEvent(
                "c", 4L, Optional.empty(), "d", "t", "alice", "ROW_FILTER", true);
        Assertions.assertTrue(ev.hintApplied());
        Assertions.assertEquals("alice", ev.requestingUser());
    }

    @Test
    public void rejectsNulls() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorAuditEvent.PlanCompletedAuditEvent(
                        null, 0L, Optional.empty(), "d", "t", 0L, 0L));
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorAuditEvent.PlanCompletedAuditEvent(
                        "c", 0L, null, "d", "t", 0L, 0L));
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorAuditEvent.CommitCompletedAuditEvent(
                        "c", 0L, Optional.empty(), "d", "t", null, 0L));
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorAuditEvent.MtmvRefreshAuditEvent(
                        "c", 0L, Optional.empty(), "d", "t", null, 0L));
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorAuditEvent.PolicyEvalAuditEvent(
                        "c", 0L, Optional.empty(), "d", "t", null, "X", false));
    }

    @Test
    public void sealedHierarchyMatrix() {
        ConnectorAuditEvent[] events = new ConnectorAuditEvent[] {
                new ConnectorAuditEvent.PlanCompletedAuditEvent(
                        "c", 0L, Optional.empty(), "d", "t", 0L, 0L),
                new ConnectorAuditEvent.CommitCompletedAuditEvent(
                        "c", 0L, Optional.empty(), "d", "t", "x", 0L),
                new ConnectorAuditEvent.MtmvRefreshAuditEvent(
                        "c", 0L, Optional.empty(), "d", "t", Optional.empty(), 0L),
                new ConnectorAuditEvent.PolicyEvalAuditEvent(
                        "c", 0L, Optional.empty(), "d", "t", "u", "ROW_FILTER", false),
        };
        int matched = 0;
        for (ConnectorAuditEvent ev : events) {
            if (ev instanceof ConnectorAuditEvent.PlanCompletedAuditEvent) {
                matched++;
            } else if (ev instanceof ConnectorAuditEvent.CommitCompletedAuditEvent) {
                matched++;
            } else if (ev instanceof ConnectorAuditEvent.MtmvRefreshAuditEvent) {
                matched++;
            } else if (ev instanceof ConnectorAuditEvent.PolicyEvalAuditEvent) {
                matched++;
            }
        }
        Assertions.assertEquals(4, matched);
    }
}
