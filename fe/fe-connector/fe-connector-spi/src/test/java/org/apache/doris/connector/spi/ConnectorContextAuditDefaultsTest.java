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

package org.apache.doris.connector.spi;

import org.apache.doris.connector.api.audit.ConnectorAuditEvent;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class ConnectorContextAuditDefaultsTest {

    private static ConnectorContext newCtx() {
        return new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "c";
            }

            @Override
            public long getCatalogId() {
                return 1L;
            }
        };
    }

    @Test
    public void publishAuditEventIsNoOp() {
        ConnectorContext ctx = newCtx();
        ConnectorAuditEvent e = new ConnectorAuditEvent.PlanCompletedAuditEvent(
                "c", 1700000000000L, Optional.of("q1"), "db", "tbl", 12L, 3L);
        // Default impl must swallow without throwing.
        ctx.publishAuditEvent(e);
        Assertions.assertEquals("c", ctx.getCatalogName());
    }

    @Test
    public void publishAuditEventAcceptsAllPermittedSubtypes() {
        ConnectorContext ctx = newCtx();
        ctx.publishAuditEvent(new ConnectorAuditEvent.PlanCompletedAuditEvent(
                "c", 1L, Optional.empty(), "db", "tbl", 0L, 0L));
        ctx.publishAuditEvent(new ConnectorAuditEvent.CommitCompletedAuditEvent(
                "c", 2L, Optional.empty(), "db", "tbl", "commit-1", 10L));
        ctx.publishAuditEvent(new ConnectorAuditEvent.MtmvRefreshAuditEvent(
                "c", 3L, Optional.empty(), "db", "tbl", Optional.of("p1"), 100L));
        ctx.publishAuditEvent(new ConnectorAuditEvent.PolicyEvalAuditEvent(
                "c", 4L, Optional.empty(), "db", "tbl", "user", "ROW_FILTER", true));
        Assertions.assertEquals(1L, ctx.getCatalogId());
    }
}
