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

package org.apache.doris.connector.api.policy;

import org.apache.doris.connector.api.ConnectorTableId;
import org.apache.doris.connector.api.credential.UserContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class PolicyOpsTest {

    @Test
    public void supportsRlsDefaultsTrue() {
        PolicyOps ops = new PolicyOps() {
            @Override
            public Optional<ColumnMaskHint> hintForColumn(
                    ConnectorTableId id, String column, UserContext user) {
                return Optional.empty();
            }
        };
        ConnectorPolicyContext ctx = new ConnectorPolicyContext(
                "hive_cat", "db", "t", "alice", Optional.empty());
        Assertions.assertTrue(ops.supportsRlsAt(ConnectorTableId.of("db", "t"), ctx));
    }

    @Test
    public void hintForColumnIsHonoured() {
        PolicyOps ops = new PolicyOps() {
            @Override
            public Optional<ColumnMaskHint> hintForColumn(
                    ConnectorTableId id, String column, UserContext user) {
                if ("ssn".equals(column)) {
                    return Optional.of(ColumnMaskHint.redact());
                }
                return Optional.empty();
            }
        };
        UserContext u = UserContext.builder().username("u").build();
        Assertions.assertEquals(ColumnMaskHint.MaskKind.REDACT,
                ops.hintForColumn(ConnectorTableId.of("d", "t"), "ssn", u).orElseThrow().maskKind());
        Assertions.assertTrue(ops.hintForColumn(ConnectorTableId.of("d", "t"), "name", u).isEmpty());
    }

    @Test
    public void onPolicyChangedDefaultIsNoop() {
        PolicyOps ops = new PolicyOps() {
            @Override
            public Optional<ColumnMaskHint> hintForColumn(
                    ConnectorTableId id, String column, UserContext user) {
                return Optional.empty();
            }
        };
        // Must not throw.
        ops.onPolicyChanged(new PolicyChangeNotification(
                "c", "d", Optional.of("t"), PolicyKind.ROW_FILTER, Instant.now(), Optional.empty()));
    }

    @Test
    public void onPolicyChangedOverrideInvoked() {
        AtomicInteger counter = new AtomicInteger();
        PolicyOps ops = new PolicyOps() {
            @Override
            public Optional<ColumnMaskHint> hintForColumn(
                    ConnectorTableId id, String column, UserContext user) {
                return Optional.empty();
            }

            @Override
            public void onPolicyChanged(PolicyChangeNotification n) {
                counter.incrementAndGet();
            }
        };
        ops.onPolicyChanged(new PolicyChangeNotification(
                "c", "d", Optional.empty(), PolicyKind.COLUMN_MASK, Instant.now(), Optional.empty()));
        Assertions.assertEquals(1, counter.get());
    }

    @Test
    public void policyContextRejectsNulls() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorPolicyContext(null, "d", "t", "u", Optional.empty()));
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorPolicyContext("c", null, "t", "u", Optional.empty()));
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorPolicyContext("c", "d", null, "u", Optional.empty()));
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorPolicyContext("c", "d", "t", null, Optional.empty()));
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorPolicyContext("c", "d", "t", "u", null));
    }
}
