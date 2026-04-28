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

package org.apache.doris.connector.hive.audit;

import org.apache.doris.connector.api.audit.ConnectorAuditOps;
import org.apache.doris.connector.api.audit.ConnectorAuditOps.AuditEventKind;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

class HiveAuditOpsTest {

    @Test
    void instanceIsSingleton() {
        Assertions.assertSame(HiveAuditOps.INSTANCE, HiveAuditOps.INSTANCE);
    }

    @Test
    void emittedKindsContainsOnlyPlanCompleted() {
        Set<AuditEventKind> kinds = HiveAuditOps.INSTANCE.emittedEventKinds();
        Assertions.assertEquals(Set.of(AuditEventKind.PLAN_COMPLETED), kinds);
    }

    @Test
    void emittedKindsHasExactlyOneElement() {
        Assertions.assertEquals(1, HiveAuditOps.INSTANCE.emittedEventKinds().size());
    }

    @Test
    void emittedKindsIsImmutable() {
        Set<AuditEventKind> kinds = HiveAuditOps.INSTANCE.emittedEventKinds();
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> kinds.add(AuditEventKind.COMMIT_COMPLETED));
    }

    @Test
    void implementsConnectorAuditOps() {
        Assertions.assertTrue(HiveAuditOps.INSTANCE instanceof ConnectorAuditOps);
    }
}
