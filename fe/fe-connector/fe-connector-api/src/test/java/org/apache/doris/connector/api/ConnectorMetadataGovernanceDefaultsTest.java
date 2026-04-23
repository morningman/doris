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

package org.apache.doris.connector.api;

import org.apache.doris.connector.api.audit.ConnectorAuditOps;
import org.apache.doris.connector.api.mtmv.MtmvOps;
import org.apache.doris.connector.api.policy.PolicyOps;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class ConnectorMetadataGovernanceDefaultsTest {

    @Test
    public void mtmvOpsDefaultsToEmpty() {
        ConnectorMetadata m = new ConnectorMetadata() { };
        Optional<MtmvOps> ops = m.mtmvOps();
        Assertions.assertNotNull(ops);
        Assertions.assertTrue(ops.isEmpty());
    }

    @Test
    public void policyOpsDefaultsToEmpty() {
        ConnectorMetadata m = new ConnectorMetadata() { };
        Optional<PolicyOps> ops = m.policyOps();
        Assertions.assertNotNull(ops);
        Assertions.assertTrue(ops.isEmpty());
    }

    @Test
    public void auditOpsDefaultsToEmpty() {
        ConnectorMetadata m = new ConnectorMetadata() { };
        Optional<ConnectorAuditOps> ops = m.auditOps();
        Assertions.assertNotNull(ops);
        Assertions.assertTrue(ops.isEmpty());
    }
}
