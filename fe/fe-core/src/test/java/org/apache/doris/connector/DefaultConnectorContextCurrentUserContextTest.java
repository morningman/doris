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

package org.apache.doris.connector;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.connector.api.credential.UserContext;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DefaultConnectorContextCurrentUserContextTest {

    @BeforeEach
    public void setUp() {
        ConnectContext.remove();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    private static DefaultConnectorContext newCtx() {
        return new DefaultConnectorContext("ctl", 1L);
    }

    @Test
    public void noConnectContextReturnsSystem() {
        UserContext user = newCtx().currentUserContext();
        Assertions.assertEquals("system", user.username());
    }

    @Test
    public void connectContextWithoutUserReturnsSystem() {
        ConnectContext cc = new ConnectContext();
        cc.setThreadLocalInfo();
        try {
            UserContext user = newCtx().currentUserContext();
            Assertions.assertEquals("system", user.username());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void connectContextUserPropagatesToUserContext() {
        ConnectContext cc = new ConnectContext();
        cc.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("alice", "%"));
        cc.setThreadLocalInfo();
        try {
            UserContext user = newCtx().currentUserContext();
            Assertions.assertEquals("alice", user.username());
        } finally {
            ConnectContext.remove();
        }
    }
}
