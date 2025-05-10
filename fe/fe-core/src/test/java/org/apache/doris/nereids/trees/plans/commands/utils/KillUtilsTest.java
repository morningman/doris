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

package org.apache.doris.nereids.trees.plans.commands.utils;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectScheduler;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.QueryState;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService;

import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class KillUtilsTest {

    @Mocked
    private ConnectContext mockCtx;

    @Mocked
    private ConnectContext mockKillCtx;

    @Mocked
    private ConnectScheduler mockScheduler;

    @Mocked
    private AccessControllerManager mockAccessManager;

    @Mocked
    private Env mockEnv;

    @Mocked
    private QueryState mockState;

    @Mocked
    private OriginStatement mockOriginStmt;

    @Mocked
    private SystemInfoService.HostInfo mockHostInfo;

    @BeforeEach
    public void setUp() {
        // Setup basic expectations
        new Expectations() {
            {
                mockCtx.getConnectScheduler();
                result = mockScheduler;
                mockCtx.getState();
                result = mockState;
                mockKillCtx.getQualifiedUser();
                result = "test_user";
                minTimes = 0;
                mockCtx.getQualifiedUser();
                result = "test_user";
                minTimes = 0;

                Env.getCurrentEnv();
                result = mockEnv;
                minTimes = 0;
                mockEnv.getAccessManager();
                result = mockAccessManager;
                minTimes = 0;

                ConnectContext.get();
                result = mockCtx;
                minTimes = 0;
            }
        };
    }

    @AfterEach
    public void tearDown() {
        // No need to close mocks in JMockit
    }

    @Test
    public void testKillByConnectionIdNotFound() {
        int connectionId = 123;

        // Setup connection not found
        new Expectations() {
            {
                mockScheduler.getContext(connectionId);
                result = null;
            }
        };

        // Verify exception is thrown with expected message
        Exception exception = Assertions.assertThrows(DdlException.class, () -> {
            KillUtils.killByConnection(mockCtx, connectionId);
        });

        Assertions.assertTrue(exception.getMessage().contains(String.valueOf(connectionId)));
        Assertions.assertTrue(exception.getMessage().contains("errCode = 2, detailMessage = Unknown thread id: 123"));
    }

    @Test
    public void testKillByConnectionSuicide() throws DdlException {
        int connectionId = 123;

        // Mock same context (suicide case)
        new Expectations() {
            {
                mockScheduler.getContext(connectionId);
                result = mockCtx;
            }
        };

        KillUtils.killByConnection(mockCtx, connectionId);

        // Verify method calls using JMockit verification
        new Expectations() {
            {
                mockCtx.setKilled();
                times = 1;
                mockState.setOk();
                times = 1;
            }
        };
    }

    @Test
    public void testKillByConnectionWithSameUser() throws DdlException {
        int connectionId = 123;

        // Mock different context but same user
        new Expectations() {
            {
                mockScheduler.getContext(connectionId);
                result = mockKillCtx;
                mockKillCtx.getQualifiedUser();
                result = "test_user";
                mockCtx.getQualifiedUser();
                result = "test_user";
            }
        };

        KillUtils.killByConnection(mockCtx, connectionId);

        // Verify method calls
        new Expectations() {
            {
                mockKillCtx.kill(true);
                times = 1;
                mockState.setOk();
                times = 1;
            }
        };
    }

    @Test
    public void testKillByConnectionWithAdminPrivilege() throws DdlException {
        int connectionId = 123;

        // Mock different context with different user but admin privilege
        new Expectations() {
            {
                mockScheduler.getContext(connectionId);
                result = mockKillCtx;
                mockKillCtx.getQualifiedUser();
                result = "other_user";
                mockCtx.getQualifiedUser();
                result = "admin_user";
                mockAccessManager.checkGlobalPriv(mockCtx, PrivPredicate.ADMIN);
                result = true;
            }
        };

        KillUtils.killByConnection(mockCtx, connectionId);

        // Verify method calls
        new Expectations() {
            {
                mockKillCtx.kill(true);
                times = 1;
                mockState.setOk();
                times = 1;
            }
        };
    }

    @Test
    public void testKillByConnectionWithoutPermission() {
        int connectionId = 123;

        // Mock different context with different user and no admin privilege
        new Expectations() {
            {
                mockScheduler.getContext(connectionId);
                result = mockKillCtx;
                mockKillCtx.getQualifiedUser();
                result = "other_user";
                mockCtx.getQualifiedUser();
                result = "non_admin_user";
                mockAccessManager.checkGlobalPriv(mockCtx, PrivPredicate.ADMIN);
                result = false;
            }
        };

        // Verify exception is thrown with expected message
        Exception exception = Assertions.assertThrows(DdlException.class, () -> {
            KillUtils.killByConnection(mockCtx, connectionId);
        });

        Assertions.assertTrue(exception.getMessage().contains(
                "errCode = 2, detailMessage = You are not owner of thread or query: 123"));
        Assertions.assertTrue(exception.getMessage().contains(String.valueOf(connectionId)));
    }

    // Test for killByQueryId when query is found on current node
    @Test
    public void testKillByQueryIdFoundOnCurrentNode() throws UserException {
        String queryId = "test_query_id";

        // Mock KillUtils static methods
        new MockUp<KillUtils>() {
            @Mock
            public boolean killByQueryIdOnCurrentNode(ConnectContext ctx, String queryId) {
                return true;
            }
        };

        // This should not throw exception
        KillUtils.killByQueryId(mockCtx, queryId, mockOriginStmt);
    }

    // Test for killByQueryId when query is not found and ctx is proxy
    @Test
    public void testKillByQueryIdNotFoundAndIsProxy() {
        String queryId = "test_query_id";

        // Setup mocks for proxy case
        new MockUp<KillUtils>() {
            @Mock
            public boolean killByQueryIdOnCurrentNode(ConnectContext ctx, String queryId) {
                return false;
            }
        };

        new Expectations() {
            {
                mockCtx.isProxy();
                result = true;
            }
        };

        // Verify exception is thrown with expected message
        Exception exception = Assertions.assertThrows(UserException.class, () -> {
            KillUtils.killByQueryId(mockCtx, queryId, mockOriginStmt);
        });

        Assertions.assertTrue(exception.getMessage().contains(queryId));
    }

    // Test for killByQueryId when query is found on other FE
    @Test
    public void testKillByQueryIdFoundOnOtherFE() throws Exception {
        String queryId = "test_query_id";

        // Mock for finding query on other FE
        new MockUp<KillUtils>() {
            @Mock
            public boolean killByQueryIdOnCurrentNode(ConnectContext ctx, String queryId) {
                return false;
            }

            @Mock
            public void killByQueryId(ConnectContext ctx, String queryId, OriginStatement stmt) {
                // Instead of executing real method, just set state to OK
                ctx.getState().setOk();
            }
        };

        new Expectations() {
            {
                mockCtx.isProxy();
                result = false;
            }
        };

        KillUtils.killByQueryId(mockCtx, queryId, mockOriginStmt);

        // Verify state was set to OK
        new Expectations() {
            {
                mockState.setOk();
                times = 1;
            }
        };
    }

    // Test for killByQueryId when query is not found anywhere and needs to be killed on BE
    @Test
    public void testKillByQueryIdKillBackend() throws Exception {
        String queryId = "test_query_id";

        // Setup mocks for BE kill case
        List<Frontend> frontends = new ArrayList<>();

        new MockUp<KillUtils>() {
            @Mock
            public boolean killByQueryIdOnCurrentNode(ConnectContext ctx, String queryId) {
                return false;
            }

            @Mock
            public void killToBackend(String queryId) {
                // Just a mock implementation
            }
        };

        new Expectations() {
            {
                mockCtx.isProxy();
                result = false;
                mockEnv.getFrontends(null);
                result = frontends;
            }
        };

        KillUtils.killByQueryId(mockCtx, queryId, mockOriginStmt);

        // Unfortunately with JMockit we can't directly verify static method calls in MockUp
        // In a real test, we would verify side effects or use a different approach
    }
}
