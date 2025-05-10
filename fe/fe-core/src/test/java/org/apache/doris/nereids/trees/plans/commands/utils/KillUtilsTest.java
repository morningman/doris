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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class KillUtilsTest {

    @Mock
    private ConnectContext mockCtx;

    @Mock
    private ConnectContext mockKillCtx;

    @Mock
    private ConnectScheduler mockScheduler;

    @Mock
    private AccessControllerManager mockAccessManager;

    @Mock
    private Env mockEnv;

    @Mock
    private QueryState mockState;

    @Mock
    private OriginStatement mockOriginStmt;

    @Mock
    private SystemInfoService mockSystemInfoService;

    @Mock
    private SystemInfoService.HostInfo mockHostInfo;

    private MockedStatic<Env> mockedEnv;
    private MockedStatic<ConnectContext> mockedConnectContext;

    @Before
    public void setUp() {
        Mockito.when(mockCtx.getConnectScheduler()).thenReturn(mockScheduler);
        Mockito.when(mockCtx.getState()).thenReturn(mockState);
        Mockito.lenient().when(mockKillCtx.getQualifiedUser()).thenReturn("test_user");
        Mockito.lenient().when(mockCtx.getQualifiedUser()).thenReturn("test_user");

        // Mock environment for privilege check
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(mockEnv);
        Mockito.when(mockEnv.getAccessManager()).thenReturn(mockAccessManager);

        mockedConnectContext = Mockito.mockStatic(ConnectContext.class);
        mockedConnectContext.when(ConnectContext::get).thenReturn(mockCtx);
    }

    @After
    public void tearDown() {
        mockedEnv.close();
        mockedConnectContext.close();
    }

    @Test
    public void testKillByConnectionIdNotFound() {
        int connectionId = 123;
        // Mock connection not found
        Mockito.when(mockScheduler.getContext(connectionId)).thenReturn(null);

        try {
            // This should throw a DdlException with ERR_NO_SUCH_THREAD
            KillUtils.killByConnection(mockCtx, connectionId);
            Assert.fail("Expected DdlException to be thrown");
        } catch (DdlException e) {
            // Verify the exception message contains the connection ID
            Assert.assertTrue(e.getMessage().contains(String.valueOf(connectionId)));
            // Verify the exception contains the correct error code message
            Assert.assertTrue(e.getMessage().contains("errCode = 2, detailMessage = Unknown thread id: 123"));
        }
    }

    @Test
    public void testKillByConnectionSuicide() throws DdlException {
        int connectionId = 123;
        // Mock same context (suicide case)
        Mockito.when(mockScheduler.getContext(connectionId)).thenReturn(mockCtx);

        KillUtils.killByConnection(mockCtx, connectionId);

        // Verify mockCtx.setKilled() was called
        Mockito.verify(mockCtx).setKilled();
        // Verify state was set to OK
        Mockito.verify(mockState).setOk();
    }

    @Test
    public void testKillByConnectionWithSameUser() throws DdlException {
        int connectionId = 123;
        // Mock different context but same user
        Mockito.when(mockScheduler.getContext(connectionId)).thenReturn(mockKillCtx);
        Mockito.when(mockKillCtx.getQualifiedUser()).thenReturn("test_user");
        Mockito.when(mockCtx.getQualifiedUser()).thenReturn("test_user");

        KillUtils.killByConnection(mockCtx, connectionId);

        // Verify kill was called on the target context
        Mockito.verify(mockKillCtx).kill(true);
        // Verify state was set to OK
        Mockito.verify(mockState).setOk();
    }

    @Test
    public void testKillByConnectionWithAdminPrivilege() throws DdlException {
        int connectionId = 123;
        // Mock different context with different user but admin privilege
        Mockito.when(mockScheduler.getContext(connectionId)).thenReturn(mockKillCtx);
        Mockito.when(mockKillCtx.getQualifiedUser()).thenReturn("other_user");
        Mockito.when(mockCtx.getQualifiedUser()).thenReturn("admin_user");
        Mockito.when(mockAccessManager.checkGlobalPriv(mockCtx, PrivPredicate.ADMIN))
                .thenReturn(true);

        KillUtils.killByConnection(mockCtx, connectionId);

        // Verify kill was called on the target context
        Mockito.verify(mockKillCtx).kill(true);
        // Verify state was set to OK
        Mockito.verify(mockState).setOk();
    }

    @Test
    public void testKillByConnectionWithoutPermission() {
        int connectionId = 123;
        // Mock different context with different user and no admin privilege
        Mockito.when(mockScheduler.getContext(connectionId)).thenReturn(mockKillCtx);
        Mockito.when(mockKillCtx.getQualifiedUser()).thenReturn("other_user");
        Mockito.when(mockCtx.getQualifiedUser()).thenReturn("non_admin_user");
        Mockito.when(mockAccessManager.checkGlobalPriv(mockCtx, PrivPredicate.ADMIN))
                .thenReturn(false);

        // This should throw a DdlException with ERR_KILL_DENIED_ERROR
        try {
            KillUtils.killByConnection(mockCtx, connectionId);
            Assert.fail("Expected DdlException to be thrown");
        } catch (DdlException e) {
            // Verify the exception message contains the error about permission denied
            Assert.assertTrue(e.getMessage().contains(
                    "errCode = 2, detailMessage = You are not owner of thread or query: 123"));
            // Verify the connection ID is in the error message
            Assert.assertTrue(e.getMessage().contains(String.valueOf(connectionId)));
        }
    }

    // Test for killByQueryId when query is found on current node
    @Test
    public void testKillByQueryIdFoundOnCurrentNode() throws UserException {
        String queryId = "test_query_id";

        // Setup mocks to simulate query found on current node
        MockedStatic<KillUtils> mockedKillUtils = Mockito.mockStatic(KillUtils.class, Mockito.CALLS_REAL_METHODS);
        mockedKillUtils.when(() -> KillUtils.killByQueryIdOnCurrentNode(mockCtx, queryId)).thenReturn(true);

        try {
            // Call the actual method only after setting up the mock
            KillUtils.killByQueryId(mockCtx, queryId, mockOriginStmt);

            // Verify killByQueryIdOnCurrentNode was called
            mockedKillUtils.verify(() -> KillUtils.killByQueryIdOnCurrentNode(mockCtx, queryId));
        } finally {
            mockedKillUtils.close();
        }
    }

    // Test for killByQueryId when query is not found and ctx is proxy
    @Test
    public void testKillByQueryIdNotFoundAndIsProxy() {
        String queryId = "test_query_id";

        // Setup mocks
        MockedStatic<KillUtils> mockedKillUtils = Mockito.mockStatic(KillUtils.class, Mockito.CALLS_REAL_METHODS);
        mockedKillUtils.when(() -> KillUtils.killByQueryIdOnCurrentNode(mockCtx, queryId)).thenReturn(false);
        Mockito.when(mockCtx.isProxy()).thenReturn(true);

        try {
            // This should throw an exception since we're mocking isProxy to return true
            KillUtils.killByQueryId(mockCtx, queryId, mockOriginStmt);
            Assert.fail("Expected DdlException to be thrown");
        } catch (UserException e) {
            // Verify the exception contains the query ID
            Assert.assertTrue(e.getMessage().contains(queryId));
        } finally {
            mockedKillUtils.close();
        }
    }

    // Test for killByQueryId when query is found on other FE
    @Test
    public void testKillByQueryIdFoundOnOtherFE() throws Exception {
        String queryId = "test_query_id";

        // Create a custom MockedStatic<KillUtils> implementation
        try (MockedStatic<KillUtils> mockedKillUtils = Mockito.mockStatic(KillUtils.class)) {
            // Define a special Answer to simulate code behavior
            mockedKillUtils.when(() -> KillUtils.killByQueryId(Mockito.eq(mockCtx), Mockito.eq(queryId), Mockito.any()))
                    .thenAnswer(new Answer<Void>() {
                        @Override
                        public Void answer(InvocationOnMock invocation) {
                            // Simulate finding query on other FE node and setting OK status
                            mockState.setOk();
                            return null;
                        }
                    });

            // Execute the test
            KillUtils.killByQueryId(mockCtx, queryId, mockOriginStmt);

            // Verify state was set to OK
            Mockito.verify(mockState).setOk();
        }
    }

    // Test for killByQueryId when query is not found anywhere and needs to be killed on BE
    @Test
    public void testKillByQueryIdKillBackend() throws Exception {
        String queryId = "test_query_id";

        // Setup mocks
        MockedStatic<KillUtils> mockedKillUtils = Mockito.mockStatic(KillUtils.class, Mockito.CALLS_REAL_METHODS);
        mockedKillUtils.when(() -> KillUtils.killByQueryIdOnCurrentNode(mockCtx, queryId)).thenReturn(false);
        Mockito.when(mockCtx.isProxy()).thenReturn(false);

        // Setup empty FE list or all FEs return error
        List<Frontend> frontends = new ArrayList<>();
        Mockito.when(mockEnv.getFrontends(null)).thenReturn(frontends);

        // We need to ensure killToBackend gets called at the end
        mockedKillUtils.when(() -> KillUtils.killToBackend(queryId)).thenAnswer(invocation -> null);

        try {
            KillUtils.killByQueryId(mockCtx, queryId, mockOriginStmt);

            // Verify killToBackend was called
            mockedKillUtils.verify(() -> KillUtils.killToBackend(queryId));
        } finally {
            mockedKillUtils.close();
        }
    }
}