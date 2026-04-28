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

import org.apache.doris.connector.api.credential.CredentialBroker;
import org.apache.doris.connector.api.credential.HttpCredentialOps;
import org.apache.doris.connector.api.credential.JdbcCredentialOps;
import org.apache.doris.connector.api.credential.MetastoreCredentialOps;
import org.apache.doris.connector.api.credential.RuntimeImpersonationOps;
import org.apache.doris.connector.api.credential.StorageCredentialOps;
import org.apache.doris.connector.api.credential.ThrowingSupplier;
import org.apache.doris.connector.api.credential.UserContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

public class ConnectorContextExecuteAuthenticatedDefaultTest {

    private static ConnectorContext ctxWithBroker(CredentialBroker broker, UserContext user) {
        return new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "c";
            }

            @Override
            public long getCatalogId() {
                return 1L;
            }

            @Override
            public CredentialBroker getCredentialBroker() {
                return broker;
            }

            @Override
            public UserContext currentUserContext() {
                return user == null ? ConnectorContext.super.currentUserContext() : user;
            }
        };
    }

    private static CredentialBroker brokerWithImpersonation(RuntimeImpersonationOps imp) {
        return new CredentialBroker() {
            @Override
            public StorageCredentialOps storage() {
                return null;
            }

            @Override
            public MetastoreCredentialOps metastore() {
                return null;
            }

            @Override
            public JdbcCredentialOps jdbc() {
                return null;
            }

            @Override
            public HttpCredentialOps http() {
                return null;
            }

            @Override
            public RuntimeImpersonationOps impersonation() {
                return imp;
            }
        };
    }

    @Test
    public void nullBrokerFallsThroughToTaskCall() throws Exception {
        ConnectorContext ctx = ctxWithBroker(null, null);
        Assertions.assertEquals(7, (int) ctx.executeAuthenticated(() -> 7));
    }

    @Test
    public void unsupportedBrokerFallsThroughToTaskCall() throws Exception {
        // The bare default ConnectorContext throws UOE on getCredentialBroker(); the
        // executeAuthenticated default must catch it and fall back to running the task.
        ConnectorContext ctx = new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "c";
            }

            @Override
            public long getCatalogId() {
                return 1L;
            }
        };
        Assertions.assertEquals(11, (int) ctx.executeAuthenticated(() -> 11));
    }

    @Test
    public void nullImpersonationFallsThroughToTaskCall() throws Exception {
        CredentialBroker broker = brokerWithImpersonation(null);
        ConnectorContext ctx = ctxWithBroker(broker, null);
        Assertions.assertEquals(13, (int) ctx.executeAuthenticated(() -> 13));
    }

    @Test
    public void brokerImpersonationObservesDefaultSystemUser() throws Exception {
        AtomicReference<UserContext> seen = new AtomicReference<>();
        RuntimeImpersonationOps imp = new RuntimeImpersonationOps() {
            @Override
            public <T> T runAs(UserContext user, ThrowingSupplier<T> action) throws Exception {
                seen.set(user);
                return action.get();
            }
        };
        ConnectorContext ctx = ctxWithBroker(brokerWithImpersonation(imp), null);
        Integer result = ctx.executeAuthenticated(() -> 42);
        Assertions.assertEquals(42, (int) result);
        Assertions.assertNotNull(seen.get());
        Assertions.assertEquals("system", seen.get().username());
    }

    @Test
    public void brokerImpersonationObservesOverriddenUser() throws Exception {
        AtomicReference<UserContext> seen = new AtomicReference<>();
        RuntimeImpersonationOps imp = new RuntimeImpersonationOps() {
            @Override
            public <T> T runAs(UserContext user, ThrowingSupplier<T> action) throws Exception {
                seen.set(user);
                return action.get();
            }
        };
        UserContext alice = UserContext.builder().username("alice").build();
        ConnectorContext ctx = ctxWithBroker(brokerWithImpersonation(imp), alice);
        ctx.executeAuthenticated(() -> "ok");
        Assertions.assertEquals("alice", seen.get().username());
    }

    @Test
    public void checkedExceptionFromTaskPropagates() {
        RuntimeImpersonationOps imp = new RuntimeImpersonationOps() {
            @Override
            public <T> T runAs(UserContext user, ThrowingSupplier<T> action) throws Exception {
                return action.get();
            }
        };
        ConnectorContext ctx = ctxWithBroker(brokerWithImpersonation(imp), null);
        java.io.IOException ioe = Assertions.assertThrows(java.io.IOException.class,
                () -> ctx.executeAuthenticated(() -> {
                    throw new java.io.IOException("boom");
                }));
        Assertions.assertEquals("boom", ioe.getMessage());
    }

    @Test
    public void uncheckedExceptionFromTaskPropagates() {
        RuntimeImpersonationOps imp = new RuntimeImpersonationOps() {
            @Override
            public <T> T runAs(UserContext user, ThrowingSupplier<T> action) throws Exception {
                return action.get();
            }
        };
        ConnectorContext ctx = ctxWithBroker(brokerWithImpersonation(imp), null);
        IllegalStateException ise = Assertions.assertThrows(IllegalStateException.class,
                () -> ctx.executeAuthenticated(() -> {
                    throw new IllegalStateException("nope");
                }));
        Assertions.assertEquals("nope", ise.getMessage());
    }

    @Test
    public void defaultCurrentUserContextIsSystem() {
        ConnectorContext ctx = new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "c";
            }

            @Override
            public long getCatalogId() {
                return 1L;
            }
        };
        UserContext user = ctx.currentUserContext();
        Assertions.assertEquals("system", user.username());
        Assertions.assertNull(user.currentRole());
        Assertions.assertTrue(user.groups().isEmpty());
        Assertions.assertTrue(user.attrs().isEmpty());
    }
}
