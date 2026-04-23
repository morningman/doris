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

package org.apache.doris.connector.api.credential;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class CredentialBrokerInterfaceTest {

    @Test
    public void anonymousImplExposesAllSubOps() {
        StorageCredentialOps storage = new StorageCredentialOps() {
            @Override
            public CredentialEnvelope resolve(StorageRequest req, Mode mode) {
                return null;
            }

            @Override
            public Map<StoragePath, CredentialEnvelope> resolveAll(List<StorageRequest> reqs, Mode mode) {
                return Collections.emptyMap();
            }

            @Override
            public Map<String, String> toBackendProperties(CredentialEnvelope env) {
                return Collections.emptyMap();
            }

            @Override
            public void invalidate(CredentialScope scope) {
            }
        };
        MetastoreCredentialOps metastore = new MetastoreCredentialOps() {
            @Override
            public <T> T runAs(MetastorePrincipal principal, ThrowingSupplier<T> action) throws Exception {
                return action.get();
            }

            @Override
            public CredentialEnvelope resolve(MetastorePrincipal principal) {
                return null;
            }

            @Override
            public void invalidate(MetastorePrincipal principal) {
            }
        };
        JdbcCredentialOps jdbc = new JdbcCredentialOps() {
            @Override
            public Properties getConnectionProperties(JdbcRequest req) {
                return new Properties();
            }

            @Override
            public boolean rotateIfNeeded(JdbcRequest req) {
                return false;
            }
        };
        HttpCredentialOps http = new HttpCredentialOps() {
            @Override
            public void sign(HttpRequestSpec spec, CredentialEnvelope env) {
            }

            @Override
            public CredentialEnvelope acquire(HttpEndpoint endpoint) {
                return null;
            }
        };
        RuntimeImpersonationOps imp = new RuntimeImpersonationOps() {
            @Override
            public <T> T runAs(UserContext user, ThrowingSupplier<T> action) throws Exception {
                return action.get();
            }
        };

        CredentialBroker broker = new CredentialBroker() {
            @Override
            public StorageCredentialOps storage() {
                return storage;
            }

            @Override
            public MetastoreCredentialOps metastore() {
                return metastore;
            }

            @Override
            public JdbcCredentialOps jdbc() {
                return jdbc;
            }

            @Override
            public HttpCredentialOps http() {
                return http;
            }

            @Override
            public RuntimeImpersonationOps impersonation() {
                return imp;
            }
        };

        Assertions.assertSame(storage, broker.storage());
        Assertions.assertSame(metastore, broker.metastore());
        Assertions.assertSame(jdbc, broker.jdbc());
        Assertions.assertSame(http, broker.http());
        Assertions.assertSame(imp, broker.impersonation());
    }
}
