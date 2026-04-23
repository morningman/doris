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

package org.apache.doris.connector.credential;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class VaultCredentialResolverTest {

    private static final ResolverContext CTX = new ResolverContext("test_catalog", 60L);

    /** Test double standing in for a real Vault HTTP client. */
    private static final class MockVaultClient implements VaultClient {
        private final Map<String, String> store = new HashMap<>();

        MockVaultClient put(String path, String field, String value) {
            store.put(path + "#" + field, value);
            return this;
        }

        @Override
        public String read(String path, String field) {
            String v = store.get(path + "#" + field);
            if (v == null) {
                throw new CredentialResolutionException("vault entry missing: " + path);
            }
            return v;
        }
    }

    @Test
    public void scheme_isVault() {
        Assertions.assertEquals("vault", new VaultCredentialResolver().scheme());
    }

    @Test
    public void unconfigured_throws() {
        Assertions.assertThrows(CredentialResolutionException.class,
                () -> new VaultCredentialResolver().resolve(
                        URI.create("vault://secret/data/db?field=password"), CTX));
    }

    @Test
    public void mockClient_returnsSecret() {
        MockVaultClient mock = new MockVaultClient().put("secret/data/db", "password", "v-pw");
        VaultCredentialResolver r = new VaultCredentialResolver(mock);
        Credential c = r.resolve(URI.create("vault://secret/data/db?field=password"), CTX);
        Assertions.assertEquals("v-pw", c.secretAsString());
    }

    @Test
    public void missingFieldQuery_throws() {
        VaultCredentialResolver r = new VaultCredentialResolver(new MockVaultClient());
        Assertions.assertThrows(CredentialResolutionException.class,
                () -> r.resolve(URI.create("vault://secret/data/db"), CTX));
    }

    @Test
    public void missingPath_throws() {
        VaultCredentialResolver r = new VaultCredentialResolver(new MockVaultClient());
        Assertions.assertThrows(CredentialResolutionException.class,
                () -> r.resolve(URI.create("vault:?field=x"), CTX));
    }

    @Test
    public void nullClient_throws() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new VaultCredentialResolver(null));
    }
}
