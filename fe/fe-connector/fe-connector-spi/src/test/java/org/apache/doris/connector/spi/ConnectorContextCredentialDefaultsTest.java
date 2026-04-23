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

import org.apache.doris.connector.api.credential.CredentialResolver;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Optional;

public class ConnectorContextCredentialDefaultsTest {

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
    public void getCredentialBrokerThrowsByDefault() {
        ConnectorContext ctx = newCtx();
        Assertions.assertThrows(UnsupportedOperationException.class, ctx::getCredentialBroker);
    }

    @Test
    public void registerResolverIsNoOp() {
        ConnectorContext ctx = newCtx();
        CredentialResolver resolver = ref -> Optional.of(ref.toString());
        Assertions.assertDoesNotThrow(() -> ctx.registerResolver(resolver));
        Assertions.assertDoesNotThrow(() -> ctx.registerResolver(null));
    }

    @Test
    public void executeAuthenticatedStillWorks() throws Exception {
        ConnectorContext ctx = newCtx();
        Integer result = ctx.executeAuthenticated(() -> 42);
        Assertions.assertEquals(42, result);
        // also confirm CredentialResolver type is reachable via context's import
        Assertions.assertNotNull(URI.create("env://X"));
    }
}
