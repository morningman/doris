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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DefaultCredentialBrokerFactoryTest {

    private static final List<String> DEFAULT_CHAIN =
            Collections.unmodifiableList(Arrays.asList("env", "file", "kms", "vault"));

    @Test
    public void parseChain_default_whenBlank() {
        Assertions.assertEquals(DEFAULT_CHAIN,
                DefaultCredentialBrokerFactory.parseChain(null));
        Assertions.assertEquals(DEFAULT_CHAIN,
                DefaultCredentialBrokerFactory.parseChain(""));
        Assertions.assertEquals(DEFAULT_CHAIN,
                DefaultCredentialBrokerFactory.parseChain("   "));
    }

    @Test
    public void parseChain_dedupsAndLowercases() {
        Assertions.assertEquals(Arrays.asList("env", "file"),
                DefaultCredentialBrokerFactory.parseChain("ENV, file, env"));
    }

    @Test
    public void buildResolver_unknownScheme_throws() {
        Assertions.assertThrows(CredentialResolutionException.class,
                () -> DefaultCredentialBrokerFactory.buildResolver(
                        "bogus", KmsCredentialResolver.UNCONFIGURED, VaultCredentialResolver.UNCONFIGURED));
    }

    @Test
    public void loadKmsProvider_emptyReturnsStub() {
        Assertions.assertSame(KmsCredentialResolver.UNCONFIGURED,
                DefaultCredentialBrokerFactory.loadKmsProvider(""));
        Assertions.assertSame(KmsCredentialResolver.UNCONFIGURED,
                DefaultCredentialBrokerFactory.loadKmsProvider(null));
    }

    @Test
    public void loadKmsProvider_validFqcn_returnsInstance() {
        KmsProvider p = DefaultCredentialBrokerFactory.loadKmsProvider(
                IdentityKmsProvider.class.getName());
        Assertions.assertTrue(p instanceof IdentityKmsProvider);
    }

    @Test
    public void loadKmsProvider_badFqcn_throws() {
        Assertions.assertThrows(CredentialResolutionException.class,
                () -> DefaultCredentialBrokerFactory.loadKmsProvider("no.such.Class"));
    }

    @Test
    public void loadVaultClient_missingEither_returnsStub() {
        Assertions.assertSame(VaultCredentialResolver.UNCONFIGURED,
                DefaultCredentialBrokerFactory.loadVaultClient("", "tok"));
        Assertions.assertSame(VaultCredentialResolver.UNCONFIGURED,
                DefaultCredentialBrokerFactory.loadVaultClient("https://v", ""));
        Assertions.assertSame(VaultCredentialResolver.UNCONFIGURED,
                DefaultCredentialBrokerFactory.loadVaultClient(null, null));
    }

    @Test
    public void forCatalog_buildsBrokerWithDefaults() {
        DefaultCredentialBroker b = DefaultCredentialBrokerFactory.forCatalog("test_catalog");
        Assertions.assertEquals("test_catalog", b.catalogName());
        Assertions.assertFalse(b.chainOrder().isEmpty());
    }
}
