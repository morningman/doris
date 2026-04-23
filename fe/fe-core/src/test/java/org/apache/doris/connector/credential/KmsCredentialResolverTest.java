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

public class KmsCredentialResolverTest {

    private static final ResolverContext CTX = new ResolverContext("test_catalog", 60L);

    @Test
    public void scheme_isKms() {
        Assertions.assertEquals("kms", new KmsCredentialResolver().scheme());
    }

    @Test
    public void unconfiguredStub_throws() {
        Assertions.assertThrows(CredentialResolutionException.class,
                () -> new KmsCredentialResolver().resolve(URI.create("kms://aws/myKey"), CTX));
    }

    @Test
    public void identityProvider_returnsKeyId() {
        KmsCredentialResolver r = new KmsCredentialResolver(new IdentityKmsProvider());
        Credential c = r.resolve(URI.create("kms://aws/myKey"), CTX);
        Assertions.assertEquals("myKey", c.secretAsString());
    }

    @Test
    public void missingProvider_throws() {
        KmsCredentialResolver r = new KmsCredentialResolver(new IdentityKmsProvider());
        Assertions.assertThrows(CredentialResolutionException.class,
                () -> r.resolve(URI.create("kms:///k"), CTX));
    }

    @Test
    public void missingKeyId_throws() {
        KmsCredentialResolver r = new KmsCredentialResolver(new IdentityKmsProvider());
        Assertions.assertThrows(CredentialResolutionException.class,
                () -> r.resolve(URI.create("kms://aws"), CTX));
    }

    @Test
    public void nullProvider_inConstructor_throws() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new KmsCredentialResolver(null));
    }
}
