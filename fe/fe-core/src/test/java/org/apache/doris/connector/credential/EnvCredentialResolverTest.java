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

public class EnvCredentialResolverTest {

    private static EnvCredentialResolver withEnv(Map<String, String> env) {
        return new EnvCredentialResolver(env::get);
    }

    @Test
    public void scheme_isEnv() {
        Assertions.assertEquals("env", new EnvCredentialResolver().scheme());
    }

    @Test
    public void resolve_existingVar_returnsValue() {
        Map<String, String> env = new HashMap<>();
        env.put("MY_TOKEN", "s3cr3t");
        Credential c = withEnv(env).resolve(URI.create("env://MY_TOKEN"), ctx());
        Assertions.assertEquals("s3cr3t", c.secretAsString());
    }

    @Test
    public void resolve_missingVar_throws() {
        Assertions.assertThrows(CredentialResolutionException.class,
                () -> withEnv(new HashMap<>()).resolve(URI.create("env://MISSING"), ctx()));
    }

    @Test
    public void resolve_blankName_throws() {
        Assertions.assertThrows(CredentialResolutionException.class,
                () -> withEnv(new HashMap<>()).resolve(URI.create("env:%20"), ctx()));
    }

    @Test
    public void resolve_nullRef_throws() {
        Assertions.assertThrows(CredentialResolutionException.class,
                () -> withEnv(new HashMap<>()).resolve(null, ctx()));
    }

    private static ResolverContext ctx() {
        return new ResolverContext("test_catalog", 60L);
    }
}
