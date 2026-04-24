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

import org.apache.doris.connector.api.credential.CredentialEnvelope;
import org.apache.doris.connector.api.credential.JdbcCredentialOps;
import org.apache.doris.connector.api.credential.JdbcRequest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class JdbcCredentialOpsImplTest {

    private static List<CredentialResolver> envChain() {
        return Collections.singletonList(new EnvCredentialResolver(name -> "v-" + name));
    }

    private static JdbcRequest req(Map<String, String> attrs) {
        return JdbcRequest.builder()
                .catalog("cat")
                .url("jdbc:mysql://h/db")
                .attrs(attrs)
                .build();
    }

    @Test
    public void plainLiterals_passThrough() {
        DefaultCredentialBroker b = new DefaultCredentialBroker("c", envChain(), 60L);
        Map<String, String> attrs = new LinkedHashMap<>();
        attrs.put("user", "alice");
        attrs.put("password", "s3cret");
        Properties p = b.jdbc().getConnectionProperties(req(attrs));
        Assertions.assertEquals("alice", p.getProperty("user"));
        Assertions.assertEquals("s3cret", p.getProperty("password"));
    }

    @Test
    public void envScheme_resolved() {
        DefaultCredentialBroker b = new DefaultCredentialBroker("c", envChain(), 60L);
        Map<String, String> attrs = new LinkedHashMap<>();
        attrs.put("user", "alice");
        attrs.put("password", "env://MYSQL_PASS");
        Properties p = b.jdbc().getConnectionProperties(req(attrs));
        Assertions.assertEquals("alice", p.getProperty("user"));
        Assertions.assertEquals("v-MYSQL_PASS", p.getProperty("password"));
    }

    @Test
    public void unknownScheme_treatedAsLiteral() {
        // env-only chain; "vault://x" not registered → passed through as literal.
        DefaultCredentialBroker b = new DefaultCredentialBroker("c", envChain(), 60L);
        Map<String, String> attrs = new LinkedHashMap<>();
        attrs.put("password", "vault://kv/data/jdbc");
        Properties p = b.jdbc().getConnectionProperties(req(attrs));
        Assertions.assertEquals("vault://kv/data/jdbc", p.getProperty("password"));
    }

    @Test
    public void resolve_envelope_carriesExpiryAndRefreshHint() {
        Instant exp = Instant.parse("2026-01-01T00:00:00Z");
        URI hint = URI.create("vault://kv/refresh");
        CredentialResolver r = new CredentialResolver() {
            @Override
            public String scheme() {
                return "env";
            }

            @Override
            public Credential resolve(URI ref, ResolverContext ctx) {
                return Credential.ofSecret("hidden", exp, hint);
            }
        };
        DefaultCredentialBroker b = new DefaultCredentialBroker(
                "c", Collections.singletonList(r), 60L);
        Map<String, String> attrs = new LinkedHashMap<>();
        attrs.put("password", "env://X");
        CredentialEnvelope env = b.jdbc().resolve(req(attrs));
        Assertions.assertTrue(env.expiresAt().isPresent());
        Assertions.assertEquals(exp, env.expiresAt().get());
        Assertions.assertTrue(env.refreshHint().isPresent());
        Assertions.assertEquals(hint.toString(), env.refreshHint().get());
        Assertions.assertEquals("hidden", env.payload().get("password"));
    }

    @Test
    public void resolve_envelope_redactsInToString() {
        DefaultCredentialBroker b = new DefaultCredentialBroker("c", envChain(), 60L);
        Map<String, String> attrs = new LinkedHashMap<>();
        attrs.put("password", "env://SECRET_PASS");
        CredentialEnvelope env = b.jdbc().resolve(req(attrs));
        String s = env.toString();
        Assertions.assertFalse(s.contains("v-SECRET_PASS"), s);
    }

    @Test
    public void rotateIfNeeded_invalidatesUriEntries() {
        DefaultCredentialBroker b = new DefaultCredentialBroker("c", envChain(), 60L);
        Map<String, String> attrs = new LinkedHashMap<>();
        attrs.put("password", "env://MYSQL_PASS");
        b.jdbc().getConnectionProperties(req(attrs));
        Assertions.assertEquals(1, b.cacheSize());
        boolean rotated = b.jdbc().rotateIfNeeded(req(attrs));
        Assertions.assertTrue(rotated);
        Assertions.assertEquals(0, b.cacheSize());
    }

    @Test
    public void rotateIfNeeded_literalsOnly_returnsFalse() {
        DefaultCredentialBroker b = new DefaultCredentialBroker("c", envChain(), 60L);
        Map<String, String> attrs = new LinkedHashMap<>();
        attrs.put("user", "alice");
        attrs.put("password", "s3cret");
        b.jdbc().getConnectionProperties(req(attrs));
        Assertions.assertFalse(b.jdbc().rotateIfNeeded(req(attrs)));
    }

    @Test
    public void getConnectionProperties_nullReq_throws() {
        DefaultCredentialBroker b = new DefaultCredentialBroker("c", envChain(), 60L);
        JdbcCredentialOps ops = b.jdbc();
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ops.getConnectionProperties(null));
    }
}
