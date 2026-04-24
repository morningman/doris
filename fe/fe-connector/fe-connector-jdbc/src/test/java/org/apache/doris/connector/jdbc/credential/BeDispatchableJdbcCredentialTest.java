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

package org.apache.doris.connector.jdbc.credential;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

class BeDispatchableJdbcCredentialTest {

    @Test
    void serialize_includesUserPasswordAndOptionalFields() {
        Instant exp = Instant.parse("2026-06-01T12:00:00Z");
        JdbcConnectionCredential c = new JdbcConnectionCredential(
                "alice", "s3cret", exp, "vault://refresh");
        BeDispatchableJdbcCredential be = new BeDispatchableJdbcCredential(c);
        Map<String, String> m = be.serialize();
        Assertions.assertEquals("JDBC", m.get("be_type"));
        Assertions.assertEquals("alice", m.get("user"));
        Assertions.assertEquals("s3cret", m.get("password"));
        Assertions.assertEquals(String.valueOf(exp.toEpochMilli()), m.get("expires_at_ms"));
        Assertions.assertEquals("vault://refresh", m.get("refresh_hint"));
    }

    @Test
    void serialize_omitsOptionalsWhenAbsent() {
        JdbcConnectionCredential c = new JdbcConnectionCredential("u", "p", null, null);
        Map<String, String> m = new BeDispatchableJdbcCredential(c).serialize();
        Assertions.assertFalse(m.containsKey("expires_at_ms"));
        Assertions.assertFalse(m.containsKey("refresh_hint"));
    }

    @Test
    void toString_redactsSecretAndHint() {
        JdbcConnectionCredential c = new JdbcConnectionCredential(
                "alice", "very-secret-value", null, "vault://refresh-host");
        BeDispatchableJdbcCredential be = new BeDispatchableJdbcCredential(c);
        String s = be.toString();
        Assertions.assertFalse(s.contains("very-secret-value"), s);
        Assertions.assertFalse(s.contains("alice"), s);
        Assertions.assertFalse(s.contains("vault://refresh-host"), s);
        Assertions.assertTrue(s.contains("REDACTED"));
    }

    @Test
    void equalsHashCode_followCredential() {
        JdbcConnectionCredential c1 = new JdbcConnectionCredential("u", "p", null, null);
        JdbcConnectionCredential c2 = new JdbcConnectionCredential("u", "p", null, null);
        Assertions.assertEquals(new BeDispatchableJdbcCredential(c1),
                new BeDispatchableJdbcCredential(c2));
        Assertions.assertEquals(new BeDispatchableJdbcCredential(c1).hashCode(),
                new BeDispatchableJdbcCredential(c2).hashCode());
    }

    @Test
    void nullCredential_throws() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new BeDispatchableJdbcCredential(null));
    }
}
