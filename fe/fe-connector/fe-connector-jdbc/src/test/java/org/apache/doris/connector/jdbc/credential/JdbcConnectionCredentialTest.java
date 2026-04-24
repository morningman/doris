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

class JdbcConnectionCredentialTest {

    @Test
    void nullUserPassword_normalisedToEmpty() {
        JdbcConnectionCredential c = new JdbcConnectionCredential(null, null, null, null);
        Assertions.assertEquals("", c.user());
        Assertions.assertEquals("", c.password());
        Assertions.assertFalse(c.expiresAt().isPresent());
        Assertions.assertFalse(c.refreshHint().isPresent());
    }

    @Test
    void isExpired_respectsClock() {
        Instant exp = Instant.parse("2026-01-01T00:00:00Z");
        JdbcConnectionCredential c = new JdbcConnectionCredential("u", "p", exp, null);
        Assertions.assertFalse(c.isExpired(exp.minusSeconds(1)));
        Assertions.assertTrue(c.isExpired(exp));
        Assertions.assertTrue(c.isExpired(exp.plusSeconds(1)));
    }

    @Test
    void isExpired_noExpiry_neverExpired() {
        JdbcConnectionCredential c = new JdbcConnectionCredential("u", "p", null, null);
        Assertions.assertFalse(c.isExpired(Instant.now()));
    }

    @Test
    void toString_doesNotRevealSecret() {
        JdbcConnectionCredential c = new JdbcConnectionCredential(
                "alice", "s3cret-payload", null, "vault://kv/host");
        String s = c.toString();
        Assertions.assertFalse(s.contains("s3cret-payload"), s);
        Assertions.assertFalse(s.contains("alice"), s);
        Assertions.assertFalse(s.contains("vault://kv/host"), s);
    }

    @Test
    void equalsHashCode_byValue() {
        JdbcConnectionCredential a = new JdbcConnectionCredential("u", "p", null, null);
        JdbcConnectionCredential b = new JdbcConnectionCredential("u", "p", null, null);
        JdbcConnectionCredential c = new JdbcConnectionCredential("u", "q", null, null);
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        Assertions.assertNotEquals(a, c);
    }
}
