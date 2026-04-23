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
import java.time.Instant;

public class BeDispatchableCredentialTest {

    @Test
    public void requiredFields_enforced() {
        Credential c = Credential.ofSecret("s");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new BeDispatchableCredential(null, c, BeDispatchableCredential.Scope.CATALOG));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new BeDispatchableCredential("", c, BeDispatchableCredential.Scope.CATALOG));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new BeDispatchableCredential("T", null, BeDispatchableCredential.Scope.CATALOG));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new BeDispatchableCredential("T", c, null));
    }

    @Test
    public void equalsAndHashCode_basedOnFields() {
        Credential c1 = Credential.ofSecret("s");
        Credential c2 = Credential.ofSecret("s");
        BeDispatchableCredential a = new BeDispatchableCredential("T", c1, BeDispatchableCredential.Scope.TABLE);
        BeDispatchableCredential b = new BeDispatchableCredential("T", c2, BeDispatchableCredential.Scope.TABLE);
        BeDispatchableCredential other = new BeDispatchableCredential("U", c1, BeDispatchableCredential.Scope.TABLE);
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        Assertions.assertNotEquals(a, other);
    }

    @Test
    public void toString_redactsSecretAndRefreshHint() {
        Credential c = Credential.ofSecret("topsecret123",
                Instant.parse("2030-01-01T00:00:00Z"),
                URI.create("https://refresh.example.com/topsecret"));
        BeDispatchableCredential d = new BeDispatchableCredential(
                "T", c, BeDispatchableCredential.Scope.SESSION);
        String s = d.toString();
        Assertions.assertFalse(s.contains("topsecret123"), "toString leaks secret: " + s);
        Assertions.assertFalse(s.contains("refresh.example.com"), "toString leaks refresh hint: " + s);
        Assertions.assertTrue(s.contains("***REDACTED***"));
        Assertions.assertTrue(s.contains("SESSION"));
    }

    @Test
    public void serialize_roundTripFields() {
        Credential c = Credential.ofSecret("v",
                Instant.ofEpochMilli(123456789L),
                URI.create("https://r/"));
        BeDispatchableCredential d = new BeDispatchableCredential(
                "TYPE_S3", c, BeDispatchableCredential.Scope.CATALOG);
        java.util.Map<String, String> m = d.serialize();
        Assertions.assertEquals("TYPE_S3", m.get("be_type"));
        Assertions.assertEquals("CATALOG", m.get("scope"));
        Assertions.assertEquals("v", m.get("secret"));
        Assertions.assertEquals("123456789", m.get("expires_at_ms"));
        Assertions.assertEquals("https://r/", m.get("refresh_hint"));
    }
}
