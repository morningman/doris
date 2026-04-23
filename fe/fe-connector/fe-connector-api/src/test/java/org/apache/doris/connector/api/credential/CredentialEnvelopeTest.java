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

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class CredentialEnvelopeTest {

    @Test
    public void builderHappyAndDefaults() {
        CredentialEnvelope env = CredentialEnvelope.builder()
                .type("AWS_BASIC")
                .payload(Collections.singletonMap("k", "v"))
                .scope(CredentialScope.CATALOG)
                .build();
        Assertions.assertEquals("AWS_BASIC", env.type());
        Assertions.assertEquals(CredentialScope.CATALOG, env.scope());
        Assertions.assertEquals("v", env.payload().get("k"));
        Assertions.assertFalse(env.expiresAt().isPresent());
        Assertions.assertFalse(env.refreshHint().isPresent());
    }

    @Test
    public void payloadIsImmutable() {
        Map<String, String> src = new LinkedHashMap<>();
        src.put("k", "v");
        CredentialEnvelope env = CredentialEnvelope.builder()
                .type("T").payload(src).scope(CredentialScope.PATH).build();
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> env.payload().put("x", "y"));
        // mutating source after build does not leak
        src.put("z", "z");
        Assertions.assertFalse(env.payload().containsKey("z"));
    }

    @Test
    public void rejectsMissingRequired() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> CredentialEnvelope.builder()
                        .payload(Collections.emptyMap())
                        .scope(CredentialScope.CATALOG).build());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> CredentialEnvelope.builder()
                        .type("T")
                        .scope(CredentialScope.CATALOG).build());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> CredentialEnvelope.builder()
                        .type("T")
                        .payload(Collections.emptyMap()).build());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> CredentialEnvelope.builder()
                        .type("")
                        .payload(Collections.emptyMap())
                        .scope(CredentialScope.CATALOG).build());
    }

    @Test
    public void isExpiredLogic() {
        Instant t = Instant.parse("2025-01-01T00:00:00Z");
        CredentialEnvelope no = CredentialEnvelope.builder()
                .type("T").payload(Collections.emptyMap()).scope(CredentialScope.CATALOG).build();
        Assertions.assertFalse(no.isExpired(t));

        CredentialEnvelope yes = CredentialEnvelope.builder()
                .type("T").payload(Collections.emptyMap()).scope(CredentialScope.CATALOG)
                .expiresAt(t).build();
        Assertions.assertTrue(yes.isExpired(t));
        Assertions.assertTrue(yes.isExpired(t.plusSeconds(1)));
        Assertions.assertFalse(yes.isExpired(t.minusSeconds(1)));
    }

    @Test
    public void toStringMasksPayloadAndRefreshHint() {
        Map<String, String> payload = new LinkedHashMap<>();
        payload.put("access_key", "AKIASECRETSECRET");
        payload.put("secret_key", "VERYSECRETVALUE");
        CredentialEnvelope env = CredentialEnvelope.builder()
                .type("AWS_BASIC")
                .payload(payload)
                .scope(CredentialScope.CATALOG)
                .refreshHint("vault://leaky/path")
                .build();
        String s = env.toString();
        Assertions.assertFalse(s.contains("AKIASECRETSECRET"), s);
        Assertions.assertFalse(s.contains("VERYSECRETVALUE"), s);
        Assertions.assertFalse(s.contains("vault://leaky/path"), s);
        Assertions.assertTrue(s.contains("access_key=***"), s);
        Assertions.assertTrue(s.contains("secret_key=***"), s);
    }

    @Test
    public void equalsAndHashCode() {
        CredentialEnvelope a = CredentialEnvelope.builder()
                .type("T").payload(Collections.singletonMap("k", "v"))
                .scope(CredentialScope.CATALOG).build();
        CredentialEnvelope b = CredentialEnvelope.builder()
                .type("T").payload(Collections.singletonMap("k", "v"))
                .scope(CredentialScope.CATALOG).build();
        CredentialEnvelope c = CredentialEnvelope.builder()
                .type("T").payload(Collections.singletonMap("k", "v"))
                .scope(CredentialScope.PATH).build();
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        Assertions.assertNotEquals(a, c);
    }
}
