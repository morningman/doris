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

import org.apache.doris.thrift.TConnectorCredential;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Instant;
import java.util.Arrays;

public class BeDispatchableCredentialThriftRoundTripTest {

    @Test
    public void roundTrip_basic() {
        Credential c = Credential.ofSecret("super-secret-token");
        BeDispatchableCredential original = new BeDispatchableCredential(
                "s3", c, BeDispatchableCredential.Scope.CATALOG);
        URI ref = URI.create("env://AWS_ACCESS_KEY");

        TConnectorCredential thrift = original.toThrift(ref);
        Assertions.assertEquals("env", thrift.getScheme());
        Assertions.assertEquals(ref.toString(), thrift.getRef());
        Assertions.assertEquals("CATALOG", thrift.getScope());
        Assertions.assertNotNull(thrift.getExtra());
        Assertions.assertEquals("s3", thrift.getExtra().get("be_type"));
        Assertions.assertArrayEquals("super-secret-token".getBytes(), thrift.getSecret());
        Assertions.assertFalse(thrift.isSetExpiresAtMs());
        Assertions.assertFalse(thrift.isSetRefreshHint());

        BeDispatchableCredential roundTripped = BeDispatchableCredential.fromThrift(thrift);
        Assertions.assertEquals(original, roundTripped);
    }

    @Test
    public void roundTrip_withExpiryAndHint() {
        Instant exp = Instant.ofEpochMilli(1_700_000_000_000L);
        URI hint = URI.create("https://fe-host/credential/refresh");
        Credential c = Credential.ofBytes(new byte[] {1, 2, 3, 4}, exp, hint);
        BeDispatchableCredential original = new BeDispatchableCredential(
                "kerberos", c, BeDispatchableCredential.Scope.SESSION);
        URI ref = URI.create("vault://secret/data/svc");

        TConnectorCredential thrift = original.toThrift(ref);
        Assertions.assertEquals(exp.toEpochMilli(), thrift.getExpiresAtMs());
        Assertions.assertEquals(hint.toString(), thrift.getRefreshHint());

        BeDispatchableCredential roundTripped = BeDispatchableCredential.fromThrift(thrift);
        Assertions.assertEquals(original, roundTripped);
        Assertions.assertTrue(Arrays.equals(new byte[] {1, 2, 3, 4}, roundTripped.secretBytes()));
    }

    @Test
    public void fromThrift_missingSecret_throws() {
        TConnectorCredential t = new TConnectorCredential();
        t.setScope("CATALOG");
        t.putToExtra("be_type", "x");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> BeDispatchableCredential.fromThrift(t));
    }

    @Test
    public void fromThrift_missingBeType_throws() {
        TConnectorCredential t = new TConnectorCredential();
        t.setScope("CATALOG");
        t.setSecret("x".getBytes());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> BeDispatchableCredential.fromThrift(t));
    }

    @Test
    public void fromThrift_null_throws() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> BeDispatchableCredential.fromThrift(null));
    }

    @Test
    public void toThrift_doesNotLeakSecretInToString() {
        Credential c = Credential.ofSecret("LEAK_ME_IF_YOU_CAN");
        BeDispatchableCredential dispatch = new BeDispatchableCredential(
                "x", c, BeDispatchableCredential.Scope.TABLE);
        String s = dispatch.toString();
        Assertions.assertFalse(s.contains("LEAK_ME_IF_YOU_CAN"), s);
    }
}
