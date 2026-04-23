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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class MetastorePrincipalTest {

    @Test
    public void roundTripAndMaskedToString() {
        Map<String, String> attrs = new LinkedHashMap<>();
        attrs.put("keytab", "/etc/secret.keytab");
        MetastorePrincipal p = MetastorePrincipal.builder()
                .type("KERBEROS").name("hive/_HOST@REALM").attrs(attrs).build();
        Assertions.assertEquals("KERBEROS", p.type());
        Assertions.assertEquals("hive/_HOST@REALM", p.name());
        Assertions.assertEquals("/etc/secret.keytab", p.attrs().get("keytab"));
        String s = p.toString();
        Assertions.assertFalse(s.contains("/etc/secret.keytab"), s);
        Assertions.assertTrue(s.contains("keytab=***"), s);
    }

    @Test
    public void rejectsMissingRequired() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> MetastorePrincipal.builder().name("n").attrs(Collections.emptyMap()).build());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> MetastorePrincipal.builder().type("t").attrs(Collections.emptyMap()).build());
    }

    @Test
    public void equality() {
        MetastorePrincipal a = MetastorePrincipal.builder()
                .type("T").name("n").attrs(Collections.emptyMap()).build();
        MetastorePrincipal b = MetastorePrincipal.builder()
                .type("T").name("n").attrs(Collections.emptyMap()).build();
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
    }
}
