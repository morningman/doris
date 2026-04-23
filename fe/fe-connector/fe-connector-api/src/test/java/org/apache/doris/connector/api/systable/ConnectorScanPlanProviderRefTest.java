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

package org.apache.doris.connector.api.systable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.UUID;

public class ConnectorScanPlanProviderRefTest {

    @Test
    public void ofStringRoundTripAndEquality() {
        ConnectorScanPlanProviderRef a = ConnectorScanPlanProviderRef.of("abc");
        ConnectorScanPlanProviderRef b = ConnectorScanPlanProviderRef.of("abc");
        Assertions.assertEquals("abc", a.id());
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        Assertions.assertTrue(a.toString().contains("abc"));
    }

    @Test
    public void ofUuidUsesUuidToString() {
        UUID u = UUID.randomUUID();
        ConnectorScanPlanProviderRef r = ConnectorScanPlanProviderRef.of(u);
        Assertions.assertEquals(u.toString(), r.id());
    }

    @Test
    public void rejectsNullAndEmpty() {
        Assertions.assertThrows(NullPointerException.class,
                () -> ConnectorScanPlanProviderRef.of((String) null));
        Assertions.assertThrows(NullPointerException.class,
                () -> ConnectorScanPlanProviderRef.of((UUID) null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorScanPlanProviderRef.of(""));
    }
}
