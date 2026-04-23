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

package org.apache.doris.connector.api.policy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;

public class PolicyChangeNotificationTest {

    @Test
    public void allFieldsRetained() {
        Instant t = Instant.ofEpochMilli(1700000000000L);
        PolicyChangeNotification n = new PolicyChangeNotification(
                "hive_cat", "db", Optional.of("t1"), PolicyKind.ROW_FILTER, t, Optional.of("pid-1"));
        Assertions.assertEquals("hive_cat", n.catalog());
        Assertions.assertEquals("db", n.database());
        Assertions.assertEquals("t1", n.table().orElse(""));
        Assertions.assertEquals(PolicyKind.ROW_FILTER, n.policyKind());
        Assertions.assertEquals(t, n.eventTime());
        Assertions.assertEquals("pid-1", n.policyId().orElse(""));
    }

    @Test
    public void rejectsNulls() {
        Instant t = Instant.now();
        Assertions.assertThrows(NullPointerException.class,
                () -> new PolicyChangeNotification(null, "d", Optional.empty(),
                        PolicyKind.COLUMN_MASK, t, Optional.empty()));
        Assertions.assertThrows(NullPointerException.class,
                () -> new PolicyChangeNotification("c", null, Optional.empty(),
                        PolicyKind.COLUMN_MASK, t, Optional.empty()));
        Assertions.assertThrows(NullPointerException.class,
                () -> new PolicyChangeNotification("c", "d", null,
                        PolicyKind.COLUMN_MASK, t, Optional.empty()));
        Assertions.assertThrows(NullPointerException.class,
                () -> new PolicyChangeNotification("c", "d", Optional.empty(),
                        null, t, Optional.empty()));
        Assertions.assertThrows(NullPointerException.class,
                () -> new PolicyChangeNotification("c", "d", Optional.empty(),
                        PolicyKind.COLUMN_MASK, null, Optional.empty()));
        Assertions.assertThrows(NullPointerException.class,
                () -> new PolicyChangeNotification("c", "d", Optional.empty(),
                        PolicyKind.COLUMN_MASK, t, null));
    }
}
