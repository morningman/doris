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

package org.apache.doris.connector.api.action;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ActionTargetTest {

    @Test
    public void ofTableCarriesBothNames() {
        ActionTarget t = ActionTarget.ofTable("db1", "t1");
        Assertions.assertEquals("db1", t.database());
        Assertions.assertTrue(t.table().isPresent());
        Assertions.assertEquals("t1", t.table().get());
    }

    @Test
    public void ofSchemaHasNoTable() {
        ActionTarget t = ActionTarget.ofSchema("db1");
        Assertions.assertEquals("db1", t.database());
        Assertions.assertTrue(t.table().isEmpty());
    }

    @Test
    public void blankDatabaseRejected() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ActionTarget.ofSchema(""));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ActionTarget.ofTable("  ", "t"));
    }

    @Test
    public void blankTableRejected() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ActionTarget.ofTable("db", " "));
    }

    @Test
    public void nullRejected() {
        Assertions.assertThrows(NullPointerException.class,
                () -> ActionTarget.ofSchema(null));
        Assertions.assertThrows(NullPointerException.class,
                () -> ActionTarget.ofTable("db", null));
    }

    @Test
    public void equalityAndHash() {
        Assertions.assertEquals(ActionTarget.ofTable("d", "t"), ActionTarget.ofTable("d", "t"));
        Assertions.assertEquals(ActionTarget.ofTable("d", "t").hashCode(),
                ActionTarget.ofTable("d", "t").hashCode());
        Assertions.assertNotEquals(ActionTarget.ofTable("d", "t"), ActionTarget.ofSchema("d"));
    }
}
