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

package org.apache.doris.connector.api.event;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TableIdentifierTest {

    @Test
    void requiredFields() {
        TableIdentifier id = new TableIdentifier("d", "t");
        Assertions.assertEquals("d", id.database());
        Assertions.assertEquals("t", id.table());
        Assertions.assertTrue(id.toString().contains("d.t"));
    }

    @Test
    void equalsAndHash() {
        TableIdentifier a = new TableIdentifier("d", "t");
        TableIdentifier b = new TableIdentifier("d", "t");
        TableIdentifier c = new TableIdentifier("d", "u");
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        Assertions.assertNotEquals(a, c);
    }

    @Test
    void nullRejected() {
        Assertions.assertThrows(NullPointerException.class, () -> new TableIdentifier(null, "t"));
        Assertions.assertThrows(NullPointerException.class, () -> new TableIdentifier("d", null));
    }
}
