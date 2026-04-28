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

package org.apache.doris.connector.api;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class ConnectorTableIdTest {

    @Test
    public void ofCarriesPair() {
        ConnectorTableId id = ConnectorTableId.of("db", "t");
        Assertions.assertEquals("db", id.database());
        Assertions.assertEquals("t", id.table());
    }

    @Test
    public void ofRejectsNulls() {
        Assertions.assertThrows(NullPointerException.class,
                () -> ConnectorTableId.of(null, "t"));
        Assertions.assertThrows(NullPointerException.class,
                () -> ConnectorTableId.of("db", null));
    }

    @Test
    public void equalsAndHashCodeAreOnPair() {
        Assertions.assertEquals(ConnectorTableId.of("db", "t"),
                ConnectorTableId.of("db", "t"));
        Assertions.assertEquals(ConnectorTableId.of("db", "t").hashCode(),
                ConnectorTableId.of("db", "t").hashCode());
        Assertions.assertNotEquals(ConnectorTableId.of("db", "t"),
                ConnectorTableId.of("db", "u"));
        Assertions.assertNotEquals(ConnectorTableId.of("db", "t"),
                ConnectorTableId.of("other", "t"));
        Assertions.assertNotEquals(ConnectorTableId.of("db", "t"), "db.t");
    }

    @Test
    public void toStringIsDottedPair() {
        Assertions.assertEquals("db.t", ConnectorTableId.of("db", "t").toString());
    }

    @Test
    public void serializationRoundTrip() throws Exception {
        ConnectorTableId original = ConnectorTableId.of("db", "t");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(original);
        }
        ConnectorTableId restored;
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
            restored = (ConnectorTableId) ois.readObject();
        }
        Assertions.assertEquals(original, restored);
        Assertions.assertEquals("db", restored.database());
        Assertions.assertEquals("t", restored.table());
    }
}
