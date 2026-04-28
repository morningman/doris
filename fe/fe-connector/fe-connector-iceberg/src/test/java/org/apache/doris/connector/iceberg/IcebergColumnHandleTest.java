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

package org.apache.doris.connector.iceberg;

import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class IcebergColumnHandleTest {

    @Test
    void minimalCtorPopulatesFields() {
        IcebergColumnHandle h = new IcebergColumnHandle(
                7, "id", Types.LongType.get(), false);
        Assertions.assertEquals(7, h.getFieldId());
        Assertions.assertEquals("id", h.getName());
        Assertions.assertEquals(Types.LongType.get(), h.getIcebergType());
        Assertions.assertFalse(h.isNullable());
        Assertions.assertNull(h.getComment());
        Assertions.assertNull(h.getPosition());
    }

    @Test
    void fullCtorPopulatesOptionalFields() {
        IcebergColumnHandle h = new IcebergColumnHandle(
                3, "name", Types.StringType.get(), true, "user name", 0);
        Assertions.assertEquals("user name", h.getComment());
        Assertions.assertEquals(0, h.getPosition());
        Assertions.assertTrue(h.isNullable());
    }

    @Test
    void equalsAndHashCodeUseAllFields() {
        IcebergColumnHandle a = new IcebergColumnHandle(
                1, "c", Types.IntegerType.get(), true, "doc", 4);
        IcebergColumnHandle b = new IcebergColumnHandle(
                1, "c", Types.IntegerType.get(), true, "doc", 4);
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());

        Assertions.assertNotEquals(a,
                new IcebergColumnHandle(2, "c", Types.IntegerType.get(), true, "doc", 4));
        Assertions.assertNotEquals(a,
                new IcebergColumnHandle(1, "x", Types.IntegerType.get(), true, "doc", 4));
        Assertions.assertNotEquals(a,
                new IcebergColumnHandle(1, "c", Types.LongType.get(), true, "doc", 4));
        Assertions.assertNotEquals(a,
                new IcebergColumnHandle(1, "c", Types.IntegerType.get(), false, "doc", 4));
        Assertions.assertNotEquals(a,
                new IcebergColumnHandle(1, "c", Types.IntegerType.get(), true, "other", 4));
        Assertions.assertNotEquals(a,
                new IcebergColumnHandle(1, "c", Types.IntegerType.get(), true, "doc", 5));
        Assertions.assertNotEquals(a, "string");
    }

    @Test
    void preservesOrderingInList() {
        List<IcebergColumnHandle> list = new ArrayList<>();
        list.add(new IcebergColumnHandle(10, "z", Types.StringType.get(), true, null, 0));
        list.add(new IcebergColumnHandle(20, "a", Types.LongType.get(), false, null, 1));
        list.add(new IcebergColumnHandle(30, "m", Types.IntegerType.get(), true, null, 2));
        Assertions.assertEquals("z", list.get(0).getName());
        Assertions.assertEquals("a", list.get(1).getName());
        Assertions.assertEquals("m", list.get(2).getName());
        Assertions.assertEquals(0, list.get(0).getPosition());
        Assertions.assertEquals(2, list.get(2).getPosition());
    }

    @Test
    void nullableFlagDistinguishesHandles() {
        IcebergColumnHandle nullable = new IcebergColumnHandle(
                1, "c", Types.StringType.get(), true);
        IcebergColumnHandle required = new IcebergColumnHandle(
                1, "c", Types.StringType.get(), false);
        Assertions.assertNotEquals(nullable, required);
        Assertions.assertTrue(nullable.isNullable());
        Assertions.assertFalse(required.isNullable());
    }

    @Test
    void requireNonNullForNameAndType() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new IcebergColumnHandle(1, null, Types.IntegerType.get(), true));
        Assertions.assertThrows(NullPointerException.class,
                () -> new IcebergColumnHandle(1, "n", null, true));
    }

    @Test
    void toStringContainsCoreFields() {
        IcebergColumnHandle h = new IcebergColumnHandle(
                42, "answer", Types.LongType.get(), true, "why", 7);
        String s = h.toString();
        Assertions.assertTrue(s.contains("fieldId=42"), s);
        Assertions.assertTrue(s.contains("name=answer"), s);
        Assertions.assertTrue(s.contains("position=7"), s);
    }
}
