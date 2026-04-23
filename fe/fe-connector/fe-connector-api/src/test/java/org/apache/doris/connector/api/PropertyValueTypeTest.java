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

import java.time.Duration;
import java.util.List;
import java.util.Map;

class PropertyValueTypeTest {

    @Test
    void javaTypeNonNullForAllValues() {
        for (PropertyValueType v : PropertyValueType.values()) {
            Assertions.assertNotNull(v.javaType(), "javaType() must be non-null for " + v);
        }
    }

    @Test
    void canonicalJavaTypeMappings() {
        Assertions.assertEquals(String.class, PropertyValueType.STRING.javaType());
        Assertions.assertEquals(Integer.class, PropertyValueType.INT.javaType());
        Assertions.assertEquals(Long.class, PropertyValueType.LONG.javaType());
        Assertions.assertEquals(Boolean.class, PropertyValueType.BOOLEAN.javaType());
        Assertions.assertEquals(Duration.class, PropertyValueType.DURATION.javaType());
        Assertions.assertEquals(Long.class, PropertyValueType.SIZE.javaType());
        Assertions.assertEquals(String.class, PropertyValueType.ENUM.javaType());
        Assertions.assertEquals(List.class, PropertyValueType.STRING_LIST.javaType());
        Assertions.assertEquals(Map.class, PropertyValueType.MAP.javaType());
    }

    @Test
    void fromJavaTypeRoundTrip() {
        Assertions.assertEquals(PropertyValueType.STRING,
                PropertyValueType.fromJavaType(String.class));
        Assertions.assertEquals(PropertyValueType.INT,
                PropertyValueType.fromJavaType(Integer.class));
        Assertions.assertEquals(PropertyValueType.INT,
                PropertyValueType.fromJavaType(int.class));
        // LONG is the canonical answer for Long.class (SIZE shares the underlying type
        // but cannot be inferred from Java class alone).
        Assertions.assertEquals(PropertyValueType.LONG,
                PropertyValueType.fromJavaType(Long.class));
        Assertions.assertEquals(PropertyValueType.BOOLEAN,
                PropertyValueType.fromJavaType(Boolean.class));
        Assertions.assertEquals(PropertyValueType.DURATION,
                PropertyValueType.fromJavaType(Duration.class));
        Assertions.assertEquals(PropertyValueType.STRING_LIST,
                PropertyValueType.fromJavaType(List.class));
        Assertions.assertEquals(PropertyValueType.MAP,
                PropertyValueType.fromJavaType(Map.class));
    }

    @Test
    void fromJavaTypeRejectsUnknown() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PropertyValueType.fromJavaType(Object.class));
    }
}
