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

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Logical value type of a {@link ConnectorPropertyMetadata}.
 *
 * <p>Each enum value maps to a canonical Java type via {@link #javaType()}; the resolver
 * uses this mapping to convert raw user-supplied strings into typed values.</p>
 */
public enum PropertyValueType {

    /** Plain UTF-8 string. */
    STRING(String.class),
    /** 32-bit signed integer. */
    INT(Integer.class),
    /** 64-bit signed integer. */
    LONG(Long.class),
    /** Boolean (true/false / 1/0). */
    BOOLEAN(Boolean.class),
    /** ISO-8601 / human-readable duration parsed into {@link java.time.Duration}. */
    DURATION(Duration.class),
    /** Size in bytes (e.g. "16MB"); stored as a {@code long}. */
    SIZE(Long.class),
    /** Constrained string from a fixed value set (see {@code enumValues}); stored as String. */
    ENUM(String.class),
    /** Comma-separated list of strings. */
    STRING_LIST(List.class),
    /** Key=value pairs (e.g. {@code k1=v1,k2=v2}). */
    MAP(Map.class);

    private final Class<?> javaType;

    PropertyValueType(Class<?> javaType) {
        this.javaType = javaType;
    }

    /** Canonical Java class associated with this value type. */
    public Class<?> javaType() {
        return javaType;
    }

    /**
     * Returns the canonical {@link PropertyValueType} for a Java class as used by the
     * v1 static factories ({@code stringProperty}, {@code intProperty}, ...).
     *
     * <p>Note: ambiguous mappings (e.g. {@code Long.class} maps to both {@link #LONG}
     * and {@link #SIZE}) resolve to the first match, which is {@link #LONG}.</p>
     *
     * @throws IllegalArgumentException if no canonical mapping exists
     */
    public static PropertyValueType fromJavaType(Class<?> clazz) {
        if (clazz == String.class) {
            return STRING;
        }
        if (clazz == Integer.class || clazz == int.class) {
            return INT;
        }
        if (clazz == Long.class || clazz == long.class) {
            return LONG;
        }
        if (clazz == Boolean.class || clazz == boolean.class) {
            return BOOLEAN;
        }
        if (clazz == Duration.class) {
            return DURATION;
        }
        if (List.class.isAssignableFrom(clazz)) {
            return STRING_LIST;
        }
        if (Map.class.isAssignableFrom(clazz)) {
            return MAP;
        }
        throw new IllegalArgumentException(
                "No canonical PropertyValueType for Java class: " + clazz.getName());
    }
}
