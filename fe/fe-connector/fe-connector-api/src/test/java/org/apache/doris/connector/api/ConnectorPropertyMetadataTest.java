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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

class ConnectorPropertyMetadataTest {

    // ----- v1 back-compat -----

    @Test
    void v1StringProperty() {
        ConnectorPropertyMetadata<String> m =
                ConnectorPropertyMetadata.stringProperty("k", "desc", "def");
        Assertions.assertEquals("k", m.getName());
        Assertions.assertEquals("desc", m.getDescription());
        Assertions.assertEquals(String.class, m.getType());
        Assertions.assertEquals("def", m.getDefaultValue());
        Assertions.assertFalse(m.isRequired());
        Assertions.assertEquals(PropertyValueType.STRING, m.getValueType());
        Assertions.assertEquals(PropertyScope.CATALOG, m.getScope());
        Assertions.assertTrue(m.getAliases().isEmpty());
        Assertions.assertFalse(m.isSensitive());
    }

    @Test
    void v1IntProperty() {
        ConnectorPropertyMetadata<Integer> m =
                ConnectorPropertyMetadata.intProperty("port", "tcp port", 9030);
        Assertions.assertEquals(Integer.class, m.getType());
        Assertions.assertEquals(Integer.valueOf(9030), m.getDefaultValue());
        Assertions.assertEquals(PropertyValueType.INT, m.getValueType());
        Assertions.assertFalse(m.isRequired());
        Assertions.assertEquals(PropertyScope.CATALOG, m.getScope());
    }

    @Test
    void v1BooleanProperty() {
        ConnectorPropertyMetadata<Boolean> m =
                ConnectorPropertyMetadata.booleanProperty("enable", "x", true);
        Assertions.assertEquals(Boolean.class, m.getType());
        Assertions.assertEquals(Boolean.TRUE, m.getDefaultValue());
        Assertions.assertEquals(PropertyValueType.BOOLEAN, m.getValueType());
        Assertions.assertFalse(m.isRequired());
        Assertions.assertEquals(PropertyScope.CATALOG, m.getScope());
    }

    @Test
    void v1RequiredStringProperty() {
        ConnectorPropertyMetadata<String> m =
                ConnectorPropertyMetadata.requiredStringProperty("uri", "the uri");
        Assertions.assertEquals(String.class, m.getType());
        Assertions.assertNull(m.getDefaultValue());
        Assertions.assertTrue(m.isRequired());
        Assertions.assertEquals(PropertyValueType.STRING, m.getValueType());
        Assertions.assertEquals(PropertyScope.CATALOG, m.getScope());
    }

    // ----- builder happy path per type -----

    @Test
    void builderHappyPathAllTypes() {
        Assertions.assertEquals(String.class,
                ConnectorPropertyMetadata.<String>builder("a", PropertyValueType.STRING)
                        .defaultValue("x").build().getType());
        Assertions.assertEquals(Integer.class,
                ConnectorPropertyMetadata.<Integer>builder("a", PropertyValueType.INT)
                        .defaultValue(1).build().getType());
        Assertions.assertEquals(Long.class,
                ConnectorPropertyMetadata.<Long>builder("a", PropertyValueType.LONG)
                        .defaultValue(1L).build().getType());
        Assertions.assertEquals(Boolean.class,
                ConnectorPropertyMetadata.<Boolean>builder("a", PropertyValueType.BOOLEAN)
                        .defaultValue(true).build().getType());
        Assertions.assertEquals(Duration.class,
                ConnectorPropertyMetadata.<Duration>builder("a", PropertyValueType.DURATION)
                        .defaultValue(Duration.ofSeconds(1)).build().getType());
        Assertions.assertEquals(Long.class,
                ConnectorPropertyMetadata.<Long>builder("a", PropertyValueType.SIZE)
                        .defaultValue(1024L).build().getType());
        Assertions.assertEquals(String.class,
                ConnectorPropertyMetadata.<String>builder("a", PropertyValueType.ENUM)
                        .enumValues("X", "Y").defaultValue("X").build().getType());
        Assertions.assertEquals(List.class,
                ConnectorPropertyMetadata.<List<String>>builder("a", PropertyValueType.STRING_LIST)
                        .defaultValue(Arrays.asList("a", "b")).build().getType());
        Assertions.assertEquals(Map.class,
                ConnectorPropertyMetadata.<Map<String, String>>builder("a", PropertyValueType.MAP)
                        .defaultValue(Collections.singletonMap("k", "v")).build().getType());
    }

    // ----- builder validation -----

    @Test
    void builderRejectsTypeMismatch() {
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> {
                    // Force a type mismatch via raw types.
                    @SuppressWarnings({"rawtypes", "unchecked"})
                    ConnectorPropertyMetadata.Builder b =
                            ConnectorPropertyMetadata.builder("k", PropertyValueType.INT);
                    b.defaultValue("not an int");
                    b.build();
                });
        Assertions.assertTrue(ex.getMessage().contains("not assignable"));
    }

    @Test
    void builderRejectsEnumWithoutValues() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorPropertyMetadata.<String>builder("k", PropertyValueType.ENUM).build());
    }

    @Test
    void builderRejectsAliasEqualsName() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorPropertyMetadata.<String>builder("k", PropertyValueType.STRING)
                        .aliases("k", "k_alt")
                        .build());
    }

    // ----- new fields round-trip -----

    @Test
    void allNewFieldsRoundTrip() {
        ConnectorPropertyMetadata.Deprecation dep = ConnectorPropertyMetadata.Deprecation.builder()
                .replacement("new.k")
                .since("3.0")
                .message("use new.k instead")
                .build();
        PropertyValidator<String> v = (key, value, ctx) -> { };
        ConnectorPropertyMetadata<String> m =
                ConnectorPropertyMetadata.<String>builder("k", PropertyValueType.ENUM)
                        .description("d")
                        .aliases("k1", "k2")
                        .defaultValue("A")
                        .required(true)
                        .sensitive(true)
                        .hidden(true)
                        .deprecated(dep)
                        .experimental(true)
                        .scope(PropertyScope.SESSION)
                        .validator(v)
                        .enumValues("A", "B")
                        .build();
        Assertions.assertEquals(Arrays.asList("k1", "k2"), m.getAliases());
        Assertions.assertTrue(m.isSensitive());
        Assertions.assertTrue(m.isHidden());
        Assertions.assertEquals(Optional.of(dep), m.getDeprecated());
        Assertions.assertEquals("new.k", m.getDeprecated().get().getReplacement());
        Assertions.assertEquals("3.0", m.getDeprecated().get().getSince());
        Assertions.assertTrue(m.isExperimental());
        Assertions.assertEquals(PropertyScope.SESSION, m.getScope());
        Assertions.assertSame(v, m.getValidator().orElseThrow(AssertionError::new));
        Assertions.assertEquals(Arrays.asList("A", "B"),
                m.getEnumValues().orElseThrow(AssertionError::new));
    }

    @Test
    void aliasesAreImmutable() {
        ConnectorPropertyMetadata<String> m =
                ConnectorPropertyMetadata.<String>builder("k", PropertyValueType.STRING)
                        .aliases("a1")
                        .build();
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> m.getAliases().add("x"));
    }

    // ----- equals / hashCode include new fields -----

    @Test
    void equalsDistinguishesSensitive() {
        ConnectorPropertyMetadata<String> a =
                ConnectorPropertyMetadata.<String>builder("k", PropertyValueType.STRING)
                        .sensitive(true).build();
        ConnectorPropertyMetadata<String> b =
                ConnectorPropertyMetadata.<String>builder("k", PropertyValueType.STRING)
                        .sensitive(false).build();
        Assertions.assertNotEquals(a, b);
        Assertions.assertNotEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void equalsDistinguishesScope() {
        ConnectorPropertyMetadata<String> a =
                ConnectorPropertyMetadata.<String>builder("k", PropertyValueType.STRING)
                        .scope(PropertyScope.CATALOG).build();
        ConnectorPropertyMetadata<String> b =
                ConnectorPropertyMetadata.<String>builder("k", PropertyValueType.STRING)
                        .scope(PropertyScope.TABLE).build();
        Assertions.assertNotEquals(a, b);
    }

    @Test
    void equalsHoldsForIdenticalBuilds() {
        ConnectorPropertyMetadata<String> a =
                ConnectorPropertyMetadata.<String>builder("k", PropertyValueType.STRING)
                        .description("d").aliases("a").defaultValue("v").build();
        ConnectorPropertyMetadata<String> b =
                ConnectorPropertyMetadata.<String>builder("k", PropertyValueType.STRING)
                        .description("d").aliases("a").defaultValue("v").build();
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
    }

    // ----- validator plumbing -----

    @Test
    void validatorIsInvokedWhenCalled() {
        AtomicInteger calls = new AtomicInteger();
        PropertyValidator<String> v = (key, value, ctx) -> {
            Assertions.assertEquals("k", key);
            Assertions.assertEquals("V", value);
            Assertions.assertTrue(ctx.allKeys().contains("k"));
            calls.incrementAndGet();
        };
        ConnectorPropertyMetadata<String> m =
                ConnectorPropertyMetadata.<String>builder("k", PropertyValueType.STRING)
                        .validator(v).build();
        PropertyValidator.ValidationContext ctx = new PropertyValidator.ValidationContext() {
            @Override
            public Set<String> allKeys() {
                return new HashSet<>(Collections.singleton("k"));
            }

            @Override
            public String rawValue(String key) {
                return "V";
            }
        };
        m.getValidator().orElseThrow(AssertionError::new).validate("k", "V", ctx);
        Assertions.assertEquals(1, calls.get());
    }

    @Test
    void validatorMayThrowConnectorPropertyException() {
        PropertyValidator<String> v = (key, value, ctx) -> {
            throw new ConnectorPropertyException("TYPE_MISMATCH", "bad: " + value);
        };
        ConnectorPropertyException ex = Assertions.assertThrows(
                ConnectorPropertyException.class,
                () -> v.validate("k", "x", null));
        Assertions.assertEquals("TYPE_MISMATCH", ex.getCode());
        Assertions.assertTrue(ex instanceof DorisConnectorException);
    }
}
