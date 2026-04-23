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

package org.apache.doris.connector;

import org.apache.doris.connector.api.ConnectorPropertyException;
import org.apache.doris.connector.api.ConnectorPropertyMetadata;
import org.apache.doris.connector.api.PropertyValidator;
import org.apache.doris.connector.api.PropertyValueType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link ConnectorPropertyResolver} (M0-04).
 */
public class ConnectorPropertyResolverTest {

    private static <T> ConnectorPropertyMetadata.Builder<T> builder(String name, PropertyValueType t) {
        return ConnectorPropertyMetadata.builder(name, t);
    }

    private static Map<String, String> map(String... kv) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    // ---------- happy path: every type round-trips ----------

    @Test
    public void resolvesAllTypes() {
        List<ConnectorPropertyMetadata<?>> ds = Arrays.asList(
                builder("s", PropertyValueType.STRING).build(),
                builder("i", PropertyValueType.INT).build(),
                builder("l", PropertyValueType.LONG).build(),
                builder("bo", PropertyValueType.BOOLEAN).build(),
                builder("dur", PropertyValueType.DURATION).build(),
                builder("dur2", PropertyValueType.DURATION).build(),
                builder("sz", PropertyValueType.SIZE).build(),
                builder("sz2", PropertyValueType.SIZE).build(),
                builder("en", PropertyValueType.ENUM).enumValues("A", "B", "C").build(),
                builder("lst", PropertyValueType.STRING_LIST).build(),
                builder("mp", PropertyValueType.MAP).build()
        );
        Map<String, String> raw = map(
                "s", "hello",
                "i", "42",
                "l", "1234567890123",
                "bo", "TRUE",
                "dur", "PT5S",
                "dur2", "10ms",
                "sz", "16m",
                "sz2", "2048",
                "en", "B",
                "lst", "a, b ,, c",
                "mp", "k1=v1, k2 = v2");

        Map<String, Object> r = ConnectorPropertyResolver.resolve(ds, raw);

        Assertions.assertEquals("hello", r.get("s"));
        Assertions.assertEquals(42, r.get("i"));
        Assertions.assertEquals(1234567890123L, r.get("l"));
        Assertions.assertEquals(Boolean.TRUE, r.get("bo"));
        Assertions.assertEquals(Duration.ofSeconds(5), r.get("dur"));
        Assertions.assertEquals(Duration.ofMillis(10), r.get("dur2"));
        Assertions.assertEquals(16L * 1024 * 1024, r.get("sz"));
        Assertions.assertEquals(2048L, r.get("sz2"));
        Assertions.assertEquals("B", r.get("en"));
        Assertions.assertEquals(Arrays.asList("a", "b", "c"), r.get("lst"));
        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("k1", "v1");
        expected.put("k2", "v2");
        Assertions.assertEquals(expected, r.get("mp"));
    }

    // ---------- alias collapse ----------

    @Test
    public void aliasCanonicalization() {
        List<ConnectorPropertyMetadata<?>> ds = Collections.singletonList(
                builder("user", PropertyValueType.STRING).aliases("username").build());
        Map<String, Object> r = ConnectorPropertyResolver.resolve(ds, map("username", "alice"));
        Assertions.assertEquals("alice", r.get("user"));
        Assertions.assertFalse(r.containsKey("username"));
    }

    @Test
    public void ambiguousAliasIsRejected() {
        List<ConnectorPropertyMetadata<?>> ds = Collections.singletonList(
                builder("user", PropertyValueType.STRING).aliases("username").build());
        ConnectorPropertyException e = Assertions.assertThrows(
                ConnectorPropertyException.class,
                () -> ConnectorPropertyResolver.resolve(ds, map("user", "a", "username", "b")));
        Assertions.assertEquals("AMBIGUOUS_ALIAS", e.getCode());
    }

    // ---------- required ----------

    @Test
    public void requiredMissing() {
        List<ConnectorPropertyMetadata<?>> ds = Collections.singletonList(
                builder("uri", PropertyValueType.STRING).required(true).build());
        ConnectorPropertyException e = Assertions.assertThrows(
                ConnectorPropertyException.class,
                () -> ConnectorPropertyResolver.resolve(ds, Collections.emptyMap()));
        Assertions.assertEquals("REQUIRED_MISSING", e.getCode());
    }

    // ---------- type mismatch ----------

    @Test
    public void typeMismatchInt() {
        List<ConnectorPropertyMetadata<?>> ds = Collections.singletonList(
                builder("i", PropertyValueType.INT).build());
        ConnectorPropertyException e = Assertions.assertThrows(
                ConnectorPropertyException.class,
                () -> ConnectorPropertyResolver.resolve(ds, map("i", "abc")));
        Assertions.assertEquals("TYPE_MISMATCH", e.getCode());
    }

    @Test
    public void typeMismatchBoolean() {
        List<ConnectorPropertyMetadata<?>> ds = Collections.singletonList(
                builder("bo", PropertyValueType.BOOLEAN).build());
        ConnectorPropertyException e = Assertions.assertThrows(
                ConnectorPropertyException.class,
                () -> ConnectorPropertyResolver.resolve(ds, map("bo", "yes")));
        Assertions.assertEquals("TYPE_MISMATCH", e.getCode());
    }

    @Test
    public void typeMismatchSize() {
        List<ConnectorPropertyMetadata<?>> ds = Collections.singletonList(
                builder("sz", PropertyValueType.SIZE).build());
        ConnectorPropertyException e = Assertions.assertThrows(
                ConnectorPropertyException.class,
                () -> ConnectorPropertyResolver.resolve(ds, map("sz", "huge")));
        Assertions.assertEquals("TYPE_MISMATCH", e.getCode());
    }

    @Test
    public void typeMismatchDuration() {
        List<ConnectorPropertyMetadata<?>> ds = Collections.singletonList(
                builder("d", PropertyValueType.DURATION).build());
        ConnectorPropertyException e = Assertions.assertThrows(
                ConnectorPropertyException.class,
                () -> ConnectorPropertyResolver.resolve(ds, map("d", "soon")));
        Assertions.assertEquals("TYPE_MISMATCH", e.getCode());
    }

    @Test
    public void enumValueNotAllowed() {
        // ENUM mismatch surfaces as TYPE_MISMATCH (chose unified code; see handoff).
        List<ConnectorPropertyMetadata<?>> ds = Collections.singletonList(
                builder("mode", PropertyValueType.ENUM).enumValues("READ", "WRITE").build());
        ConnectorPropertyException e = Assertions.assertThrows(
                ConnectorPropertyException.class,
                () -> ConnectorPropertyResolver.resolve(ds, map("mode", "read")));
        Assertions.assertEquals("TYPE_MISMATCH", e.getCode());
    }

    // ---------- unknown property ----------

    @Test
    public void unknownPropertyRejected() {
        List<ConnectorPropertyMetadata<?>> ds = Collections.singletonList(
                builder("known", PropertyValueType.STRING).build());
        ConnectorPropertyException e = Assertions.assertThrows(
                ConnectorPropertyException.class,
                () -> ConnectorPropertyResolver.resolve(ds, map("known", "v", "extra", "x")));
        Assertions.assertEquals("UNKNOWN_PROPERTY", e.getCode());
    }

    // ---------- validator ----------

    @Test
    public void validatorIsInvokedAndCanThrow() {
        PropertyValidator<Integer> v = (key, value, ctx) -> {
            if (value < 0) {
                throw new ConnectorPropertyException("OUT_OF_RANGE",
                        "property '" + key + "' must be >= 0");
            }
        };
        ConnectorPropertyMetadata<Integer> md = ConnectorPropertyMetadata.<Integer>builder(
                "n", PropertyValueType.INT).validator(v).build();
        List<ConnectorPropertyMetadata<?>> ds = Collections.singletonList(md);

        ConnectorPropertyException e = Assertions.assertThrows(
                ConnectorPropertyException.class,
                () -> ConnectorPropertyResolver.resolve(ds, map("n", "-1")));
        Assertions.assertEquals("OUT_OF_RANGE", e.getCode());

        Map<String, Object> ok = ConnectorPropertyResolver.resolve(ds, map("n", "5"));
        Assertions.assertEquals(5, ok.get("n"));
    }

    // ---------- defaults ----------

    @Test
    public void defaultsFilledForAbsentOptionals() {
        ConnectorPropertyMetadata<String> withDefault = ConnectorPropertyMetadata.<String>builder(
                "region", PropertyValueType.STRING).defaultValue("us-east-1").build();
        ConnectorPropertyMetadata<String> noDefault = ConnectorPropertyMetadata.<String>builder(
                "tag", PropertyValueType.STRING).build();
        List<ConnectorPropertyMetadata<?>> ds = Arrays.asList(withDefault, noDefault);

        Map<String, Object> r = ConnectorPropertyResolver.resolve(ds, Collections.emptyMap());
        Assertions.assertEquals("us-east-1", r.get("region"));
        Assertions.assertTrue(r.containsKey("tag"));
        Assertions.assertNull(r.get("tag"));
    }

    // ---------- sanitize ----------

    @Test
    public void sanitizeMasksSensitive() {
        List<ConnectorPropertyMetadata<?>> ds = Arrays.asList(
                ConnectorPropertyMetadata.<String>builder("user", PropertyValueType.STRING).build(),
                ConnectorPropertyMetadata.<String>builder("pwd", PropertyValueType.STRING)
                        .sensitive(true).build(),
                ConnectorPropertyMetadata.<String>builder("absent", PropertyValueType.STRING).build());

        Map<String, String> sanitized = ConnectorPropertyResolver.sanitize(
                ds, map("user", "alice", "pwd", "secret"));

        Assertions.assertEquals("alice", sanitized.get("user"));
        Assertions.assertEquals("***", sanitized.get("pwd"));
        Assertions.assertFalse(sanitized.containsKey("absent"));
    }

    // ---------- deprecation ----------

    @Test
    public void deprecatedPropertyStillResolves() {
        ConnectorPropertyMetadata.Deprecation dep = ConnectorPropertyMetadata.Deprecation.builder()
                .replacement("new_key").since("2.1").message("renamed").build();
        ConnectorPropertyMetadata<String> md = ConnectorPropertyMetadata.<String>builder(
                "old_key", PropertyValueType.STRING).deprecated(dep).build();
        Map<String, Object> r = ConnectorPropertyResolver.resolve(
                Collections.singletonList(md), map("old_key", "v"));
        Assertions.assertEquals("v", r.get("old_key"));
    }

    // ---------- nulls / empties ----------

    @Test
    public void nullDescriptorsAndRawAreSafe() {
        Map<String, Object> r = ConnectorPropertyResolver.resolve(null, null);
        Assertions.assertTrue(r.isEmpty());
    }

    @Test
    public void emptyRawWithRequiredFails() {
        List<ConnectorPropertyMetadata<?>> ds = Collections.singletonList(
                builder("uri", PropertyValueType.STRING).required(true).build());
        Assertions.assertThrows(ConnectorPropertyException.class,
                () -> ConnectorPropertyResolver.resolve(ds, new HashMap<>()));
    }
}
