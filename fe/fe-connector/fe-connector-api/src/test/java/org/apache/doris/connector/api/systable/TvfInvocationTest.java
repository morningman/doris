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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class TvfInvocationTest {

    @Test
    public void happyPathPreservesOrderAndIsImmutable() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put("b", "2");
        props.put("a", "1");
        TvfInvocation inv = new TvfInvocation("iceberg_meta", props);

        Assertions.assertEquals("iceberg_meta", inv.functionName());
        Assertions.assertEquals(2, inv.properties().size());
        Assertions.assertEquals("[b, a]", inv.properties().keySet().toString());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> inv.properties().put("c", "3"));

        props.put("leak", "x");
        Assertions.assertFalse(inv.properties().containsKey("leak"));
    }

    @Test
    public void nullPropertiesBecomesEmpty() {
        TvfInvocation inv = new TvfInvocation("partitions", null);
        Assertions.assertTrue(inv.properties().isEmpty());
    }

    @Test
    public void equalsAndHashCode() {
        Map<String, String> p = Collections.singletonMap("k", "v");
        TvfInvocation a = new TvfInvocation("f", p);
        TvfInvocation b = new TvfInvocation("f", p);
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        Assertions.assertNotEquals(a, new TvfInvocation("g", p));
    }

    @Test
    public void rejectsNullOrEmptyName() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new TvfInvocation(null, Collections.emptyMap()));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new TvfInvocation("", Collections.emptyMap()));
    }
}
