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

package org.apache.doris.connector.api.dispatch;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class DispatchTargetTest {

    @Test
    void builderHappyPath() {
        DispatchTarget t = DispatchTarget.builder("iceberg")
                .backendName("ice-be-1")
                .putNormalizedProp("warehouse", "s3://w")
                .build();
        Assertions.assertEquals("iceberg", t.connectorType());
        Assertions.assertEquals(Optional.of("ice-be-1"), t.backendName());
        Assertions.assertEquals("s3://w", t.normalizedProps().get("warehouse"));
    }

    @Test
    void blankConnectorTypeRejected() {
        Assertions.assertThrows(NullPointerException.class, () -> DispatchTarget.builder(null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> DispatchTarget.builder(""));
        Assertions.assertThrows(IllegalArgumentException.class, () -> DispatchTarget.builder("   "));
    }

    @Test
    void backendNameOptional() {
        DispatchTarget t = DispatchTarget.builder("iceberg").build();
        Assertions.assertEquals(Optional.empty(), t.backendName());
        Assertions.assertTrue(t.normalizedProps().isEmpty());
    }

    @Test
    void normalizedPropsImmutableAndDoesNotLeak() {
        Map<String, String> props = new HashMap<>();
        props.put("k", "v");
        DispatchTarget t = DispatchTarget.builder("iceberg").normalizedProps(props).build();
        props.put("k", "v2");
        Assertions.assertEquals("v", t.normalizedProps().get("k"));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> t.normalizedProps().put("x", "y"));
    }

    @Test
    void equalsHashCodeRoundTrip() {
        DispatchTarget a = DispatchTarget.builder("iceberg")
                .backendName("be").putNormalizedProp("k", "v").build();
        DispatchTarget b = DispatchTarget.builder("iceberg")
                .backendName("be").putNormalizedProp("k", "v").build();
        DispatchTarget c = DispatchTarget.builder("hudi")
                .backendName("be").putNormalizedProp("k", "v").build();
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        Assertions.assertNotEquals(a, c);
        Assertions.assertTrue(a.toString().contains("iceberg"));
    }
}
