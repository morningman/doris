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

package org.apache.doris.connector.api.cache;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;

public class ConnectorCacheSpecTest {

    @Test
    public void defaultsRoundTrip() {
        ConnectorCacheSpec spec = ConnectorCacheSpec.defaults();
        Assertions.assertEquals(10_000L, spec.getMaxSize());
        Assertions.assertEquals(Duration.ofHours(1), spec.getTtl());
        Assertions.assertNull(spec.getRefreshAfter());
        Assertions.assertFalse(spec.isSoftValues());
        Assertions.assertEquals(RefreshPolicy.TTL, spec.getRefreshPolicy());
    }

    @Test
    public void builderHappyPath() {
        ConnectorCacheSpec spec = ConnectorCacheSpec.builder()
                .maxSize(42)
                .ttl(Duration.ofMinutes(5))
                .refreshAfter(Duration.ofMinutes(1))
                .softValues(true)
                .refreshPolicy(RefreshPolicy.TTL)
                .build();
        Assertions.assertEquals(42, spec.getMaxSize());
        Assertions.assertEquals(Duration.ofMinutes(5), spec.getTtl());
        Assertions.assertEquals(Duration.ofMinutes(1), spec.getRefreshAfter());
        Assertions.assertTrue(spec.isSoftValues());
        Assertions.assertEquals(RefreshPolicy.TTL, spec.getRefreshPolicy());
    }

    @Test
    public void rejectsNonPositiveMaxSize() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorCacheSpec.builder().maxSize(0).build());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorCacheSpec.builder().maxSize(-1).build());
    }

    @Test
    public void rejectsNullTtl() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorCacheSpec.builder().ttl(null).build());
    }

    @Test
    public void rejectsRefreshAfterGreaterThanTtl() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorCacheSpec.builder()
                        .ttl(Duration.ofMinutes(1))
                        .refreshAfter(Duration.ofMinutes(2))
                        .build());
    }

    @Test
    public void rejectsRefreshAfterWhenPolicyNotTtl() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorCacheSpec.builder()
                        .refreshPolicy(RefreshPolicy.MANUAL_ONLY)
                        .refreshAfter(Duration.ofSeconds(1))
                        .build());
    }

    @Test
    public void allowsRefreshAfterEqualToTtl() {
        ConnectorCacheSpec spec = ConnectorCacheSpec.builder()
                .ttl(Duration.ofMinutes(1))
                .refreshAfter(Duration.ofMinutes(1))
                .build();
        Assertions.assertEquals(Duration.ofMinutes(1), spec.getRefreshAfter());
    }

    @Test
    public void equalsAndHashCode() {
        ConnectorCacheSpec a = ConnectorCacheSpec.builder()
                .maxSize(7).ttl(Duration.ofSeconds(30)).build();
        ConnectorCacheSpec b = ConnectorCacheSpec.builder()
                .maxSize(7).ttl(Duration.ofSeconds(30)).build();
        ConnectorCacheSpec c = ConnectorCacheSpec.builder()
                .maxSize(8).ttl(Duration.ofSeconds(30)).build();
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        Assertions.assertNotEquals(a, c);
        Assertions.assertTrue(a.toString().contains("maxSize=7"));
    }
}
