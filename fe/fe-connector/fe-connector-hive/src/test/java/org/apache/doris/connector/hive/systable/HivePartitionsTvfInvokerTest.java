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

package org.apache.doris.connector.hive.systable;

import org.apache.doris.connector.api.systable.TvfInvocation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

class HivePartitionsTvfInvokerTest {

    @Test
    void resolveProducesPartitionValuesInvocation() {
        HivePartitionsTvfInvoker invoker = new HivePartitionsTvfInvoker();
        TvfInvocation inv = invoker.resolve("default", "orders", "partitions", Optional.empty());
        Assertions.assertEquals("partition_values", inv.functionName());
        Assertions.assertEquals("default", inv.properties().get("database"));
        Assertions.assertEquals("orders", inv.properties().get("table"));
        Assertions.assertEquals(2, inv.properties().size());
    }

    @Test
    void resolveStripsNullVersionViaOptional() {
        HivePartitionsTvfInvoker invoker = new HivePartitionsTvfInvoker();
        // version Optional is required to be present (Optional.empty is allowed),
        // not null — invoker is engine-facing and must reject misuse.
        Assertions.assertThrows(NullPointerException.class,
                () -> invoker.resolve("d", "t", "partitions", null));
    }

    @Test
    void resolveRejectsNullDatabaseTableOrName() {
        HivePartitionsTvfInvoker invoker = new HivePartitionsTvfInvoker();
        Assertions.assertThrows(NullPointerException.class,
                () -> invoker.resolve(null, "t", "partitions", Optional.empty()));
        Assertions.assertThrows(NullPointerException.class,
                () -> invoker.resolve("d", null, "partitions", Optional.empty()));
        Assertions.assertThrows(NullPointerException.class,
                () -> invoker.resolve("d", "t", null, Optional.empty()));
    }

    @Test
    void invocationPropertiesAreImmutable() {
        HivePartitionsTvfInvoker invoker = new HivePartitionsTvfInvoker();
        TvfInvocation inv = invoker.resolve("default", "orders", "partitions", Optional.empty());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> inv.properties().put("foo", "bar"));
    }

    @Test
    void resolveDoesNotPlaceCatalogProperty() {
        // The plugin layer is intentionally agnostic of fe-core's catalog name;
        // PluginTvfDispatcher in fe-core injects the catalog at TVF construction
        // time. Ensure the plugin invocation does NOT carry a 'catalog' key.
        HivePartitionsTvfInvoker invoker = new HivePartitionsTvfInvoker();
        TvfInvocation inv = invoker.resolve("d", "t", "partitions", Optional.empty());
        Assertions.assertFalse(inv.properties().containsKey("catalog"));
    }
}
