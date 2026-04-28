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

package org.apache.doris.connector.api.action;

import org.apache.doris.connector.api.ConnectorTableId;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class ActionInvocationTest {

    @Test
    public void builderHappyPath() {
        ActionInvocation inv = ActionInvocation.builder()
                .name("rewrite_data_files")
                .scope(ConnectorActionOps.Scope.TABLE)
                .target(ActionTarget.ofTable(ConnectorTableId.of("db", "t")))
                .putArgument("target_file_size_bytes", 134217728L)
                .invocationId("inv-1")
                .build();
        Assertions.assertEquals("rewrite_data_files", inv.name());
        Assertions.assertEquals(ConnectorActionOps.Scope.TABLE, inv.scope());
        Assertions.assertTrue(inv.target().isPresent());
        Assertions.assertEquals(1, inv.arguments().size());
        Assertions.assertEquals(134217728L, inv.arguments().get("target_file_size_bytes"));
        Assertions.assertTrue(inv.invocationId().isPresent());
        Assertions.assertEquals("inv-1", inv.invocationId().get());
    }

    @Test
    public void targetAndInvocationIdOptional() {
        ActionInvocation inv = ActionInvocation.builder()
                .name("cleanup").scope(ConnectorActionOps.Scope.CATALOG).build();
        Assertions.assertTrue(inv.target().isEmpty());
        Assertions.assertTrue(inv.invocationId().isEmpty());
        Assertions.assertTrue(inv.arguments().isEmpty());
    }

    @Test
    public void argumentsImmutableAndDoNotLeak() {
        Map<String, Object> src = new HashMap<>();
        src.put("a", 1);
        ActionInvocation inv = ActionInvocation.builder()
                .name("x").scope(ConnectorActionOps.Scope.TABLE)
                .arguments(src).build();
        src.put("b", 2);
        Assertions.assertEquals(1, inv.arguments().size());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> inv.arguments().put("c", 3));
    }

    @Test
    public void blankNameRejected() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ActionInvocation.builder().name(" ").scope(ConnectorActionOps.Scope.TABLE).build());
    }

    @Test
    public void nullScopeRejected() {
        Assertions.assertThrows(NullPointerException.class,
                () -> ActionInvocation.builder().name("x").build());
    }
}
