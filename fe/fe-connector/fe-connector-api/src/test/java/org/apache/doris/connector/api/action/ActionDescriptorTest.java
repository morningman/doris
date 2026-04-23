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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class ActionDescriptorTest {

    @Test
    public void builderHappyPath() {
        ActionDescriptor d = ActionDescriptor.builder()
                .name("rewrite_data_files")
                .scope(ConnectorActionOps.Scope.TABLE)
                .addArgument(new ActionArgument("target_file_size_bytes", ActionArgumentType.LONG, false, "134217728"))
                .addResultColumn(new ActionDescriptor.ResultColumn("rewritten_files_count", ActionArgumentType.LONG))
                .transactional(true)
                .blocking(true)
                .idempotent(true)
                .build();
        Assertions.assertEquals("rewrite_data_files", d.name());
        Assertions.assertEquals(ConnectorActionOps.Scope.TABLE, d.scope());
        Assertions.assertEquals(1, d.arguments().size());
        Assertions.assertEquals(1, d.resultSchema().size());
        Assertions.assertTrue(d.transactional());
        Assertions.assertTrue(d.blocking());
        Assertions.assertTrue(d.idempotent());
    }

    @Test
    public void defaultsAreFalseAndEmpty() {
        ActionDescriptor d = ActionDescriptor.builder()
                .name("compact")
                .scope(ConnectorActionOps.Scope.TABLE)
                .build();
        Assertions.assertFalse(d.transactional());
        Assertions.assertFalse(d.blocking());
        Assertions.assertFalse(d.idempotent());
        Assertions.assertTrue(d.arguments().isEmpty());
        Assertions.assertTrue(d.resultSchema().isEmpty());
    }

    @Test
    public void blankNameRejected() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ActionDescriptor.builder().name(" ").scope(ConnectorActionOps.Scope.TABLE).build());
    }

    @Test
    public void nullNameRejected() {
        Assertions.assertThrows(NullPointerException.class,
                () -> ActionDescriptor.builder().scope(ConnectorActionOps.Scope.TABLE).build());
    }

    @Test
    public void nullScopeRejected() {
        Assertions.assertThrows(NullPointerException.class,
                () -> ActionDescriptor.builder().name("x").build());
    }

    @Test
    public void argumentsListImmutable() {
        List<ActionArgument> args = new ArrayList<>();
        args.add(new ActionArgument("a", ActionArgumentType.STRING, true));
        ActionDescriptor d = ActionDescriptor.builder()
                .name("x").scope(ConnectorActionOps.Scope.TABLE)
                .arguments(args).build();
        args.add(new ActionArgument("b", ActionArgumentType.STRING, true));
        Assertions.assertEquals(1, d.arguments().size());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> d.arguments().add(new ActionArgument("c", ActionArgumentType.STRING, true)));
    }

    @Test
    public void resultSchemaListImmutable() {
        List<ActionDescriptor.ResultColumn> cols = new ArrayList<>();
        cols.add(new ActionDescriptor.ResultColumn("a", ActionArgumentType.LONG));
        ActionDescriptor d = ActionDescriptor.builder()
                .name("x").scope(ConnectorActionOps.Scope.TABLE)
                .resultSchema(cols).build();
        cols.add(new ActionDescriptor.ResultColumn("b", ActionArgumentType.LONG));
        Assertions.assertEquals(1, d.resultSchema().size());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> d.resultSchema().add(new ActionDescriptor.ResultColumn("c", ActionArgumentType.LONG)));
    }

    @Test
    public void resultColumnValidatesName() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new ActionDescriptor.ResultColumn(" ", ActionArgumentType.LONG));
        Assertions.assertThrows(NullPointerException.class,
                () -> new ActionDescriptor.ResultColumn(null, ActionArgumentType.LONG));
        Assertions.assertThrows(NullPointerException.class,
                () -> new ActionDescriptor.ResultColumn("x", null));
    }
}
