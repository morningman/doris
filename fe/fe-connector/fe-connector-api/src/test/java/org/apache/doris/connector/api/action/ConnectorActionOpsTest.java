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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ConnectorActionOpsTest {

    @Test
    public void scopeHasThreeValues() {
        Assertions.assertEquals(3, ConnectorActionOps.Scope.values().length);
        Assertions.assertNotNull(ConnectorActionOps.Scope.valueOf("CATALOG"));
        Assertions.assertNotNull(ConnectorActionOps.Scope.valueOf("SCHEMA"));
        Assertions.assertNotNull(ConnectorActionOps.Scope.valueOf("TABLE"));
    }

    @Test
    public void anonymousImplFiltersByScopeAndExecutes() {
        ActionDescriptor tableAction = ActionDescriptor.builder()
                .name("rewrite_data_files")
                .scope(ConnectorActionOps.Scope.TABLE)
                .addArgument(new ActionArgument("target_file_size_bytes",
                        ActionArgumentType.LONG, false, "134217728"))
                .addResultColumn(new ActionDescriptor.ResultColumn("files_rewritten", ActionArgumentType.LONG))
                .build();

        ConnectorActionOps ops = new ConnectorActionOps() {
            @Override
            public List<ActionDescriptor> listActions(Scope scope, Optional<ActionTarget> target) {
                if (scope == Scope.TABLE) {
                    return Collections.singletonList(tableAction);
                }
                return Collections.emptyList();
            }

            @Override
            public ActionResult executeAction(ActionInvocation invocation) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("files_rewritten", 7L);
                return ActionResult.completed(Collections.singletonList(row));
            }
        };

        List<ActionDescriptor> tableDescs = ops.listActions(
                ConnectorActionOps.Scope.TABLE,
                Optional.of(ActionTarget.ofTable("db", "t")));
        Assertions.assertEquals(1, tableDescs.size());
        Assertions.assertEquals("rewrite_data_files", tableDescs.get(0).name());

        List<ActionDescriptor> catalogDescs = ops.listActions(
                ConnectorActionOps.Scope.CATALOG, Optional.empty());
        Assertions.assertTrue(catalogDescs.isEmpty());

        ActionResult r = ops.executeAction(ActionInvocation.builder()
                .name("rewrite_data_files")
                .scope(ConnectorActionOps.Scope.TABLE)
                .target(ActionTarget.ofTable("db", "t"))
                .build());
        Assertions.assertEquals(ActionResult.Lifecycle.COMPLETED, r.lifecycle());
        Assertions.assertEquals(Arrays.asList(7L),
                new ArrayList<>(r.rows().get(0).values()));
    }
}
