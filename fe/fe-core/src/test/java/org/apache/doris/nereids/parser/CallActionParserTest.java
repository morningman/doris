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

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.trees.plans.commands.PluginDrivenExecuteActionCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Parser tests for the catalog-qualified CALL syntax dispatched to
 * {@link PluginDrivenExecuteActionCommand}.
 */
public class CallActionParserTest {

    private final NereidsParser parser = new NereidsParser();

    @Test
    public void testZeroArgCall() {
        LogicalPlan plan = parser.parseSingle("CALL ctl.db.tbl.act()");
        Assertions.assertInstanceOf(PluginDrivenExecuteActionCommand.class, plan);
        PluginDrivenExecuteActionCommand cmd = (PluginDrivenExecuteActionCommand) plan;
        Assertions.assertEquals("ctl", cmd.getTableNameInfo().getCtl());
        Assertions.assertEquals("db", cmd.getTableNameInfo().getDb());
        Assertions.assertEquals("tbl", cmd.getTableNameInfo().getTbl());
        Assertions.assertEquals("act", cmd.getActionName());
        Assertions.assertTrue(cmd.getPositionalArguments().isEmpty());
        Assertions.assertTrue(cmd.getNamedArguments().isEmpty());
    }

    @Test
    public void testPositionalArgs() {
        LogicalPlan plan = parser.parseSingle("CALL ctl.db.tbl.act(1, 'x')");
        Assertions.assertInstanceOf(PluginDrivenExecuteActionCommand.class, plan);
        PluginDrivenExecuteActionCommand cmd = (PluginDrivenExecuteActionCommand) plan;
        Assertions.assertEquals(2, cmd.getPositionalArguments().size());
        Assertions.assertTrue(cmd.getNamedArguments().isEmpty());
    }

    @Test
    public void testNamedArgs() {
        LogicalPlan plan = parser.parseSingle(
                "CALL ctl.db.tbl.act(name => 'x', count => 5)");
        Assertions.assertInstanceOf(PluginDrivenExecuteActionCommand.class, plan);
        PluginDrivenExecuteActionCommand cmd = (PluginDrivenExecuteActionCommand) plan;
        Assertions.assertTrue(cmd.getPositionalArguments().isEmpty());
        Assertions.assertEquals(2, cmd.getNamedArguments().size());
        Assertions.assertTrue(cmd.getNamedArguments().containsKey("name"));
        Assertions.assertTrue(cmd.getNamedArguments().containsKey("count"));
    }

    @Test
    public void testMixedArgs() {
        LogicalPlan plan = parser.parseSingle("CALL ctl.db.tbl.act(1, name => 'x')");
        Assertions.assertInstanceOf(PluginDrivenExecuteActionCommand.class, plan);
        PluginDrivenExecuteActionCommand cmd = (PluginDrivenExecuteActionCommand) plan;
        Assertions.assertEquals(1, cmd.getPositionalArguments().size());
        Assertions.assertEquals(1, cmd.getNamedArguments().size());
    }

    @Test
    public void testDuplicateNamedArgRejected() {
        Assertions.assertThrows(ParseException.class,
                () -> parser.parseSingle("CALL ctl.db.tbl.act(name => 'x', NAME => 'y')"));
    }

    @Test
    public void testPositionalAfterNamedRejected() {
        Assertions.assertThrows(ParseException.class,
                () -> parser.parseSingle("CALL ctl.db.tbl.act(name => 'x', 5)"));
    }

    @Test
    public void testNamedArgRejectedForStoredProcedure() {
        // Named args are only supported for 4-part catalog-qualified calls.
        // The check fires in the visitor before any context-dependent analysis.
        Assertions.assertThrows(ParseException.class,
                () -> parser.parseSingle("CALL db.proc(x => 1)"));
    }
}
