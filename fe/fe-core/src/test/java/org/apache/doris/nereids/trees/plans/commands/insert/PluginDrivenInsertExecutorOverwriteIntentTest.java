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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.connector.api.write.WriteIntent;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Verifies that {@link PluginDrivenInsertExecutor#buildWriteIntent} translates
 * the nereids INSERT/INSERT OVERWRITE pipeline output into the correct
 * {@link WriteIntent.OverwriteMode} for plugin-driven external tables.
 */
public class PluginDrivenInsertExecutorOverwriteIntentTest {

    @Test
    public void plainInsertProducesNoneIntent() {
        PluginDrivenInsertCommandContext ctx = new PluginDrivenInsertCommandContext();
        ctx.setOverwrite(false);

        WriteIntent intent = PluginDrivenInsertExecutor.buildWriteIntent(Optional.of(ctx));

        Assertions.assertEquals(WriteIntent.OverwriteMode.NONE, intent.overwriteMode());
        Assertions.assertTrue(intent.staticPartitions().isEmpty());
    }

    @Test
    public void absentContextProducesSimpleIntent() {
        WriteIntent intent = PluginDrivenInsertExecutor.buildWriteIntent(Optional.empty());

        Assertions.assertSame(WriteIntent.simple(), intent);
    }

    @Test
    public void nonPluginContextProducesSimpleIntent() {
        InsertCommandContext other = new HiveInsertCommandContext();

        WriteIntent intent = PluginDrivenInsertExecutor.buildWriteIntent(Optional.of(other));

        Assertions.assertSame(WriteIntent.simple(), intent);
    }

    @Test
    public void overwriteTableWithoutPartitionProducesFullTable() {
        PluginDrivenInsertCommandContext ctx = new PluginDrivenInsertCommandContext();
        ctx.setOverwrite(true);

        WriteIntent intent = PluginDrivenInsertExecutor.buildWriteIntent(Optional.of(ctx));

        Assertions.assertEquals(WriteIntent.OverwriteMode.FULL_TABLE, intent.overwriteMode());
        Assertions.assertTrue(intent.staticPartitions().isEmpty());
    }

    @Test
    public void overwriteSingleStaticPartitionProducesStaticPartition() {
        PluginDrivenInsertCommandContext ctx = new PluginDrivenInsertCommandContext();
        ctx.setOverwrite(true);
        ctx.setStaticPartitionValues(ImmutableMap.of("dt", "2024-01-01"));

        WriteIntent intent = PluginDrivenInsertExecutor.buildWriteIntent(Optional.of(ctx));

        Assertions.assertEquals(WriteIntent.OverwriteMode.STATIC_PARTITION, intent.overwriteMode());
        Assertions.assertEquals(ImmutableMap.of("dt", "2024-01-01"), intent.staticPartitions());
    }

    @Test
    public void overwriteMultiKeyStaticPartitionPreservesOrderAndValues() {
        Map<String, String> spec = new LinkedHashMap<>();
        spec.put("region", "us");
        spec.put("dt", "2024-02-15");
        PluginDrivenInsertCommandContext ctx = new PluginDrivenInsertCommandContext();
        ctx.setOverwrite(true);
        ctx.setStaticPartitionValues(spec);

        WriteIntent intent = PluginDrivenInsertExecutor.buildWriteIntent(Optional.of(ctx));

        Assertions.assertEquals(WriteIntent.OverwriteMode.STATIC_PARTITION, intent.overwriteMode());
        Assertions.assertEquals(spec, intent.staticPartitions());
        Assertions.assertEquals(ImmutableList.of("region", "dt"),
                ImmutableList.copyOf(intent.staticPartitions().keySet()));
    }

    @Test
    public void overwriteDynamicPartitionByNameOnlyProducesDynamic() {
        PluginDrivenInsertCommandContext ctx = new PluginDrivenInsertCommandContext();
        ctx.setOverwrite(true);
        ctx.setDynamicPartitionNames(ImmutableList.of("dt"));

        WriteIntent intent = PluginDrivenInsertExecutor.buildWriteIntent(Optional.of(ctx));

        Assertions.assertEquals(WriteIntent.OverwriteMode.DYNAMIC_PARTITION, intent.overwriteMode());
        Assertions.assertTrue(intent.staticPartitions().isEmpty());
    }

    @Test
    public void overwriteAllDynamicMultiColumnsProducesDynamic() {
        PluginDrivenInsertCommandContext ctx = new PluginDrivenInsertCommandContext();
        ctx.setOverwrite(true);
        ctx.setDynamicPartitionNames(ImmutableList.of("region", "dt"));

        WriteIntent intent = PluginDrivenInsertExecutor.buildWriteIntent(Optional.of(ctx));

        Assertions.assertEquals(WriteIntent.OverwriteMode.DYNAMIC_PARTITION, intent.overwriteMode());
        Assertions.assertTrue(intent.staticPartitions().isEmpty());
    }

    @Test
    public void staticPartitionTakesPrecedenceOverDynamicNames() {
        // Defensive check on the helper's branch order: when a (malformed) context
        // carries both, STATIC_PARTITION wins because the WriteIntent invariant
        // forbids combining DYNAMIC_PARTITION with a non-empty staticPartitions map.
        PluginDrivenInsertCommandContext ctx = new PluginDrivenInsertCommandContext();
        ctx.setOverwrite(true);
        ctx.setStaticPartitionValues(ImmutableMap.of("dt", "2024-03-01"));
        ctx.setDynamicPartitionNames(ImmutableList.of("region"));

        WriteIntent intent = PluginDrivenInsertExecutor.buildWriteIntent(Optional.of(ctx));

        Assertions.assertEquals(WriteIntent.OverwriteMode.STATIC_PARTITION, intent.overwriteMode());
        Assertions.assertEquals(ImmutableMap.of("dt", "2024-03-01"), intent.staticPartitions());
    }
}
