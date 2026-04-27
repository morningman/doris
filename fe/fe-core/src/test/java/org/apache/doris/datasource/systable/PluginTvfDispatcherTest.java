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

package org.apache.doris.datasource.systable;

import org.apache.doris.connector.api.systable.TvfInvocation;
import org.apache.doris.nereids.trees.expressions.functions.table.PartitionValues;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.tablefunction.PartitionValuesTableValuedFunction;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

class PluginTvfDispatcherTest {

    @Test
    void dispatchesPartitionValuesByName() {
        TvfInvocation inv = new TvfInvocation("partition_values", Collections.emptyMap());
        TableValuedFunction tvf = PluginTvfDispatcher.toTableValuedFunction("ctl", "db", "tbl", inv);
        Assertions.assertTrue(tvf instanceof PartitionValues,
                "partition_values must dispatch to PartitionValues TVF");
        Assertions.assertEquals("partition_values", tvf.getName());
    }

    @Test
    void dispatchedFunctionCarriesCatalogDbTableProps() {
        TvfInvocation inv = new TvfInvocation("partition_values", Collections.emptyMap());
        PartitionValues tvf = (PartitionValues)
                PluginTvfDispatcher.toTableValuedFunction("hive_ctl", "default", "orders", inv);
        Map<String, String> args = tvf.getTVFProperties().getMap();
        Assertions.assertEquals("hive_ctl", args.get(PartitionValuesTableValuedFunction.CATALOG));
        Assertions.assertEquals("default", args.get(PartitionValuesTableValuedFunction.DB));
        Assertions.assertEquals("orders", args.get(PartitionValuesTableValuedFunction.TABLE));
    }

    @Test
    void unknownFunctionNameRejected() {
        TvfInvocation inv = new TvfInvocation("not_a_tvf", Collections.emptyMap());
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> PluginTvfDispatcher.toTableValuedFunction("c", "d", "t", inv));
        Assertions.assertTrue(ex.getMessage().contains("not_a_tvf"));
    }

    @Test
    void caseInsensitiveOnFunctionName() {
        TvfInvocation upper = new TvfInvocation("PARTITION_VALUES", Collections.emptyMap());
        TableValuedFunction tvf = PluginTvfDispatcher.toTableValuedFunction("c", "d", "t", upper);
        Assertions.assertTrue(tvf instanceof PartitionValues);
    }

    @Test
    void invocationPropertiesIgnoredForPartitionValues() {
        // The plugin invocation MAY pass database / table properties; the
        // dispatcher must use the catalog/db/table arguments (engine truth)
        // not those properties (plugin self-report). Diverging values surface
        // the engine's view.
        Map<String, String> wrong = new LinkedHashMap<>();
        wrong.put("database", "wrong_db");
        wrong.put("table", "wrong_tbl");
        TvfInvocation inv = new TvfInvocation("partition_values", wrong);
        PartitionValues tvf = (PartitionValues)
                PluginTvfDispatcher.toTableValuedFunction("ctl", "engine_db", "engine_tbl", inv);
        Map<String, String> args = tvf.getTVFProperties().getMap();
        Assertions.assertEquals("engine_db", args.get(PartitionValuesTableValuedFunction.DB));
        Assertions.assertEquals("engine_tbl", args.get(PartitionValuesTableValuedFunction.TABLE));
    }

    @Test
    void rejectsNullArgs() {
        TvfInvocation inv = new TvfInvocation("partition_values", Collections.emptyMap());
        Assertions.assertThrows(NullPointerException.class,
                () -> PluginTvfDispatcher.toTableValuedFunction(null, "d", "t", inv));
        Assertions.assertThrows(NullPointerException.class,
                () -> PluginTvfDispatcher.toTableValuedFunction("c", null, "t", inv));
        Assertions.assertThrows(NullPointerException.class,
                () -> PluginTvfDispatcher.toTableValuedFunction("c", "d", null, inv));
        Assertions.assertThrows(NullPointerException.class,
                () -> PluginTvfDispatcher.toTableValuedFunction("c", "d", "t", null));
    }
}
