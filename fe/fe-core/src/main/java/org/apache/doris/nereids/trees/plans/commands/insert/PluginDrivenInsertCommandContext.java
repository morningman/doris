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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Insert command context for plugin-driven connector catalogs.
 *
 * <p>Carries the overwrite flag (inherited from {@link BaseExternalTableInsertCommandContext})
 * along with the partition specification parsed by the nereids INSERT OVERWRITE pipeline:</p>
 * <ul>
 *   <li>{@code staticPartitionValues} — literal-only key/value pairs from
 *       {@code PARTITION (k='v', ...)}; populated when overwrite is fully static.</li>
 *   <li>{@code dynamicPartitionNames} — partition column names from
 *       {@code PARTITION (p1, p2)} (no values); populated when overwrite targets
 *       runtime-determined partitions.</li>
 * </ul>
 *
 * <p>Both collections are empty for plain {@code INSERT INTO} and for
 * {@code INSERT OVERWRITE TABLE} without a {@code PARTITION} clause.</p>
 */
public class PluginDrivenInsertCommandContext extends BaseExternalTableInsertCommandContext {

    private Map<String, String> staticPartitionValues = ImmutableMap.of();
    private List<String> dynamicPartitionNames = ImmutableList.of();

    public Map<String, String> getStaticPartitionValues() {
        return staticPartitionValues;
    }

    public void setStaticPartitionValues(Map<String, String> staticPartitionValues) {
        Objects.requireNonNull(staticPartitionValues, "staticPartitionValues");
        this.staticPartitionValues = ImmutableMap.copyOf(staticPartitionValues);
    }

    public List<String> getDynamicPartitionNames() {
        return dynamicPartitionNames;
    }

    public void setDynamicPartitionNames(List<String> dynamicPartitionNames) {
        Objects.requireNonNull(dynamicPartitionNames, "dynamicPartitionNames");
        this.dynamicPartitionNames = ImmutableList.copyOf(dynamicPartitionNames);
    }
}
