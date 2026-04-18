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

package org.apache.doris.connector.api.scan;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Encapsulates scan-node-level properties along with filter pushdown metadata.
 *
 * <p>Connectors that perform fine-grained conjunct pushdown (e.g., ES query DSL
 * building) return this from {@link ConnectorScanPlanProvider#getScanNodePropertiesResult}
 * to communicate both the scan properties and which conjuncts were NOT pushed down.</p>
 *
 * <p>The {@code notPushedConjunctIndices} set contains 0-based indices into the
 * AND children of the filter expression, in the same order as the conjuncts list.
 * Conjuncts whose indices are NOT in this set were successfully pushed down and
 * will be pruned from the scan node's conjunct list by the engine.</p>
 */
public class ScanNodePropertiesResult {

    private final Map<String, String> properties;
    private final Set<Integer> notPushedConjunctIndices;

    /**
     * Creates a result where all conjuncts are assumed pushed (or no fine-grained tracking).
     */
    public ScanNodePropertiesResult(Map<String, String> properties) {
        this(properties, Collections.emptySet());
    }

    /**
     * Creates a result with explicit not-pushed conjunct tracking.
     *
     * @param properties              scan-node-level properties
     * @param notPushedConjunctIndices indices of conjuncts that were NOT pushed down
     */
    public ScanNodePropertiesResult(Map<String, String> properties,
            Set<Integer> notPushedConjunctIndices) {
        this.properties = properties;
        this.notPushedConjunctIndices = notPushedConjunctIndices;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public Set<Integer> getNotPushedConjunctIndices() {
        return notPushedConjunctIndices;
    }

    public boolean hasNotPushedConjuncts() {
        return !notPushedConjunctIndices.isEmpty();
    }
}
