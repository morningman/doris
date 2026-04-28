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

package org.apache.doris.connector.cache;

import org.apache.doris.connector.api.ConnectorTableId;
import org.apache.doris.connector.api.cache.ConnectorMetaCacheBinding;
import org.apache.doris.connector.api.cache.ConnectorMetaCacheInvalidation;
import org.apache.doris.connector.api.cache.InvalidateRequest;
import org.apache.doris.connector.api.cache.InvalidateScope;
import org.apache.doris.connector.api.cache.MetaCacheHandle;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that all 5 {@link InvalidateScope} values flow through
 * {@link ConnectorMetaCacheRegistry#invalidate(InvalidateRequest)} and reach
 * the bindings whose {@link ConnectorMetaCacheInvalidation} predicate matches.
 *
 * <p>Each test wires two bindings against the registry: one with an
 * {@link ConnectorMetaCacheInvalidation#always() always} strategy (should be
 * cleared) and one whose predicate explicitly excludes the scope under test
 * (should NOT be cleared).</p>
 */
public class MetaCacheRegistryInvalidateScopeTest {

    private ConnectorMetaCacheRegistry registry;
    private MetaCacheHandle<String, String> alwaysHandle;
    private MetaCacheHandle<String, String> neverHandle;

    @BeforeEach
    public void setUp() {
        registry = new ConnectorMetaCacheRegistry("cat-scope");
        ConnectorMetaCacheBinding<String, String> always = ConnectorMetaCacheBinding
                .builder("always", String.class, String.class, k -> "v-" + k)
                .invalidationStrategy(ConnectorMetaCacheInvalidation.always())
                .build();
        ConnectorMetaCacheBinding<String, String> never = ConnectorMetaCacheBinding
                .builder("never", String.class, String.class, k -> "v-" + k)
                .invalidationStrategy(req -> false)
                .build();
        alwaysHandle = registry.bind(always);
        neverHandle = registry.bind(never);
        alwaysHandle.put("k", "v");
        neverHandle.put("k", "v");
    }

    private void assertScopeBehaviour(InvalidateRequest req, InvalidateScope expectedScope) {
        Assertions.assertEquals(expectedScope, req.getScope(),
                "test data sanity: request must carry expected scope");
        Assertions.assertTrue(alwaysHandle.getIfPresent("k").isPresent());
        Assertions.assertTrue(neverHandle.getIfPresent("k").isPresent());

        registry.invalidate(req);

        Assertions.assertFalse(alwaysHandle.getIfPresent("k").isPresent(),
                "always-binding should be invalidated by " + expectedScope);
        Assertions.assertTrue(neverHandle.getIfPresent("k").isPresent(),
                "never-binding should NOT be invalidated by " + expectedScope);
    }

    @Test
    public void catalogScopeAppliesToAlwaysButNotNever() {
        assertScopeBehaviour(InvalidateRequest.ofCatalog(), InvalidateScope.CATALOG);
    }

    @Test
    public void databaseScopeAppliesToAlwaysButNotNever() {
        assertScopeBehaviour(InvalidateRequest.ofDatabase("db1"), InvalidateScope.DATABASE);
    }

    @Test
    public void tableScopeAppliesToAlwaysButNotNever() {
        assertScopeBehaviour(InvalidateRequest.ofTable(ConnectorTableId.of("db1", "t1")), InvalidateScope.TABLE);
    }

    @Test
    public void partitionsScopeAppliesToAlwaysButNotNever() {
        assertScopeBehaviour(
                InvalidateRequest.ofPartitions(ConnectorTableId.of("db1", "t1"), java.util.List.of("p=1")),
                InvalidateScope.PARTITIONS);
    }

    @Test
    public void sysTableScopeAppliesToAlwaysButNotNever() {
        assertScopeBehaviour(
                InvalidateRequest.ofSysTable(ConnectorTableId.of("db1", "t1"), "$snapshots"),
                InvalidateScope.SYS_TABLE);
    }

    @Test
    public void byScopeStrategyIsolatesOneScope() {
        ConnectorMetaCacheRegistry r = new ConnectorMetaCacheRegistry("cat-isolate");
        ConnectorMetaCacheBinding<String, String> tableOnly = ConnectorMetaCacheBinding
                .builder("table-only", String.class, String.class, k -> "v")
                .invalidationStrategy(ConnectorMetaCacheInvalidation.byScope(InvalidateScope.TABLE))
                .build();
        MetaCacheHandle<String, String> h = r.bind(tableOnly);
        h.put("k", "v");

        // database-scope: should NOT clear
        r.invalidate(InvalidateRequest.ofDatabase("db1"));
        Assertions.assertTrue(h.getIfPresent("k").isPresent());

        // partitions-scope: should NOT clear (different scope)
        r.invalidate(InvalidateRequest.ofPartitions(ConnectorTableId.of("db1", "t1"), java.util.List.of()));
        Assertions.assertTrue(h.getIfPresent("k").isPresent());

        // table-scope: SHOULD clear
        r.invalidate(InvalidateRequest.ofTable(ConnectorTableId.of("db1", "t1")));
        Assertions.assertFalse(h.getIfPresent("k").isPresent());
    }

    @Test
    public void invalidateRejectsNullRequest() {
        Assertions.assertThrows(NullPointerException.class, () -> registry.invalidate(null));
    }
}
