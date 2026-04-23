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

package org.apache.doris.connector.api;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Verifies the M0-03 property entry points on {@link Connector}: defaults are
 * empty, the {@code Map} overload of {@code getCatalogProperties} delegates
 * to the no-arg form, and {@code getTableProperties(TableScope)} delegates
 * to the no-arg {@code getTableProperties()} for every scope.
 */
public class ConnectorPropertyEntryPointsTest {

    private static ConnectorPropertyMetadata<String> stringProp(String name) {
        return ConnectorPropertyMetadata.stringProperty(name, "", null);
    }

    @Test
    public void defaultsAreEmptyForAllNewEntryPoints() {
        Connector c = new Connector() {
            @Override
            public ConnectorMetadata getMetadata(ConnectorSession session) {
                return null;
            }
        };

        Assertions.assertTrue(c.getCatalogProperties().isEmpty());
        Assertions.assertTrue(c.getCatalogProperties(new HashMap<>()).isEmpty());
        Assertions.assertTrue(c.getCatalogProperties(Collections.emptyMap()).isEmpty());
        for (TableScope scope : TableScope.values()) {
            Assertions.assertTrue(c.getTableProperties(scope).isEmpty(),
                    "expected empty for scope " + scope);
        }
    }

    @Test
    public void mapOverloadDelegatesToNoArgCatalogProperties() {
        ConnectorPropertyMetadata<String> p = stringProp("warehouse");
        Connector c = new Connector() {
            @Override
            public ConnectorMetadata getMetadata(ConnectorSession session) {
                return null;
            }

            @Override
            public List<ConnectorPropertyMetadata<?>> getCatalogProperties() {
                return Collections.singletonList(p);
            }
        };

        List<ConnectorPropertyMetadata<?>> noArg = c.getCatalogProperties();
        List<ConnectorPropertyMetadata<?>> withMap = c.getCatalogProperties(new HashMap<>());
        Map<String, String> raw = new HashMap<>();
        raw.put("type", "hudi");
        List<ConnectorPropertyMetadata<?>> withRaw = c.getCatalogProperties(raw);

        Assertions.assertEquals(1, noArg.size());
        Assertions.assertSame(p, noArg.get(0));
        Assertions.assertEquals(noArg, withMap);
        Assertions.assertEquals(noArg, withRaw);
    }

    @Test
    public void tableScopeOverloadDelegatesToNoArgTableProperties() {
        ConnectorPropertyMetadata<String> p = stringProp("format");
        Connector c = new Connector() {
            @Override
            public ConnectorMetadata getMetadata(ConnectorSession session) {
                return null;
            }

            @Override
            public List<ConnectorPropertyMetadata<?>> getTableProperties() {
                return Collections.singletonList(p);
            }
        };

        List<ConnectorPropertyMetadata<?>> noArg = c.getTableProperties();
        Assertions.assertEquals(1, noArg.size());
        Assertions.assertSame(p, noArg.get(0));

        for (TableScope scope : TableScope.values()) {
            List<ConnectorPropertyMetadata<?>> scoped = c.getTableProperties(scope);
            Assertions.assertEquals(noArg, scoped, "scope " + scope + " should delegate to no-arg");
        }
    }

    @Test
    public void tableScopeEnumHasExpectedConstants() {
        Assertions.assertEquals(4, TableScope.values().length);
        Assertions.assertEquals("CREATE", TableScope.CREATE.name());
        Assertions.assertEquals("ALTER", TableScope.ALTER.name());
        Assertions.assertEquals("INSERT", TableScope.INSERT.name());
        Assertions.assertEquals("SHOW", TableScope.SHOW.name());
        Assertions.assertSame(TableScope.CREATE, TableScope.valueOf("CREATE"));
        Assertions.assertSame(TableScope.ALTER, TableScope.valueOf("ALTER"));
        Assertions.assertSame(TableScope.INSERT, TableScope.valueOf("INSERT"));
        Assertions.assertSame(TableScope.SHOW, TableScope.valueOf("SHOW"));
    }
}
