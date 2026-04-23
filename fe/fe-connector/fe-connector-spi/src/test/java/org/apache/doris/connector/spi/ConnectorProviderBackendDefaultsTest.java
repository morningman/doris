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

package org.apache.doris.connector.spi;

import org.apache.doris.connector.api.Connector;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Verifies the M0-05 default behaviour of the new backend-dispatch hooks on
 * {@link ConnectorProvider}: {@link ConnectorProvider#getCatalogTypeProperty()}
 * and {@link ConnectorProvider#getSupportedBackends()}.
 */
public class ConnectorProviderBackendDefaultsTest {

    /** Minimal in-test impl that only sets the connector type. */
    private static final class TypeOnlyProvider implements ConnectorProvider {
        private final String type;

        TypeOnlyProvider(String type) {
            this.type = type;
        }

        @Override
        public String getType() {
            return type;
        }

        @Override
        public Connector create(Map<String, String> properties, ConnectorContext context) {
            throw new UnsupportedOperationException("not used in this test");
        }
    }

    /** In-test impl that overrides both backend-dispatch hooks. */
    private static final class MultiBackendProvider implements ConnectorProvider {
        private final String type;
        private final String catalogTypeProperty;
        private final Set<String> supportedBackends;

        MultiBackendProvider(String type, String catalogTypeProperty, Set<String> supportedBackends) {
            this.type = type;
            this.catalogTypeProperty = catalogTypeProperty;
            this.supportedBackends = supportedBackends;
        }

        @Override
        public String getType() {
            return type;
        }

        @Override
        public Optional<String> getCatalogTypeProperty() {
            return Optional.of(catalogTypeProperty);
        }

        @Override
        public Set<String> getSupportedBackends() {
            return supportedBackends;
        }

        @Override
        public Connector create(Map<String, String> properties, ConnectorContext context) {
            throw new UnsupportedOperationException("not used in this test");
        }
    }

    @Test
    public void catalogTypePropertyDefaultsToEmpty() {
        ConnectorProvider provider = new TypeOnlyProvider("jdbc");
        Assertions.assertEquals(Optional.empty(), provider.getCatalogTypeProperty());
    }

    @Test
    public void supportedBackendsDefaultsToSingletonOfType() {
        for (String type : new String[] {"jdbc", "es", "iceberg"}) {
            ConnectorProvider provider = new TypeOnlyProvider(type);
            Set<String> backends = provider.getSupportedBackends();
            Assertions.assertEquals(Collections.singleton(type), backends,
                    "default getSupportedBackends() should be singleton(getType()) for type=" + type);
            Assertions.assertEquals(1, backends.size());
            Assertions.assertTrue(backends.contains(type));
        }
    }

    @Test
    public void defaultSupportedBackendsEqualsCollectionsSingleton() {
        ConnectorProvider provider = new TypeOnlyProvider("hive");
        Assertions.assertEquals(Collections.singleton("hive"), provider.getSupportedBackends());
    }

    @Test
    public void overriddenHooksRoundTrip() {
        Set<String> backends = new LinkedHashSet<>();
        backends.add("hms");
        backends.add("rest");
        backends.add("glue");
        backends.add("dlf");
        backends.add("filesystem");

        ConnectorProvider provider = new MultiBackendProvider(
                "iceberg", "iceberg.catalog.type", backends);

        Assertions.assertEquals(Optional.of("iceberg.catalog.type"), provider.getCatalogTypeProperty());
        Assertions.assertEquals(new HashSet<>(backends), new HashSet<>(provider.getSupportedBackends()));
        Assertions.assertEquals(5, provider.getSupportedBackends().size());
        Assertions.assertTrue(provider.getSupportedBackends().contains("rest"));
    }
}
