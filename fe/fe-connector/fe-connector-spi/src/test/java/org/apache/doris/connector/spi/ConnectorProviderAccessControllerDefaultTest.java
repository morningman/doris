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

import java.util.Map;
import java.util.Optional;

/**
 * Verifies the M0-17 D8 spi-side default behaviour of
 * {@link ConnectorProvider#defaultAccessControllerFactoryName()}.
 */
public class ConnectorProviderAccessControllerDefaultTest {

    private static final class TypeOnlyProvider implements ConnectorProvider {
        @Override
        public String getType() {
            return "test";
        }

        @Override
        public Connector create(Map<String, String> properties, ConnectorContext context) {
            throw new UnsupportedOperationException("not used in this test");
        }
    }

    private static final class OverridingProvider implements ConnectorProvider {
        @Override
        public String getType() {
            return "iceberg";
        }

        @Override
        public Connector create(Map<String, String> properties, ConnectorContext context) {
            throw new UnsupportedOperationException("not used in this test");
        }

        @Override
        public Optional<String> defaultAccessControllerFactoryName() {
            return Optional.of("ranger-hive");
        }
    }

    @Test
    public void defaultIsEmpty() {
        ConnectorProvider p = new TypeOnlyProvider();
        Assertions.assertEquals(Optional.empty(), p.defaultAccessControllerFactoryName());
    }

    @Test
    public void overrideIsRespected() {
        ConnectorProvider p = new OverridingProvider();
        Assertions.assertEquals(Optional.of("ranger-hive"), p.defaultAccessControllerFactoryName());
    }
}
