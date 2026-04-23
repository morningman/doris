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
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ConnectorRegistryTest {

    private static final class StubConnector implements Connector {
        private final Set<ConnectorCapability> caps;

        StubConnector(Set<ConnectorCapability> caps) {
            this.caps = caps;
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return null;
        }

        @Override
        public Set<ConnectorCapability> getCapabilities() {
            return caps;
        }
    }

    private static ConnectorRegistry newRegistry(Map<String, Connector> backing) {
        return new ConnectorRegistry() {
            @Override
            public Optional<Connector> lookup(String type, ConnectorContext ctx) {
                return Optional.ofNullable(backing.get(type));
            }

            @Override
            public Set<String> listLoaded() {
                return Collections.unmodifiableSet(backing.keySet());
            }

            @Override
            public boolean supports(String type, ConnectorCapability cap) {
                Connector c = backing.get(type);
                return c != null && c.getCapabilities().contains(cap);
            }
        };
    }

    @Test
    public void roundTrip() {
        Map<String, Connector> backing = new HashMap<>();
        backing.put("hive", new StubConnector(EnumSet.of(ConnectorCapability.EMITS_DELEGATABLE_TABLES)));
        backing.put("iceberg", new StubConnector(EnumSet.of(ConnectorCapability.ACCEPTS_DELEGATION_FROM_HMS)));

        ConnectorRegistry reg = newRegistry(backing);

        Assertions.assertTrue(reg.lookup("hive", null).isPresent());
        Assertions.assertTrue(reg.lookup("iceberg", null).isPresent());
        Assertions.assertTrue(reg.lookup("missing", null).isEmpty());

        Assertions.assertEquals(2, reg.listLoaded().size());
        Assertions.assertTrue(reg.listLoaded().contains("hive"));

        Assertions.assertTrue(reg.supports("hive", ConnectorCapability.EMITS_DELEGATABLE_TABLES));
        Assertions.assertFalse(reg.supports("hive", ConnectorCapability.ACCEPTS_DELEGATION_FROM_HMS));
        Assertions.assertTrue(reg.supports("iceberg", ConnectorCapability.ACCEPTS_DELEGATION_FROM_HMS));
        Assertions.assertFalse(reg.supports("missing", ConnectorCapability.EMITS_DELEGATABLE_TABLES));
    }
}
