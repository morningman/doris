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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * SPI entry point for the Paimon connector.
 *
 * <p>Registered via {@code META-INF/services/org.apache.doris.connector.spi.ConnectorProvider}.
 * Returns type {@code "paimon"} matching the CatalogFactory dispatch key.
 * Internally dispatches to all Paimon catalog backends (filesystem, hms,
 * rest, aliyun-dlf) via the ServiceLoader-driven
 * {@link PaimonBackendRegistry}.
 */
public class PaimonConnectorProvider implements ConnectorProvider {

    @Override
    public String getType() {
        return "paimon";
    }

    @Override
    public Optional<String> getCatalogTypeProperty() {
        return Optional.of(PaimonConnectorProperties.PAIMON_CATALOG_TYPE);
    }

    @Override
    public Set<String> getSupportedBackends() {
        return Set.of(
                PaimonConnectorProperties.TYPE_FILESYSTEM,
                PaimonConnectorProperties.TYPE_HMS,
                PaimonConnectorProperties.TYPE_REST,
                PaimonConnectorProperties.TYPE_ALIYUN_DLF);
    }

    @Override
    public Connector create(Map<String, String> properties, ConnectorContext context) {
        return new PaimonConnector(properties, context);
    }
}
