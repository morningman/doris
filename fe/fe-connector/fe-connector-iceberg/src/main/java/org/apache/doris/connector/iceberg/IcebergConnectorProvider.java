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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.timetravel.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * SPI entry point for the Iceberg connector plugin.
 *
 * <p>Registered via {@code META-INF/services/org.apache.doris.connector.spi.ConnectorProvider}.
 * The type is {@code "iceberg"} to match the existing catalog type in CatalogFactory.
 * Internally dispatches to all Iceberg catalog backends (REST, HMS, Glue, DLF,
 * JDBC, Hadoop, S3Tables) via the Iceberg SDK's {@code CatalogUtil}.</p>
 */
public class IcebergConnectorProvider implements ConnectorProvider {

    @Override
    public String getType() {
        return "iceberg";
    }

    @Override
    public Optional<String> getCatalogTypeProperty() {
        return Optional.of(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE);
    }

    @Override
    public Set<String> getSupportedBackends() {
        return Set.of(
                IcebergConnectorProperties.TYPE_HMS,
                IcebergConnectorProperties.TYPE_REST,
                IcebergConnectorProperties.TYPE_GLUE,
                IcebergConnectorProperties.TYPE_DLF,
                IcebergConnectorProperties.TYPE_S3_TABLES,
                IcebergConnectorProperties.TYPE_HADOOP);
    }

    @Override
    public Connector create(Map<String, String> properties, ConnectorContext context) {
        return new IcebergConnector(properties, context);
    }

    /**
     * Iceberg catalogs reuse the Hive Ranger plugin for authorization, so we
     * default to {@code ranger-hive} (D8 §11.4). Users can override per-catalog
     * via {@code access_controller.class}.
     */
    @Override
    public Optional<String> defaultAccessControllerFactoryName() {
        return Optional.of("ranger-hive");
    }

    /**
     * Iceberg supports MVCC snapshot pinning; the codec round-trips an
     * {@link IcebergConnectorMvccSnapshot} via a fixed 24-byte binary
     * frame (see {@link IcebergConnectorMvccSnapshot.Codec}). The engine
     * uses this codec to ferry the snapshot across FE→BE and to persist
     * MTMV refresh metadata.
     */
    @Override
    public Optional<ConnectorMvccSnapshot.Codec> getMvccSnapshotCodec() {
        return Optional.of(new IcebergConnectorMvccSnapshot.Codec());
    }
}
