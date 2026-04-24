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

package org.apache.doris.connector.iceberg.api;

import org.apache.hadoop.conf.Configuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Inputs supplied to {@link IcebergBackend#buildCatalog(IcebergBackendContext)}.
 *
 * <p>Carries everything a backend needs to instantiate the Iceberg SDK
 * {@link org.apache.iceberg.catalog.Catalog} for one Doris catalog: the
 * catalog name, the user-supplied properties, and a Hadoop
 * {@link Configuration} pre-populated by the orchestrator from
 * {@code hadoop.* / fs.* / dfs.* / hive.*} entries in the property map.
 */
public final class IcebergBackendContext {

    private final String catalogName;
    private final Map<String, String> properties;
    private final Configuration hadoopConf;

    public IcebergBackendContext(String catalogName,
                                 Map<String, String> properties,
                                 Configuration hadoopConf) {
        this.catalogName = Objects.requireNonNull(catalogName, "catalogName");
        this.properties = Collections.unmodifiableMap(
                new HashMap<>(Objects.requireNonNull(properties, "properties")));
        this.hadoopConf = Objects.requireNonNull(hadoopConf, "hadoopConf");
    }

    public String catalogName() {
        return catalogName;
    }

    public Map<String, String> properties() {
        return properties;
    }

    public Configuration hadoopConf() {
        return hadoopConf;
    }
}
