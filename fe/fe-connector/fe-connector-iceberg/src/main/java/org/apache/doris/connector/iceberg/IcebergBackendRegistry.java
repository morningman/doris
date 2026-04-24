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

import org.apache.doris.connector.iceberg.api.IcebergBackendFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * ServiceLoader-backed registry of {@link IcebergBackendFactory} instances.
 *
 * <p>Discovery is performed once on first access using the
 * {@link Thread#getContextClassLoader() thread context classloader}, which
 * inside the FE plugin runtime resolves to the iceberg plugin's classloader.
 * That classloader sees this orchestrator jar plus every backend jar shipped
 * in the plugin's {@code lib/} directory (one per
 * {@code fe-connector-iceberg-backend-<name>} module), so all installed
 * backends are visible without any static {@code if (type.equals(...))}
 * dispatch in the orchestrator.
 */
public final class IcebergBackendRegistry {

    private static final class Holder {
        private static final Map<String, IcebergBackendFactory> FACTORIES = load();

        private static Map<String, IcebergBackendFactory> load() {
            Map<String, IcebergBackendFactory> map = new LinkedHashMap<>();
            ServiceLoader<IcebergBackendFactory> loader =
                    ServiceLoader.load(IcebergBackendFactory.class);
            for (IcebergBackendFactory f : loader) {
                map.put(f.name().toLowerCase(Locale.ROOT), f);
            }
            return Collections.unmodifiableMap(map);
        }
    }

    private IcebergBackendRegistry() {
    }

    public static Optional<IcebergBackendFactory> get(String backendType) {
        return Optional.ofNullable(
                Holder.FACTORIES.get(backendType.toLowerCase(Locale.ROOT)));
    }

    public static Set<String> availableTypes() {
        return Holder.FACTORIES.keySet();
    }
}
