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

package org.apache.doris.connector.api.systable;

import java.util.Objects;
import java.util.UUID;

/**
 * Opaque handle referencing a {@code ConnectorScanPlanProvider} that the
 * plugin owns and will resolve itself at plan time.
 *
 * <p>Used from {@link SysTableSpec#scanPlanProviderRef()} when
 * {@link SysTableExecutionMode#CONNECTOR_SCAN_PLAN} is selected. The engine
 * treats the identifier as a pure string token — it never interprets or
 * parses its contents. The plugin is responsible for registering the
 * provider under this identifier and for resolving it when the engine
 * asks for the scan plan.</p>
 *
 * <p>Intentionally does NOT reference any scan-request types that do not
 * yet exist in {@code fe-connector-api}; if a richer binding is needed in
 * the future, additional fields can be appended in a backwards-compatible
 * way.</p>
 */
public final class ConnectorScanPlanProviderRef {

    private final String id;

    private ConnectorScanPlanProviderRef(String id) {
        this.id = Objects.requireNonNull(id, "id");
        if (id.isEmpty()) {
            throw new IllegalArgumentException("id must not be empty");
        }
    }

    /** Wraps an arbitrary plugin-chosen identifier (e.g. a catalog-local key). */
    public static ConnectorScanPlanProviderRef of(String id) {
        return new ConnectorScanPlanProviderRef(id);
    }

    /** Wraps a {@link UUID}-based identifier. */
    public static ConnectorScanPlanProviderRef of(UUID uuid) {
        return new ConnectorScanPlanProviderRef(Objects.requireNonNull(uuid, "uuid").toString());
    }

    /** The opaque identifier string. Never {@code null}, never empty. */
    public String id() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorScanPlanProviderRef)) {
            return false;
        }
        ConnectorScanPlanProviderRef that = (ConnectorScanPlanProviderRef) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return "ConnectorScanPlanProviderRef{" + id + "}";
    }
}
