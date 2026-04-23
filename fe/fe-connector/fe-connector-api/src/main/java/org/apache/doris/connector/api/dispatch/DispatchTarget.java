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

package org.apache.doris.connector.api.dispatch;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Result of {@link ConnectorDispatchOps#resolveTarget}: identifies the
 * delegate connector that should serve subsequent schema/scan/stats/MTMV/
 * policy/sys-table requests for a given table.
 *
 * <p>Immutable; constructed via {@link #builder(String)}.</p>
 */
public final class DispatchTarget {

    private final String connectorType;
    private final Optional<String> backendName;
    private final Map<String, String> normalizedProps;

    private DispatchTarget(String connectorType,
                           Optional<String> backendName,
                           Map<String, String> normalizedProps) {
        this.connectorType = connectorType;
        this.backendName = backendName;
        this.normalizedProps = normalizedProps;
    }

    /**
     * Connector type name (matches {@code ConnectorProvider.getType()} of the
     * delegate, e.g., {@code "iceberg"}). Never blank.
     */
    public String connectorType() {
        return connectorType;
    }

    /**
     * Optional backend name when the host wants to pin the delegate to a
     * specific backing service instance (per D2). Empty when unspecified.
     */
    public Optional<String> backendName() {
        return backendName;
    }

    /**
     * Normalized properties forwarded to the delegate connector. Immutable;
     * never {@code null}; may be empty.
     */
    public Map<String, String> normalizedProps() {
        return normalizedProps;
    }

    public static Builder builder(String connectorType) {
        return new Builder(connectorType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DispatchTarget)) {
            return false;
        }
        DispatchTarget that = (DispatchTarget) o;
        return connectorType.equals(that.connectorType)
                && backendName.equals(that.backendName)
                && normalizedProps.equals(that.normalizedProps);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectorType, backendName, normalizedProps);
    }

    @Override
    public String toString() {
        return "DispatchTarget{connectorType='" + connectorType + '\''
                + ", backendName=" + backendName
                + ", normalizedProps=" + normalizedProps + '}';
    }

    public static final class Builder {
        private final String connectorType;
        private String backendName;
        private Map<String, String> normalizedProps = new LinkedHashMap<>();

        private Builder(String connectorType) {
            Objects.requireNonNull(connectorType, "connectorType");
            if (connectorType.trim().isEmpty()) {
                throw new IllegalArgumentException("connectorType must not be blank");
            }
            this.connectorType = connectorType;
        }

        public Builder backendName(String backendName) {
            this.backendName = backendName;
            return this;
        }

        public Builder normalizedProps(Map<String, String> props) {
            Objects.requireNonNull(props, "normalizedProps");
            this.normalizedProps = new LinkedHashMap<>(props);
            return this;
        }

        public Builder putNormalizedProp(String key, String value) {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(value, "value");
            this.normalizedProps.put(key, value);
            return this;
        }

        public DispatchTarget build() {
            Optional<String> backend = (backendName == null) ? Optional.empty() : Optional.of(backendName);
            Map<String, String> props = Collections.unmodifiableMap(new LinkedHashMap<>(normalizedProps));
            return new DispatchTarget(connectorType, backend, props);
        }
    }
}
