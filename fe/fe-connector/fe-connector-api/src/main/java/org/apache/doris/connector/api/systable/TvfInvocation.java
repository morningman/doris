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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Opaque descriptor of a fe-core-side metadata TVF invocation.
 *
 * <p>Produced by {@link TvfSysTableInvoker#resolve} to tell fe-core which
 * built-in metadata TVF should serve a sys table, and what argument map to
 * apply. The engine is responsible for translating this into the actual
 * fe-core TVF relation; the SPI carries only the function name plus a
 * string property bag so that the api module stays free of engine types.</p>
 */
public final class TvfInvocation {

    private final String functionName;
    private final Map<String, String> properties;

    public TvfInvocation(String functionName, Map<String, String> properties) {
        this.functionName = Objects.requireNonNull(functionName, "functionName");
        if (functionName.isEmpty()) {
            throw new IllegalArgumentException("functionName must not be empty");
        }
        this.properties = (properties == null || properties.isEmpty())
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }

    /** The fe-core metadata TVF name (e.g. {@code "iceberg_meta"}, {@code "partitions"}). */
    public String functionName() {
        return functionName;
    }

    /**
     * Additional TVF arguments, preserved in insertion order. The keys /
     * values are plugin-defined strings that the engine forwards verbatim to
     * the TVF implementation; no key is reserved by the SPI.
     */
    public Map<String, String> properties() {
        return properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TvfInvocation)) {
            return false;
        }
        TvfInvocation that = (TvfInvocation) o;
        return functionName.equals(that.functionName) && properties.equals(that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(functionName, properties);
    }

    @Override
    public String toString() {
        return "TvfInvocation{" + functionName + ", properties=" + properties + "}";
    }
}
