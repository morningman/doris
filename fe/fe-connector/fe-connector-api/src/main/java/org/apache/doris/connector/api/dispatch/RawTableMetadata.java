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

/**
 * Already-fetched raw table metadata that a host connector exposes to
 * {@link ConnectorDispatchOps#resolveTarget} so the dispatch decision
 * can be made WITHOUT issuing additional metastore RPCs.
 *
 * <p>Implementations must be effectively immutable. Use
 * {@link #of(Map, String, String, Map)} for the standard immutable view.</p>
 */
public interface RawTableMetadata {

    /** Hive/HMS-style table-level parameters. Never {@code null}. */
    Map<String, String> tableParameters();

    /** Hive {@code InputFormat} class name (or empty string when unknown). */
    String inputFormat();

    /** Storage location URI (or empty string when unknown). */
    String storageLocation();

    /** Hive SerDe parameters. Never {@code null}. */
    Map<String, String> serdeParameters();

    /**
     * Returns an immutable {@link RawTableMetadata} backed by defensive copies
     * of the input maps.
     *
     * @throws NullPointerException if any argument is {@code null}
     */
    static RawTableMetadata of(Map<String, String> tableParams,
                               String inputFormat,
                               String storageLocation,
                               Map<String, String> serdeParams) {
        Objects.requireNonNull(tableParams, "tableParams");
        Objects.requireNonNull(inputFormat, "inputFormat");
        Objects.requireNonNull(storageLocation, "storageLocation");
        Objects.requireNonNull(serdeParams, "serdeParams");
        Map<String, String> tp = Collections.unmodifiableMap(new LinkedHashMap<>(tableParams));
        Map<String, String> sp = Collections.unmodifiableMap(new LinkedHashMap<>(serdeParams));
        return new RawTableMetadataImpl(tp, inputFormat, storageLocation, sp);
    }

    /** Private immutable record-backed implementation. */
    record RawTableMetadataImpl(Map<String, String> tableParameters,
                                String inputFormat,
                                String storageLocation,
                                Map<String, String> serdeParameters) implements RawTableMetadata {
    }
}
