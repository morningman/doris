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

package org.apache.doris.connector.api.timetravel;

import java.time.Instant;
import java.util.Optional;

/**
 * Connector-supplied MVCC snapshot pinning the visible state of a table for
 * the duration of a query.
 *
 * <p>Engine treats instances as opaque; serialization across FE→BE is done via
 * the plugin-provided {@link Codec}.</p>
 */
public interface ConnectorMvccSnapshot {

    /** Wall-clock commit time the snapshot represents. */
    Instant commitTime();

    /** Best-effort lossless conversion to a {@link ConnectorTableVersion}. */
    Optional<ConnectorTableVersion> asVersion();

    /** Plugin-readable opaque token; only meaningful to the same plugin. */
    String toOpaqueToken();

    /**
     * Plugin-provided codec used by the engine to serialize the snapshot
     * across the FE→BE plan boundary.
     */
    interface Codec {
        byte[] encode(ConnectorMvccSnapshot s);

        ConnectorMvccSnapshot decode(byte[] bytes);
    }
}
