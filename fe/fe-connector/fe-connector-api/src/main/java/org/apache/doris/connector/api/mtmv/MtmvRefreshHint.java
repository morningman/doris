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

package org.apache.doris.connector.api.mtmv;

import java.util.Objects;
import java.util.Optional;

/**
 * Hint passed to {@link MtmvOps#getPartitionSnapshot} / {@link MtmvOps#getTableSnapshot}
 * describing the refresh intent. Connectors may ignore everything except
 * {@link RefreshMode} when computing a snapshot marker.
 *
 * @param mode           Refresh mode requested by the engine.
 * @param partitionScope Optional engine-side partition scope identifier (e.g. partition
 *                       name) that the connector may use to short-circuit the lookup.
 */
public record MtmvRefreshHint(RefreshMode mode, Optional<String> partitionScope) {

    public MtmvRefreshHint {
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(partitionScope, "partitionScope");
    }

    /** Convenience: refresh hint with no partition scope. */
    public static MtmvRefreshHint of(RefreshMode mode) {
        return new MtmvRefreshHint(mode, Optional.empty());
    }

    /** Refresh mode requested by the engine. */
    public enum RefreshMode {
        /** Force full reload of the snapshot regardless of staleness. */
        FORCE_FULL,
        /** Allow connector to return cached / incremental snapshot when valid. */
        INCREMENTAL_AUTO,
        /** Refresh triggered by an explicit user-issued {@code REFRESH MATERIALIZED VIEW}. */
        ON_DEMAND
    }
}
