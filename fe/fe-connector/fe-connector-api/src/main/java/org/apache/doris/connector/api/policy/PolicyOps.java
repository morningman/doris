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

package org.apache.doris.connector.api.policy;

import org.apache.doris.connector.api.ConnectorTableId;
import org.apache.doris.connector.api.credential.UserContext;

import java.util.Optional;

/**
 * D8 — Governance policy hint surface.
 *
 * <p>Strictly hint-only: this interface never replaces the engine's
 * authoritative {@code AccessControllerFactory} / row-filter / column-mask
 * pipelines. Connectors use it to (a) opt out of row-filter pushdown when
 * the plugin cannot safely express the filter at the source, and
 * (b) volunteer per-column masking suggestions that may be honoured by the
 * engine after its own access-control decision.</p>
 *
 * <p>Table identity is carried as a typed {@link ConnectorTableId}.
 * Connectors must declare {@code ConnectorCapability.SUPPORTS_RLS_HINT}
 * and/or {@code SUPPORTS_MASK_HINT} for the engine to honour the
 * implementation.</p>
 */
public interface PolicyOps {

    /**
     * Whether the connector can safely accept a row-level filter pushed down
     * by the engine for this table in the given context. Defaults to
     * {@code true}; plugins return {@code false} only when pushdown would
     * change semantics (e.g. plugin-side virtual columns).
     */
    default boolean supportsRlsAt(ConnectorTableId id, ConnectorPolicyContext ctx) {
        return true;
    }

    /**
     * Returns the plugin's mask hint for the given column, evaluated for the
     * given user. Implementations should return {@link Optional#empty()}
     * when the plugin has no opinion — the engine's own mask policies still
     * apply.
     */
    Optional<ColumnMaskHint> hintForColumn(
            ConnectorTableId id, String column, UserContext user);

    /**
     * Notification that an engine-managed policy attached to one of the
     * connector's tables has changed upstream. Default no-op; connectors with
     * plugin-side caches override this to invalidate.
     */
    default void onPolicyChanged(PolicyChangeNotification n) {
    }
}
