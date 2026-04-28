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

import org.apache.doris.connector.api.ConnectorTableId;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Branch/tag (ref) operations exposed by a connector.
 *
 * <p>Plugins should report only the {@link RefKind}s they actually support via
 * {@link #supportedRefKinds()}; engine is expected to reject mutations of
 * unsupported kinds upstream of the SPI call.</p>
 *
 * <p>Table identity is carried as a typed {@link ConnectorTableId}.</p>
 */
public interface RefOps {

    /** Set of ref kinds the connector understands. */
    Set<RefKind> supportedRefKinds();

    /** Lists the refs currently known for the given table. */
    List<ConnectorRef> listRefs(ConnectorTableId id);

    /** Creates or replaces a ref on the given table per the supplied mutation. */
    void createOrReplaceRef(ConnectorTableId id, ConnectorRefMutation mutation);

    /** Drops the named ref of the given kind on the given table. */
    void dropRef(ConnectorTableId id, String name, RefKind kind);

    /**
     * Returns the named ref of the given kind, or empty if absent.
     *
     * <p>Default implementation linearly scans {@link #listRefs}. Plugins MAY
     * override for a constant-time lookup. Connectors that do not support refs
     * at all SHOULD leave this default and rely on {@link #supportedRefKinds()}
     * returning empty.</p>
     */
    default Optional<ConnectorRef> getRef(ConnectorTableId id, String name, RefKind kind) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(kind, "kind");
        return listRefs(id).stream()
                .filter(r -> r.kind() == kind && name.equals(r.name()))
                .findFirst();
    }

    /**
     * Cherry-picks the given snapshot onto the current main branch.
     *
     * <p>Implementations MAY throw {@link UnsupportedOperationException} when
     * the underlying table format does not expose a cherrypick primitive. The
     * default implementation always throws to surface attempted mutations
     * loudly rather than silently dropping them.</p>
     */
    default void cherrypickSnapshot(ConnectorTableId id, long snapshotId) {
        throw new UnsupportedOperationException(
                "RefOps.cherrypickSnapshot not supported by this connector");
    }

    /**
     * Fast-forwards or rewinds {@code branch} to point at {@code snapshotId}.
     *
     * <p>Implementations MAY throw {@link UnsupportedOperationException} when
     * the underlying table format does not expose this primitive. The default
     * implementation always throws to surface attempted mutations loudly
     * rather than silently dropping them.</p>
     */
    default void replaceBranch(ConnectorTableId id, String branch, long snapshotId) {
        throw new UnsupportedOperationException(
                "RefOps.replaceBranch not supported by this connector");
    }
}
