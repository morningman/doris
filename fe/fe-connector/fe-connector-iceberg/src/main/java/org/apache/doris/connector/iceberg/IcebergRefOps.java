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

import org.apache.doris.connector.api.ConnectorTableId;
import org.apache.doris.connector.api.timetravel.ConnectorRef;
import org.apache.doris.connector.api.timetravel.ConnectorRefMutation;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;
import org.apache.doris.connector.api.timetravel.RefKind;
import org.apache.doris.connector.api.timetravel.RefOps;
import org.apache.doris.connector.iceberg.api.IcebergBackend;
import org.apache.doris.connector.iceberg.api.IcebergBackendContext;

import org.apache.iceberg.Snapshot;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Iceberg-specific {@link RefOps} wiring. Delegates read-side operations
 * ({@link #supportedRefKinds()}, {@link #listRefs(ConnectorTableId)}) to the
 * selected {@link IcebergBackend} (routed via
 * {@link IcebergBackendRegistry}). Mutation-side methods
 * ({@link #createOrReplaceRef}, {@link #dropRef}) are intentionally left as
 * {@link UnsupportedOperationException} in M1-07 — wiring them through
 * {@code Table.manageSnapshots()} is tracked separately.
 *
 * <p>Additionally exposes {@link #resolveVersion(String, String, ConnectorTableVersion)}
 * — an iceberg-specific extension used by the scan side to translate a
 * D5 {@link ConnectorTableVersion} into a concrete Iceberg {@link Snapshot}
 * pin. The method returns a {@link ConnectorTableVersion.BySnapshotId}
 * carrying the resolved snapshot id so callers stay on the api-shared
 * sealed hierarchy.
 */
public final class IcebergRefOps implements RefOps {

    private static final Set<RefKind> SUPPORTED =
            Set.of(RefKind.BRANCH, RefKind.TAG);

    private final IcebergBackend backend;
    private final IcebergBackendContext context;

    public IcebergRefOps(IcebergBackend backend, IcebergBackendContext context) {
        this.backend = Objects.requireNonNull(backend, "backend");
        this.context = Objects.requireNonNull(context, "context");
    }

    @Override
    public Set<RefKind> supportedRefKinds() {
        return SUPPORTED;
    }

    @Override
    public List<ConnectorRef> listRefs(ConnectorTableId id) {
        Objects.requireNonNull(id, "id");
        return backend.listRefs(context, id.database(), id.table());
    }

    @Override
    public void createOrReplaceRef(ConnectorTableId id, ConnectorRefMutation mutation) {
        throw new UnsupportedOperationException(
                "iceberg RefOps.createOrReplaceRef not yet implemented (tracked post-M1-07)");
    }

    @Override
    public void dropRef(ConnectorTableId id, String name, RefKind kind) {
        throw new UnsupportedOperationException(
                "iceberg RefOps.dropRef not yet implemented (tracked post-M1-07)");
    }

    @Override
    public Optional<ConnectorRef> getRef(ConnectorTableId id, String name, RefKind kind) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(kind, "kind");
        return backend.getRef(context, id.database(), id.table(), name, kind);
    }

    @Override
    public void cherrypickSnapshot(ConnectorTableId id, long snapshotId) {
        Objects.requireNonNull(id, "id");
        backend.cherrypickSnapshot(context, id.database(), id.table(), snapshotId);
    }

    @Override
    public void replaceBranch(ConnectorTableId id, String branch, long snapshotId) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(branch, "branch");
        backend.replaceBranch(context, id.database(), id.table(), branch, snapshotId);
    }

    /**
     * Resolve a {@link ConnectorTableVersion} to its canonical
     * {@link ConnectorTableVersion.BySnapshotId} form by asking the backend
     * to load the table, walk branches/tags/timestamps, and return the
     * chosen Iceberg {@link Snapshot}. Callers (scan planners) stash the
     * resolved snapshot id alongside their existing time-travel state.
     */
    public ConnectorTableVersion.BySnapshotId resolveVersion(
            String database, String table, ConnectorTableVersion version) {
        Snapshot snap = backend.resolveVersion(context, database, table, version);
        return new ConnectorTableVersion.BySnapshotId(snap.snapshotId());
    }
}
