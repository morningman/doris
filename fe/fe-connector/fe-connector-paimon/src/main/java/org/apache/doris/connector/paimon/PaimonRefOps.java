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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.api.timetravel.ConnectorRef;
import org.apache.doris.connector.api.timetravel.ConnectorRefMutation;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;
import org.apache.doris.connector.api.timetravel.RefKind;
import org.apache.doris.connector.api.timetravel.RefOps;
import org.apache.doris.connector.paimon.api.PaimonBackend;
import org.apache.doris.connector.paimon.api.PaimonBackendContext;

import org.apache.paimon.Snapshot;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Paimon-specific {@link RefOps} wiring. Delegates read-side operations
 * ({@link #supportedRefKinds()}, {@link #listRefs(String, String)}) to the
 * selected {@link PaimonBackend} (routed via {@link PaimonBackendRegistry}).
 * Mutation-side methods ({@link #createOrReplaceRef}, {@link #dropRef}) are
 * intentionally left as {@link UnsupportedOperationException} in M1-08:
 * paimon branches are <em>schema-level</em> isolated metadata trees with
 * write restrictions (cf. D5 / paimon plan-doc), so branch creation is
 * deliberately not wired through {@code RefOps} until M1-11 (write-path
 * migration) clarifies the semantics. RefOps is read-only for paimon.
 *
 * <p>Additionally exposes
 * {@link #resolveVersion(String, String, ConnectorTableVersion)} — a
 * paimon-specific extension used by the scan side to translate a D5
 * {@link ConnectorTableVersion} into a concrete paimon
 * {@link Snapshot} pin. The method returns a
 * {@link ConnectorTableVersion.BySnapshotId} carrying the resolved snapshot
 * id so callers stay on the api-shared sealed hierarchy. Note: for paimon
 * branch refs the snapshot id is branch-local (see
 * {@code PaimonBackend#resolveVersion}); branch-name plumbing for the scan
 * side is tracked as follow-up work.
 */
public final class PaimonRefOps implements RefOps {

    private static final Set<RefKind> SUPPORTED =
            Set.of(RefKind.BRANCH, RefKind.TAG);

    private final PaimonBackend backend;
    private final PaimonBackendContext context;

    public PaimonRefOps(PaimonBackend backend, PaimonBackendContext context) {
        this.backend = Objects.requireNonNull(backend, "backend");
        this.context = Objects.requireNonNull(context, "context");
    }

    @Override
    public Set<RefKind> supportedRefKinds() {
        return SUPPORTED;
    }

    @Override
    public List<ConnectorRef> listRefs(String database, String table) {
        return backend.listRefs(context, database, table);
    }

    @Override
    public void createOrReplaceRef(String database, String table, ConnectorRefMutation mutation) {
        throw new UnsupportedOperationException(
                "paimon RefOps.createOrReplaceRef is intentionally read-only in M1-08;"
                        + " branch writes are subject to paimon's schema-level branch"
                        + " restrictions and are tracked for the M1-11 write-path migration");
    }

    @Override
    public void dropRef(String database, String table, String name, RefKind kind) {
        throw new UnsupportedOperationException(
                "paimon RefOps.dropRef is intentionally read-only in M1-08;"
                        + " ref mutations are tracked for the M1-11 write-path migration");
    }

    /**
     * Resolve a {@link ConnectorTableVersion} to its canonical
     * {@link ConnectorTableVersion.BySnapshotId} form by asking the backend
     * to load the table, walk branches/tags/timestamps, and return the
     * chosen paimon {@link Snapshot}. Callers (scan planners) stash the
     * resolved snapshot id alongside their existing time-travel state.
     */
    public ConnectorTableVersion.BySnapshotId resolveVersion(
            String database, String table, ConnectorTableVersion version) {
        Snapshot snap = backend.resolveVersion(context, database, table, version);
        return new ConnectorTableVersion.BySnapshotId(snap.id());
    }
}
