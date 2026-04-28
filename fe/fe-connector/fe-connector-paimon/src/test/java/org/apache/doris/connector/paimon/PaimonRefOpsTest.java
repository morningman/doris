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

import org.apache.doris.connector.api.ConnectorTableId;
import org.apache.doris.connector.api.timetravel.ConnectorRef;
import org.apache.doris.connector.api.timetravel.ConnectorRefMutation;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;
import org.apache.doris.connector.api.timetravel.RefKind;
import org.apache.doris.connector.paimon.api.PaimonBackend;
import org.apache.doris.connector.paimon.api.PaimonBackendContext;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

class PaimonRefOpsTest {

    private static PaimonBackendContext newCtx() {
        return new PaimonBackendContext("cat", new HashMap<>());
    }

    private static final class RecordingBackend implements PaimonBackend {
        private final List<ConnectorRef> refs;
        private final Snapshot snapshot;
        final AtomicReference<String> lastCall = new AtomicReference<>();

        RecordingBackend(List<ConnectorRef> refs, Snapshot snapshot) {
            this.refs = refs;
            this.snapshot = snapshot;
        }

        @Override
        public String name() {
            return "fake";
        }

        @Override
        public Catalog buildCatalog(PaimonBackendContext context) {
            throw new AssertionError("buildCatalog must not be invoked in this test");
        }

        @Override
        public List<ConnectorRef> listRefs(PaimonBackendContext context,
                                           String database, String table) {
            lastCall.set("listRefs:" + database + "." + table);
            return refs;
        }

        @Override
        public Snapshot resolveVersion(PaimonBackendContext context,
                                       String database, String table,
                                       ConnectorTableVersion version) {
            lastCall.set("resolveVersion:" + database + "." + table + ":"
                    + version.getClass().getSimpleName());
            return snapshot;
        }
    }

    @Test
    void supportedRefKindsReturnsBranchAndTag() {
        PaimonRefOps ops = new PaimonRefOps(
                new RecordingBackend(Collections.emptyList(), null), newCtx());
        Assertions.assertEquals(Set.of(RefKind.BRANCH, RefKind.TAG), ops.supportedRefKinds());
    }

    @Test
    void listRefsDelegatesToBackend() {
        ConnectorRef expected = ConnectorRef.builder()
                .name("main").kind(RefKind.BRANCH).snapshotId(-1L).build();
        RecordingBackend backend = new RecordingBackend(List.of(expected), null);
        PaimonRefOps ops = new PaimonRefOps(backend, newCtx());

        List<ConnectorRef> got = ops.listRefs(ConnectorTableId.of("db", "t"));

        Assertions.assertEquals(List.of(expected), got);
        Assertions.assertEquals("listRefs:db.t", backend.lastCall.get());
    }

    @Test
    void resolveVersionReturnsBySnapshotId() {
        Snapshot snap = org.mockito.Mockito.mock(Snapshot.class);
        org.mockito.Mockito.when(snap.id()).thenReturn(9001L);
        RecordingBackend backend = new RecordingBackend(Collections.emptyList(), snap);
        PaimonRefOps ops = new PaimonRefOps(backend, newCtx());

        ConnectorTableVersion.BySnapshotId out = ops.resolveVersion("db", "t", new ConnectorTableVersion.ByRef("main", RefKind.BRANCH));

        Assertions.assertEquals(9001L, out.snapshotId());
        Assertions.assertEquals("resolveVersion:db.t:ByRef", backend.lastCall.get());
    }

    @Test
    void mutationMethodsThrowUoe() {
        PaimonRefOps ops = new PaimonRefOps(
                new RecordingBackend(Collections.emptyList(), null), newCtx());
        ConnectorRefMutation mut = ConnectorRefMutation.builder()
                .name("b").kind(RefKind.BRANCH).build();
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> ops.createOrReplaceRef(ConnectorTableId.of("db", "t"), mut));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> ops.dropRef(ConnectorTableId.of("db", "t"), "b", RefKind.BRANCH));
    }

    @Test
    void rejectsNullBackendOrContext() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new PaimonRefOps(null, newCtx()));
        Assertions.assertThrows(NullPointerException.class,
                () -> new PaimonRefOps(
                        new RecordingBackend(Collections.emptyList(), null), null));
    }

    @Test
    void registryRoutingProducesBackendOfCorrectType() {
        // Cross-check: the registry already knows each real backend name;
        // the PaimonRefOps constructed off of a registry-supplied backend
        // exposes that backend's name() through a smoke path.
        for (String name : PaimonBackendRegistry.availableTypes()) {
            PaimonBackend backend = PaimonBackendRegistry.get(name)
                    .orElseThrow().create();
            Assertions.assertEquals(name, backend.name(),
                    "backend factory " + name + " produced mismatched backend");
        }
    }
}
