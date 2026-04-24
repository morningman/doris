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
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;
import org.apache.doris.connector.api.timetravel.RefKind;
import org.apache.doris.connector.api.timetravel.RefOps;
import org.apache.doris.connector.paimon.api.PaimonBackend;
import org.apache.doris.connector.paimon.api.PaimonBackendContext;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

class PaimonConnectorMetadataRefOpsTest {

    private static final class StubBackend implements PaimonBackend {
        @Override
        public String name() {
            return "stub";
        }

        @Override
        public Catalog buildCatalog(PaimonBackendContext context) {
            throw new AssertionError("not invoked");
        }

        @Override
        public List<ConnectorRef> listRefs(PaimonBackendContext context,
                                           String database, String table) {
            return List.of(ConnectorRef.builder()
                    .name("main").kind(RefKind.BRANCH).snapshotId(-1L).build());
        }

        @Override
        public Snapshot resolveVersion(PaimonBackendContext context,
                                       String database, String table,
                                       ConnectorTableVersion version) {
            Snapshot s = org.mockito.Mockito.mock(Snapshot.class);
            org.mockito.Mockito.when(s.id()).thenReturn(123L);
            return s;
        }
    }

    @Test
    void refOpsReturnsEmptyWhenBackendNotProvided() {
        PaimonConnectorMetadata md = new PaimonConnectorMetadata(null, new HashMap<>());
        Assertions.assertEquals(Optional.empty(), md.refOps());
    }

    @Test
    void refOpsReturnsPresentWhenBackendAndContextWired() {
        PaimonConnectorMetadata md = new PaimonConnectorMetadata(
                null, new HashMap<>(),
                new StubBackend(),
                new PaimonBackendContext("c", new HashMap<>()));
        Optional<RefOps> ops = md.refOps();
        Assertions.assertTrue(ops.isPresent());
        Assertions.assertTrue(ops.get() instanceof PaimonRefOps);
    }

    @Test
    void refOpsSupportedRefKindsAndListRefsRoute() {
        PaimonConnectorMetadata md = new PaimonConnectorMetadata(
                null, new HashMap<>(),
                new StubBackend(),
                new PaimonBackendContext("c", new HashMap<>()));
        RefOps ops = md.refOps().orElseThrow();
        Assertions.assertTrue(ops.supportedRefKinds().contains(RefKind.BRANCH));
        Assertions.assertTrue(ops.supportedRefKinds().contains(RefKind.TAG));
        List<ConnectorRef> refs = ops.listRefs("db", "t");
        Assertions.assertEquals(1, refs.size());
        Assertions.assertEquals("main", refs.get(0).name());
    }

    @Test
    void refOpsResolveVersionReturnsBySnapshotId() {
        PaimonConnectorMetadata md = new PaimonConnectorMetadata(
                null, new HashMap<>(),
                new StubBackend(),
                new PaimonBackendContext("c", new HashMap<>()));
        PaimonRefOps ops = (PaimonRefOps) md.refOps().orElseThrow();
        ConnectorTableVersion.BySnapshotId out = ops.resolveVersion(
                "db", "t", new ConnectorTableVersion.BySnapshotId(123L));
        Assertions.assertEquals(123L, out.snapshotId());
    }

    @Test
    void refOpsAbsentForLegacyConstructor() {
        PaimonConnectorMetadata md = new PaimonConnectorMetadata(
                null, Collections.emptyMap());
        Assertions.assertTrue(md.refOps().isEmpty());
    }
}
