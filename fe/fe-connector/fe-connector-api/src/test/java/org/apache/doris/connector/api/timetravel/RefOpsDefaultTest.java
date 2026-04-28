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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Tests for the default-method behaviour on {@link RefOps}: the linear
 * {@code getRef} scan and the {@code UnsupportedOperationException}
 * surfacing for {@code cherrypickSnapshot} / {@code replaceBranch}.
 */
class RefOpsDefaultTest {

    private static final ConnectorRef MAIN = ConnectorRef.builder()
            .name("main").kind(RefKind.BRANCH).snapshotId(1L).build();
    private static final ConnectorRef DEV = ConnectorRef.builder()
            .name("dev").kind(RefKind.BRANCH).snapshotId(2L).build();
    private static final ConnectorRef V1 = ConnectorRef.builder()
            .name("v1").kind(RefKind.TAG).snapshotId(3L).build();

    private static final class StubOps implements RefOps {
        private final List<ConnectorRef> refs;

        StubOps(List<ConnectorRef> refs) {
            this.refs = refs;
        }

        @Override
        public Set<RefKind> supportedRefKinds() {
            return Set.of(RefKind.BRANCH, RefKind.TAG);
        }

        @Override
        public List<ConnectorRef> listRefs(ConnectorTableId id) {
            return refs;
        }

        @Override
        public void createOrReplaceRef(ConnectorTableId id, ConnectorRefMutation mutation) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropRef(ConnectorTableId id, String name, RefKind kind) {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    void getRefDefaultFindsMatchingRef() {
        RefOps ops = new StubOps(List.of(MAIN, DEV, V1));
        Optional<ConnectorRef> got = ops.getRef(ConnectorTableId.of("db", "t"), "dev", RefKind.BRANCH);
        Assertions.assertTrue(got.isPresent());
        Assertions.assertEquals(DEV, got.get());
    }

    @Test
    void getRefDefaultEmptyWhenNameMissing() {
        RefOps ops = new StubOps(List.of(MAIN, DEV, V1));
        Optional<ConnectorRef> got = ops.getRef(ConnectorTableId.of("db", "t"), "nope", RefKind.BRANCH);
        Assertions.assertTrue(got.isEmpty());
    }

    @Test
    void getRefDefaultEmptyWhenKindMismatches() {
        RefOps ops = new StubOps(List.of(MAIN, DEV, V1));
        // 'main' exists as a BRANCH; asking as TAG must return empty rather
        // than the branch entry.
        Optional<ConnectorRef> got = ops.getRef(ConnectorTableId.of("db", "t"), "main", RefKind.TAG);
        Assertions.assertTrue(got.isEmpty());
    }

    @Test
    void getRefDefaultRejectsNullArgs() {
        RefOps ops = new StubOps(List.of(MAIN));
        Assertions.assertThrows(NullPointerException.class,
                () -> ops.getRef(ConnectorTableId.of("db", "t"), null, RefKind.BRANCH));
        Assertions.assertThrows(NullPointerException.class,
                () -> ops.getRef(ConnectorTableId.of("db", "t"), "main", null));
    }

    @Test
    void cherrypickSnapshotDefaultThrowsUoe() {
        RefOps ops = new StubOps(List.of());
        UnsupportedOperationException ex = Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> ops.cherrypickSnapshot(ConnectorTableId.of("db", "t"), 42L));
        Assertions.assertTrue(ex.getMessage().contains("cherrypickSnapshot"));
    }

    @Test
    void replaceBranchDefaultThrowsUoe() {
        RefOps ops = new StubOps(List.of());
        UnsupportedOperationException ex = Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> ops.replaceBranch(ConnectorTableId.of("db", "t"), "main", 42L));
        Assertions.assertTrue(ex.getMessage().contains("replaceBranch"));
    }
}
