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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class RefOpsTest {

    private static RefOps newInMemory(List<ConnectorRef> store) {
        return new RefOps() {
            @Override
            public Set<RefKind> supportedRefKinds() {
                return EnumSet.of(RefKind.BRANCH, RefKind.TAG);
            }

            @Override
            public List<ConnectorRef> listRefs(String database, String table) {
                return new ArrayList<>(store);
            }

            @Override
            public void createOrReplaceRef(String database, String table, ConnectorRefMutation mutation) {
                Iterator<ConnectorRef> it = store.iterator();
                while (it.hasNext()) {
                    ConnectorRef r = it.next();
                    if (r.name().equals(mutation.name()) && r.kind() == mutation.kind()) {
                        if (!mutation.replaceIfExists()) {
                            throw new IllegalStateException("already exists");
                        }
                        it.remove();
                        break;
                    }
                }
                store.add(ConnectorRef.builder()
                        .name(mutation.name())
                        .kind(mutation.kind())
                        .snapshotId(mutation.fromSnapshot().orElse(-1L))
                        .build());
            }

            @Override
            public void dropRef(String database, String table, String name, RefKind kind) {
                store.removeIf(r -> r.name().equals(name) && r.kind() == kind);
            }
        };
    }

    @Test
    public void roundTrip() {
        List<ConnectorRef> store = new ArrayList<>();
        RefOps ops = newInMemory(store);

        Assertions.assertTrue(ops.supportedRefKinds().contains(RefKind.BRANCH));
        Assertions.assertTrue(ops.supportedRefKinds().contains(RefKind.TAG));
        Assertions.assertFalse(ops.supportedRefKinds().contains(RefKind.UNKNOWN));

        Assertions.assertTrue(ops.listRefs("db", "t").isEmpty());

        ops.createOrReplaceRef("db", "t",
                ConnectorRefMutation.builder().name("main").kind(RefKind.BRANCH).fromSnapshot(1L).build());
        ops.createOrReplaceRef("db", "t",
                ConnectorRefMutation.builder().name("v1").kind(RefKind.TAG).fromSnapshot(1L).build());

        List<ConnectorRef> refs = ops.listRefs("db", "t");
        Assertions.assertEquals(2, refs.size());

        ops.createOrReplaceRef("db", "t",
                ConnectorRefMutation.builder().name("main").kind(RefKind.BRANCH)
                        .fromSnapshot(2L).replaceIfExists(true).build());
        Assertions.assertEquals(2L, ops.listRefs("db", "t").stream()
                .filter(r -> r.name().equals("main")).findFirst().orElseThrow().snapshotId());

        ops.dropRef("db", "t", "v1", RefKind.TAG);
        Assertions.assertEquals(1, ops.listRefs("db", "t").size());
    }
}
