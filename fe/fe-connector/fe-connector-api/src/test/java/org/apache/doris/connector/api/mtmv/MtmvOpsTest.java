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

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorTableId;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.timetravel.ConnectorMvccSnapshot;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class MtmvOpsTest {

    /** Minimal in-memory implementation that exercises the full method matrix. */
    private static final class StubMtmvOps implements MtmvOps {
        @Override
        public Map<String, ConnectorPartitionItem> listPartitions(
                ConnectorTableId id, Optional<ConnectorMvccSnapshot> snapshot) {
            Map<String, ConnectorPartitionItem> m = new HashMap<>();
            m.put("p", new ConnectorPartitionItem.UnpartitionedItem());
            return m;
        }

        @Override
        public ConnectorPartitionType getPartitionType(
                ConnectorTableId id, Optional<ConnectorMvccSnapshot> snapshot) {
            return ConnectorPartitionType.UNPARTITIONED;
        }

        @Override
        public Set<String> getPartitionColumnNames(
                ConnectorTableId id, Optional<ConnectorMvccSnapshot> snapshot) {
            return Collections.emptySet();
        }

        @Override
        public List<ConnectorColumn> getPartitionColumns(
                ConnectorTableId id, Optional<ConnectorMvccSnapshot> snapshot) {
            return Collections.emptyList();
        }

        @Override
        public ConnectorMtmvSnapshot getPartitionSnapshot(
                ConnectorTableId id, String partitionName,
                MtmvRefreshHint hint, Optional<ConnectorMvccSnapshot> snapshot) {
            return new ConnectorMtmvSnapshot.VersionMtmvSnapshot(1L);
        }

        @Override
        public ConnectorMtmvSnapshot getTableSnapshot(
                ConnectorTableId id,
                MtmvRefreshHint hint, Optional<ConnectorMvccSnapshot> snapshot) {
            return new ConnectorMtmvSnapshot.SnapshotIdMtmvSnapshot(7L);
        }

        @Override
        public long getNewestUpdateVersionOrTime(ConnectorTableId id) {
            return 100L;
        }

        @Override
        public boolean isPartitionColumnAllowNull(ConnectorTableId id) {
            return false;
        }

        @Override
        public boolean isValidRelatedTable(ConnectorTableId id) {
            return true;
        }
    }

    @Test
    public void stubExposesAllMethods() {
        MtmvOps ops = new StubMtmvOps();
        Optional<ConnectorMvccSnapshot> snap = Optional.empty();

        Assertions.assertEquals(1, ops.listPartitions(ConnectorTableId.of("d", "t"), snap).size());
        Assertions.assertEquals(ConnectorPartitionType.UNPARTITIONED,
                ops.getPartitionType(ConnectorTableId.of("d", "t"), snap));
        Assertions.assertTrue(ops.getPartitionColumnNames(ConnectorTableId.of("d", "t"), snap).isEmpty());
        Assertions.assertTrue(ops.getPartitionColumns(ConnectorTableId.of("d", "t"), snap).isEmpty());
        Assertions.assertEquals(1L, ops.getPartitionSnapshot(ConnectorTableId.of("d", "t"), "p",
                MtmvRefreshHint.of(MtmvRefreshHint.RefreshMode.FORCE_FULL), snap).marker());
        Assertions.assertEquals(7L, ops.getTableSnapshot(ConnectorTableId.of("d", "t"),
                MtmvRefreshHint.of(MtmvRefreshHint.RefreshMode.FORCE_FULL), snap).marker());
        Assertions.assertEquals(100L, ops.getNewestUpdateVersionOrTime(ConnectorTableId.of("d", "t")));
        Assertions.assertFalse(ops.isPartitionColumnAllowNull(ConnectorTableId.of("d", "t")));
        Assertions.assertTrue(ops.isValidRelatedTable(ConnectorTableId.of("d", "t")));
    }

    @Test
    public void needAutoRefreshDefaultsTrue() {
        MtmvOps ops = new StubMtmvOps();
        Assertions.assertTrue(ops.needAutoRefresh(ConnectorTableId.of("d", "t")));
    }

    @Test
    public void connectorColumnConstructible() {
        // Smoke: ensure api.mtmv compiles against the existing ConnectorColumn type.
        ConnectorColumn c = new ConnectorColumn("p", new ConnectorType("STRING"), null, true, null);
        Assertions.assertEquals("p", c.getName());
    }
}
