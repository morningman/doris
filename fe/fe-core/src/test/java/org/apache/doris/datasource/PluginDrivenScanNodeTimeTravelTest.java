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

package org.apache.doris.datasource;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.scan.ConnectorScanRequest;
import org.apache.doris.connector.api.timetravel.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.timetravel.ConnectorRefSpec;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;
import org.apache.doris.connector.api.timetravel.RefKind;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.Collections;
import java.util.Optional;

/**
 * Verifies that {@link PluginDrivenScanNode} threads the time-travel
 * coordinates set by the Nereids translator
 * ({@link ConnectorTableVersion}, {@link ConnectorRefSpec},
 * {@link ConnectorMvccSnapshot}) into the {@link ConnectorScanRequest}
 * passed to {@code planScan}.
 */
public class PluginDrivenScanNodeTimeTravelTest {

    private static PluginDrivenScanNode newNode() {
        Connector connector = Mockito.mock(Connector.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        SessionVariable sv = new SessionVariable();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        return new PluginDrivenScanNode(new PlanNodeId(0), desc, false, sv,
                ScanContext.EMPTY, connector, session, handle);
    }

    private static void setLimit(PluginDrivenScanNode node, long limit) {
        node.setLimit(limit);
    }

    @Test
    public void requestHasEmptyTimeTravelByDefault() {
        PluginDrivenScanNode node = newNode();
        ConnectorScanRequest req = node.buildScanRequest(Collections.emptyList(), Optional.empty());
        Assertions.assertTrue(req.getVersion().isEmpty());
        Assertions.assertTrue(req.getRefSpec().isEmpty());
        Assertions.assertTrue(req.getMvccSnapshot().isEmpty());
        Assertions.assertTrue(req.getLimit().isEmpty());
    }

    @Test
    public void requestPropagatesConnectorTableVersion() {
        PluginDrivenScanNode node = newNode();
        ConnectorTableVersion version = new ConnectorTableVersion.BySnapshotId(987L);
        node.setConnectorTableVersion(version);

        ConnectorScanRequest req = node.buildScanRequest(Collections.emptyList(), Optional.empty());
        Assertions.assertTrue(req.getVersion().isPresent());
        Assertions.assertEquals(version, req.getVersion().get());
    }

    @Test
    public void requestPropagatesRefSpec() {
        PluginDrivenScanNode node = newNode();
        ConnectorRefSpec spec = ConnectorRefSpec.builder().name("dev").kind(RefKind.BRANCH).build();
        node.setConnectorRefSpec(spec);

        ConnectorScanRequest req = node.buildScanRequest(Collections.emptyList(), Optional.empty());
        Assertions.assertTrue(req.getRefSpec().isPresent());
        Assertions.assertEquals(spec, req.getRefSpec().get());
    }

    @Test
    public void requestPropagatesMvccSnapshot() {
        PluginDrivenScanNode node = newNode();
        ConnectorMvccSnapshot snapshot = new ConnectorMvccSnapshot() {
            @Override
            public Instant commitTime() {
                return Instant.EPOCH;
            }

            @Override
            public Optional<ConnectorTableVersion> asVersion() {
                return Optional.empty();
            }

            @Override
            public String toOpaqueToken() {
                return "tok";
            }
        };
        node.setConnectorMvccSnapshot(snapshot);

        ConnectorScanRequest req = node.buildScanRequest(Collections.emptyList(), Optional.empty());
        Assertions.assertTrue(req.getMvccSnapshot().isPresent());
        Assertions.assertSame(snapshot, req.getMvccSnapshot().get());
    }

    @Test
    public void requestPropagatesPositiveLimit() {
        PluginDrivenScanNode node = newNode();
        setLimit(node, 50L);

        ConnectorScanRequest req = node.buildScanRequest(Collections.emptyList(), Optional.empty());
        Assertions.assertTrue(req.getLimit().isPresent());
        Assertions.assertEquals(50L, req.getLimit().getAsLong());
    }

    @Test
    public void settersAndGettersAreSymmetric() {
        PluginDrivenScanNode node = newNode();

        Assertions.assertNull(node.getConnectorRefSpec());
        Assertions.assertNull(node.getConnectorMvccSnapshot());

        ConnectorRefSpec spec = ConnectorRefSpec.builder().name("v1").kind(RefKind.TAG).build();
        node.setConnectorRefSpec(spec);
        Assertions.assertSame(spec, node.getConnectorRefSpec());

        ConnectorMvccSnapshot snap = new ConnectorMvccSnapshot() {
            @Override
            public Instant commitTime() {
                return Instant.EPOCH;
            }

            @Override
            public Optional<ConnectorTableVersion> asVersion() {
                return Optional.empty();
            }

            @Override
            public String toOpaqueToken() {
                return "t";
            }
        };
        node.setConnectorMvccSnapshot(snap);
        Assertions.assertSame(snap, node.getConnectorMvccSnapshot());
    }
}
