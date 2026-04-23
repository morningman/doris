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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConnectorMtmvSnapshotTest {

    @Test
    public void versionMarker() {
        ConnectorMtmvSnapshot s = new ConnectorMtmvSnapshot.VersionMtmvSnapshot(42L);
        Assertions.assertEquals(42L, s.marker());
    }

    @Test
    public void timestampMarker() {
        ConnectorMtmvSnapshot s = new ConnectorMtmvSnapshot.TimestampMtmvSnapshot(1000L);
        Assertions.assertEquals(1000L, s.marker());
    }

    @Test
    public void maxTimestampMarker() {
        ConnectorMtmvSnapshot s = new ConnectorMtmvSnapshot.MaxTimestampMtmvSnapshot(2000L);
        Assertions.assertEquals(2000L, s.marker());
    }

    @Test
    public void snapshotIdMarker() {
        ConnectorMtmvSnapshot s = new ConnectorMtmvSnapshot.SnapshotIdMtmvSnapshot(999L);
        Assertions.assertEquals(999L, s.marker());
    }

    @Test
    public void distinctSubtypesNotEqual() {
        ConnectorMtmvSnapshot a = new ConnectorMtmvSnapshot.VersionMtmvSnapshot(1L);
        ConnectorMtmvSnapshot b = new ConnectorMtmvSnapshot.SnapshotIdMtmvSnapshot(1L);
        Assertions.assertNotEquals(a, b);
        Assertions.assertEquals(a.marker(), b.marker());
    }

    @Test
    public void sealedExhaustiveMatrix() {
        ConnectorMtmvSnapshot[] all = new ConnectorMtmvSnapshot[] {
                new ConnectorMtmvSnapshot.VersionMtmvSnapshot(1L),
                new ConnectorMtmvSnapshot.TimestampMtmvSnapshot(2L),
                new ConnectorMtmvSnapshot.MaxTimestampMtmvSnapshot(3L),
                new ConnectorMtmvSnapshot.SnapshotIdMtmvSnapshot(4L),
        };
        long sum = 0;
        for (ConnectorMtmvSnapshot s : all) {
            if (s instanceof ConnectorMtmvSnapshot.VersionMtmvSnapshot v) {
                sum += v.version();
            } else if (s instanceof ConnectorMtmvSnapshot.TimestampMtmvSnapshot t) {
                sum += t.epochMillis();
            } else if (s instanceof ConnectorMtmvSnapshot.MaxTimestampMtmvSnapshot m) {
                sum += m.epochMillis();
            } else if (s instanceof ConnectorMtmvSnapshot.SnapshotIdMtmvSnapshot id) {
                sum += id.snapshotId();
            }
        }
        Assertions.assertEquals(10L, sum);
    }
}
