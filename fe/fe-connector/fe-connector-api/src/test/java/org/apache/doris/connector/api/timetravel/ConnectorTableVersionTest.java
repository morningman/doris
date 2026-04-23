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

import java.time.Instant;

public class ConnectorTableVersionTest {

    @Test
    public void bySnapshotIdRoundTrip() {
        ConnectorTableVersion v = new ConnectorTableVersion.BySnapshotId(42L);
        Assertions.assertEquals(42L, ((ConnectorTableVersion.BySnapshotId) v).snapshotId());
        Assertions.assertEquals(v, new ConnectorTableVersion.BySnapshotId(42L));
    }

    @Test
    public void byTimestampRoundTripAndNullRejected() {
        Instant now = Instant.now();
        ConnectorTableVersion v = new ConnectorTableVersion.ByTimestamp(now);
        Assertions.assertEquals(now, ((ConnectorTableVersion.ByTimestamp) v).ts());
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorTableVersion.ByTimestamp(null));
    }

    @Test
    public void byRefRoundTripAndNullRejected() {
        ConnectorTableVersion.ByRef v = new ConnectorTableVersion.ByRef("main", RefKind.BRANCH);
        Assertions.assertEquals("main", v.name());
        Assertions.assertEquals(RefKind.BRANCH, v.kind());
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorTableVersion.ByRef(null, RefKind.BRANCH));
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorTableVersion.ByRef("main", null));
    }

    @Test
    public void byRefAtTimestampRoundTripAndNullRejected() {
        Instant t = Instant.parse("2024-01-01T00:00:00Z");
        ConnectorTableVersion.ByRefAtTimestamp v =
                new ConnectorTableVersion.ByRefAtTimestamp("rel", RefKind.TAG, t);
        Assertions.assertEquals("rel", v.name());
        Assertions.assertEquals(RefKind.TAG, v.kind());
        Assertions.assertEquals(t, v.ts());
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorTableVersion.ByRefAtTimestamp(null, RefKind.TAG, t));
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorTableVersion.ByRefAtTimestamp("rel", null, t));
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorTableVersion.ByRefAtTimestamp("rel", RefKind.TAG, null));
    }

    @Test
    public void byOpaqueRoundTripAndNullRejected() {
        ConnectorTableVersion.ByOpaque v = new ConnectorTableVersion.ByOpaque("tok");
        Assertions.assertEquals("tok", v.token());
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorTableVersion.ByOpaque(null));
    }

    @Test
    public void sealedExhaustiveSwitchCompiles() {
        ConnectorTableVersion v = new ConnectorTableVersion.BySnapshotId(7L);
        String label = describe(v);
        Assertions.assertEquals("snap:7", label);

        Assertions.assertEquals("ts", describe(new ConnectorTableVersion.ByTimestamp(Instant.EPOCH)));
        Assertions.assertEquals("ref:main:BRANCH",
                describe(new ConnectorTableVersion.ByRef("main", RefKind.BRANCH)));
        Assertions.assertEquals("refAt:r:TAG",
                describe(new ConnectorTableVersion.ByRefAtTimestamp("r", RefKind.TAG, Instant.EPOCH)));
        Assertions.assertEquals("opaque:x", describe(new ConnectorTableVersion.ByOpaque("x")));
    }

    private static String describe(ConnectorTableVersion v) {
        if (v instanceof ConnectorTableVersion.BySnapshotId b) {
            return "snap:" + b.snapshotId();
        } else if (v instanceof ConnectorTableVersion.ByTimestamp t) {
            Assertions.assertNotNull(t.ts());
            return "ts";
        } else if (v instanceof ConnectorTableVersion.ByRef r) {
            return "ref:" + r.name() + ":" + r.kind();
        } else if (v instanceof ConnectorTableVersion.ByRefAtTimestamp ra) {
            return "refAt:" + ra.name() + ":" + ra.kind();
        } else if (v instanceof ConnectorTableVersion.ByOpaque o) {
            return "opaque:" + o.token();
        }
        throw new AssertionError("unreachable: " + v);
    }
}
