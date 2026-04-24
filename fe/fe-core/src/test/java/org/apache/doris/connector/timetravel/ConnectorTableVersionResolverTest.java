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

package org.apache.doris.connector.timetravel;

import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;
import org.apache.doris.connector.api.timetravel.RefKind;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Optional;

public class ConnectorTableVersionResolverTest {

    private static final ZoneId UTC = ZoneOffset.UTC;
    private static final ZoneId TOKYO = ZoneId.of("Asia/Tokyo");

    // ---- BySnapshotId ----
    @Test
    public void resolvesSnapshotIdFromForVersion() {
        Optional<ConnectorTableVersion> v =
                ConnectorTableVersionResolver.resolve(TableSnapshot.versionOf("12345"), UTC);
        Assertions.assertTrue(v.isPresent());
        ConnectorTableVersion.BySnapshotId by =
                Assertions.assertInstanceOf(ConnectorTableVersion.BySnapshotId.class, v.get());
        Assertions.assertEquals(12345L, by.snapshotId());
    }

    @Test
    public void rejectsNonNumericSnapshotId() {
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorTableVersionResolver.resolve(TableSnapshot.versionOf("not-a-number"), UTC));
        Assertions.assertTrue(ex.getMessage().contains("Invalid snapshot id"));
    }

    // ---- ByTimestamp ----
    @Test
    public void resolvesTimestampFromForTimeUtc() {
        Optional<ConnectorTableVersion> v = ConnectorTableVersionResolver.resolve(
                TableSnapshot.timeOf("2024-01-01 00:00:00"), UTC);
        Assertions.assertTrue(v.isPresent());
        ConnectorTableVersion.ByTimestamp ts =
                Assertions.assertInstanceOf(ConnectorTableVersion.ByTimestamp.class, v.get());
        Assertions.assertEquals(
                LocalDateTime.of(2024, 1, 1, 0, 0, 0).atZone(UTC).toInstant(),
                ts.ts());
    }

    @Test
    public void resolvesTimestampHonoursZone() {
        Instant utc = ConnectorTableVersionResolver.resolve(
                TableSnapshot.timeOf("2024-01-01 00:00:00"), UTC).get() instanceof ConnectorTableVersion.ByTimestamp t
                ? t.ts() : null;
        Instant tokyo = ConnectorTableVersionResolver.resolve(
                TableSnapshot.timeOf("2024-01-01 00:00:00"), TOKYO).get() instanceof ConnectorTableVersion.ByTimestamp t
                ? t.ts() : null;
        Assertions.assertNotNull(utc);
        Assertions.assertNotNull(tokyo);
        // Tokyo is UTC+9 → the same wall clock literal sits 9h earlier on the global timeline.
        Assertions.assertEquals(9 * 3600L, utc.getEpochSecond() - tokyo.getEpochSecond());
    }

    @Test
    public void resolvesTimestampWithMillisAndIsoSeparator() {
        ConnectorTableVersion v1 = ConnectorTableVersionResolver.resolve(
                TableSnapshot.timeOf("2024-01-01 00:00:00.250"), UTC).get();
        ConnectorTableVersion v2 = ConnectorTableVersionResolver.resolve(
                TableSnapshot.timeOf("2024-01-01T00:00:00.250"), UTC).get();
        Assertions.assertEquals(v1, v2);
        ConnectorTableVersion.ByTimestamp ts = (ConnectorTableVersion.ByTimestamp) v1;
        Assertions.assertEquals(250, ts.ts().toEpochMilli() % 1000);
    }

    @Test
    public void rejectsAmbiguousTimestamp() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorTableVersionResolver.resolve(TableSnapshot.timeOf("01/01/2024"), UTC));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorTableVersionResolver.resolve(TableSnapshot.timeOf("2024-01-01"), UTC));
    }

    // ---- ByRef (BRANCH / TAG) ----
    @Test
    public void buildsBranchByRef() {
        ConnectorTableVersion v = ConnectorTableVersionResolver.forBranch("main");
        ConnectorTableVersion.ByRef r =
                Assertions.assertInstanceOf(ConnectorTableVersion.ByRef.class, v);
        Assertions.assertEquals("main", r.name());
        Assertions.assertEquals(RefKind.BRANCH, r.kind());
    }

    @Test
    public void buildsTagByRef() {
        ConnectorTableVersion v = ConnectorTableVersionResolver.forTag("release-2024-01");
        ConnectorTableVersion.ByRef r =
                Assertions.assertInstanceOf(ConnectorTableVersion.ByRef.class, v);
        Assertions.assertEquals("release-2024-01", r.name());
        Assertions.assertEquals(RefKind.TAG, r.kind());
    }

    @Test
    public void rejectsBlankRefName() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorTableVersionResolver.forBranch(" "));
        Assertions.assertThrows(NullPointerException.class,
                () -> ConnectorTableVersionResolver.forTag(null));
    }

    // ---- ByRefAtTimestamp ----
    @Test
    public void buildsBranchAtTimestamp() {
        Instant ts = Instant.parse("2024-01-01T00:00:00Z");
        ConnectorTableVersion v = ConnectorTableVersionResolver.forBranchAtTimestamp("main", ts);
        ConnectorTableVersion.ByRefAtTimestamp r =
                Assertions.assertInstanceOf(ConnectorTableVersion.ByRefAtTimestamp.class, v);
        Assertions.assertEquals("main", r.name());
        Assertions.assertEquals(RefKind.BRANCH, r.kind());
        Assertions.assertEquals(ts, r.ts());
    }

    @Test
    public void buildsTagAtTimestamp() {
        Instant ts = Instant.parse("2024-06-30T12:34:56Z");
        ConnectorTableVersion v = ConnectorTableVersionResolver.forTagAtTimestamp("rel", ts);
        ConnectorTableVersion.ByRefAtTimestamp r =
                Assertions.assertInstanceOf(ConnectorTableVersion.ByRefAtTimestamp.class, v);
        Assertions.assertEquals(RefKind.TAG, r.kind());
    }

    // ---- ByOpaque (covers MVCC stamp slot) ----
    @Test
    public void buildsOpaqueToken() {
        ConnectorTableVersion v = ConnectorTableVersionResolver.forOpaque("mvcc:42:abcd");
        ConnectorTableVersion.ByOpaque o =
                Assertions.assertInstanceOf(ConnectorTableVersion.ByOpaque.class, v);
        Assertions.assertEquals("mvcc:42:abcd", o.token());
    }

    // ---- null & empty ----
    @Test
    public void nullSnapshotIsEmpty() {
        Assertions.assertTrue(ConnectorTableVersionResolver.resolve(null).isEmpty());
        Assertions.assertTrue(ConnectorTableVersionResolver.resolve(null, UTC).isEmpty());
    }

    // ---- sealed-hierarchy coverage check ----
    @Test
    public void coversAllFiveSealedSubtypes() {
        ConnectorTableVersion[] cases = new ConnectorTableVersion[] {
                ConnectorTableVersionResolver.resolve(TableSnapshot.versionOf("1"), UTC).get(),
                ConnectorTableVersionResolver.resolve(TableSnapshot.timeOf("2024-01-01 00:00:00"), UTC).get(),
                ConnectorTableVersionResolver.forBranch("main"),
                ConnectorTableVersionResolver.forBranchAtTimestamp("main", Instant.EPOCH),
                ConnectorTableVersionResolver.forOpaque("tok"),
        };
        Assertions.assertTrue(cases[0] instanceof ConnectorTableVersion.BySnapshotId);
        Assertions.assertTrue(cases[1] instanceof ConnectorTableVersion.ByTimestamp);
        Assertions.assertTrue(cases[2] instanceof ConnectorTableVersion.ByRef);
        Assertions.assertTrue(cases[3] instanceof ConnectorTableVersion.ByRefAtTimestamp);
        Assertions.assertTrue(cases[4] instanceof ConnectorTableVersion.ByOpaque);
    }
}
