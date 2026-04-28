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

package org.apache.doris.connector.api.scan;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.timetravel.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.timetravel.ConnectorRefSpec;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;
import org.apache.doris.connector.api.timetravel.RefKind;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

class ConnectorScanRequestTest {

    private static class StubSession implements ConnectorSession, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public String getQueryId() {
            return "q";
        }

        @Override
        public String getUser() {
            return "u";
        }

        @Override
        public String getTimeZone() {
            return "UTC";
        }

        @Override
        public String getLocale() {
            return "en_US";
        }

        @Override
        public long getCatalogId() {
            return 1L;
        }

        @Override
        public String getCatalogName() {
            return "ctl";
        }

        @Override
        public <T> T getProperty(String name, Class<T> type) {
            return null;
        }

        @Override
        public Map<String, String> getCatalogProperties() {
            return Collections.emptyMap();
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof StubSession;
        }

        @Override
        public int hashCode() {
            return 17;
        }
    }

    private static class StubHandle implements ConnectorTableHandle {
        private static final long serialVersionUID = 1L;
        private final String name;

        StubHandle(String name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof StubHandle && ((StubHandle) o).name.equals(name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }
    }

    private static class StubColumn implements ConnectorColumnHandle {
        private static final long serialVersionUID = 1L;
        private final String name;

        StubColumn(String name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof StubColumn && ((StubColumn) o).name.equals(name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }
    }

    private static class StubMvcc implements ConnectorMvccSnapshot, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public Instant commitTime() {
            return Instant.ofEpochMilli(123L);
        }

        @Override
        public Optional<ConnectorTableVersion> asVersion() {
            return Optional.of(new ConnectorTableVersion.BySnapshotId(7L));
        }

        @Override
        public String toOpaqueToken() {
            return "stub:7:123";
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof StubMvcc;
        }

        @Override
        public int hashCode() {
            return 41;
        }
    }

    @Test
    void builderDefaultsAreEmpty() {
        ConnectorScanRequest r = ConnectorScanRequest.builder()
                .session(new StubSession())
                .table(new StubHandle("t"))
                .build();
        Assertions.assertTrue(r.getColumns().isEmpty());
        Assertions.assertEquals(Optional.empty(), r.getFilter());
        Assertions.assertEquals(OptionalLong.empty(), r.getLimit());
        Assertions.assertEquals(Optional.empty(), r.getVersion());
        Assertions.assertEquals(Optional.empty(), r.getRefSpec());
        Assertions.assertEquals(Optional.empty(), r.getMvccSnapshot());
    }

    @Test
    void builderFullySpecified() {
        List<ConnectorColumnHandle> cols = Arrays.asList(new StubColumn("a"), new StubColumn("b"));
        ConnectorRefSpec ref = ConnectorRefSpec.builder()
                .name("main").kind(RefKind.BRANCH).build();
        ConnectorScanRequest r = ConnectorScanRequest.builder()
                .session(new StubSession())
                .table(new StubHandle("t"))
                .columns(cols)
                .filter(Optional.empty())
                .limit(OptionalLong.of(100L))
                .version(Optional.of(new ConnectorTableVersion.BySnapshotId(42L)))
                .refSpec(Optional.of(ref))
                .mvccSnapshot(Optional.of(new StubMvcc()))
                .build();
        Assertions.assertEquals(2, r.getColumns().size());
        Assertions.assertEquals(OptionalLong.of(100L), r.getLimit());
        Assertions.assertTrue(r.getVersion().get() instanceof ConnectorTableVersion.BySnapshotId);
        Assertions.assertEquals("main", r.getRefSpec().get().name());
        Assertions.assertTrue(r.getMvccSnapshot().get() instanceof StubMvcc);
    }

    @Test
    void equalsAndHashCodeSymmetric() {
        StubSession sess = new StubSession();
        StubHandle handle = new StubHandle("t");
        ConnectorScanRequest a = ConnectorScanRequest.builder()
                .session(sess).table(handle).build();
        ConnectorScanRequest b = ConnectorScanRequest.builder()
                .session(sess).table(handle).build();
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(b, a);
        Assertions.assertEquals(a.hashCode(), b.hashCode());

        ConnectorScanRequest c = ConnectorScanRequest.builder()
                .session(sess).table(handle).limit(OptionalLong.of(1L)).build();
        Assertions.assertNotEquals(a, c);
    }

    @Test
    void serializableRoundTrip() throws IOException, ClassNotFoundException {
        ConnectorScanRequest r = ConnectorScanRequest.builder()
                .session(new StubSession())
                .table(new StubHandle("t"))
                .columns(Collections.singletonList(new StubColumn("a")))
                .limit(OptionalLong.of(50L))
                .mvccSnapshot(Optional.of(new StubMvcc()))
                .build();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(baos)) {
            out.writeObject(r);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
            ConnectorScanRequest back = (ConnectorScanRequest) in.readObject();
            Assertions.assertEquals(r, back);
        }
    }

    @Test
    void fromLegacyShortcut() {
        StubSession sess = new StubSession();
        StubHandle handle = new StubHandle("t");
        List<ConnectorColumnHandle> cols = Collections.singletonList(new StubColumn("c"));
        ConnectorScanRequest r = ConnectorScanRequest.from(sess, handle, cols, Optional.empty());
        Assertions.assertSame(sess, r.getSession());
        Assertions.assertSame(handle, r.getTable());
        Assertions.assertEquals(cols, r.getColumns());
        Assertions.assertEquals(OptionalLong.empty(), r.getLimit());
        Assertions.assertEquals(Optional.empty(), r.getVersion());
    }

    @Test
    void columnsListIsImmutable() {
        ConnectorScanRequest r = ConnectorScanRequest.builder()
                .session(new StubSession())
                .table(new StubHandle("t"))
                .columns(Collections.singletonList(new StubColumn("c")))
                .build();
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> r.getColumns().add(new StubColumn("x")));
    }

    @Test
    void requireNonNullTableButSessionMayBeNull() {
        Assertions.assertThrows(NullPointerException.class,
                () -> ConnectorScanRequest.builder().session(new StubSession()).build());
        // session may be null — legacy planScan entry points accept it.
        ConnectorScanRequest r = ConnectorScanRequest.builder()
                .table(new StubHandle("t")).build();
        Assertions.assertNull(r.getSession());
    }
}
