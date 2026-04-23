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

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable read-side descriptor of a single ref (branch/tag) that a
 * connector currently exposes for a given table.
 */
public final class ConnectorRef {

    private final String name;
    private final RefKind kind;
    private final long snapshotId;
    private final Optional<Instant> createdAt;

    private ConnectorRef(Builder b) {
        if (b.name == null || b.name.isEmpty()) {
            throw new IllegalArgumentException("name is required");
        }
        if (b.kind == null) {
            throw new IllegalArgumentException("kind is required");
        }
        this.name = b.name;
        this.kind = b.kind;
        this.snapshotId = b.snapshotId;
        this.createdAt = b.createdAt;
    }

    public String name() {
        return name;
    }

    public RefKind kind() {
        return kind;
    }

    /** Snapshot id pointed at, or {@code -1} if unknown. */
    public long snapshotId() {
        return snapshotId;
    }

    public Optional<Instant> createdAt() {
        return createdAt;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorRef)) {
            return false;
        }
        ConnectorRef that = (ConnectorRef) o;
        return snapshotId == that.snapshotId
                && name.equals(that.name)
                && kind == that.kind
                && createdAt.equals(that.createdAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, kind, snapshotId, createdAt);
    }

    @Override
    public String toString() {
        return "ConnectorRef{name=" + name
                + ", kind=" + kind
                + ", snapshotId=" + snapshotId
                + ", createdAt=" + createdAt.orElse(null)
                + '}';
    }

    /** Builder for {@link ConnectorRef}. */
    public static final class Builder {
        private String name;
        private RefKind kind;
        private long snapshotId = -1L;
        private Optional<Instant> createdAt = Optional.empty();

        private Builder() {
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder kind(RefKind kind) {
            this.kind = kind;
            return this;
        }

        public Builder snapshotId(long snapshotId) {
            this.snapshotId = snapshotId;
            return this;
        }

        public Builder createdAt(Instant createdAt) {
            this.createdAt = Optional.ofNullable(createdAt);
            return this;
        }

        public ConnectorRef build() {
            return new ConnectorRef(this);
        }
    }
}
