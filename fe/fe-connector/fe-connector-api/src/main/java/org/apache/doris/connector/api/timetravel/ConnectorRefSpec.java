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

import java.util.Objects;
import java.util.Optional;

/**
 * Immutable write-side ref payload describing how the engine wants a branch
 * or tag to be created/replaced.
 */
public final class ConnectorRefSpec {

    private final String name;
    private final RefKind kind;
    private final Optional<Long> snapshotId;
    private final Optional<Long> retentionMs;

    private ConnectorRefSpec(Builder b) {
        if (b.name == null || b.name.isEmpty()) {
            throw new IllegalArgumentException("name is required");
        }
        if (b.kind == null) {
            throw new IllegalArgumentException("kind is required");
        }
        if (b.kind == RefKind.UNKNOWN) {
            throw new IllegalArgumentException("kind must be BRANCH or TAG");
        }
        this.name = b.name;
        this.kind = b.kind;
        this.snapshotId = b.snapshotId;
        this.retentionMs = b.retentionMs;
    }

    public String name() {
        return name;
    }

    public RefKind kind() {
        return kind;
    }

    public Optional<Long> snapshotId() {
        return snapshotId;
    }

    public Optional<Long> retentionMs() {
        return retentionMs;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorRefSpec)) {
            return false;
        }
        ConnectorRefSpec that = (ConnectorRefSpec) o;
        return name.equals(that.name)
                && kind == that.kind
                && snapshotId.equals(that.snapshotId)
                && retentionMs.equals(that.retentionMs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, kind, snapshotId, retentionMs);
    }

    @Override
    public String toString() {
        return "ConnectorRefSpec{name=" + name
                + ", kind=" + kind
                + ", snapshotId=" + snapshotId.orElse(null)
                + ", retentionMs=" + retentionMs.orElse(null)
                + '}';
    }

    /** Builder for {@link ConnectorRefSpec}. */
    public static final class Builder {
        private String name;
        private RefKind kind;
        private Optional<Long> snapshotId = Optional.empty();
        private Optional<Long> retentionMs = Optional.empty();

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

        public Builder snapshotId(Long snapshotId) {
            this.snapshotId = Optional.ofNullable(snapshotId);
            return this;
        }

        public Builder retentionMs(Long retentionMs) {
            this.retentionMs = Optional.ofNullable(retentionMs);
            return this;
        }

        public ConnectorRefSpec build() {
            return new ConnectorRefSpec(this);
        }
    }
}
