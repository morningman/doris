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
 * Immutable payload passed to {@link RefOps#createOrReplaceRef} describing the
 * desired branch / tag mutation.
 */
public final class ConnectorRefMutation {

    private final String name;
    private final RefKind kind;
    private final Optional<Long> fromSnapshot;
    private final Optional<Long> retentionMs;
    private final boolean replaceIfExists;

    private ConnectorRefMutation(Builder b) {
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
        this.fromSnapshot = b.fromSnapshot;
        this.retentionMs = b.retentionMs;
        this.replaceIfExists = b.replaceIfExists;
    }

    public String name() {
        return name;
    }

    public RefKind kind() {
        return kind;
    }

    public Optional<Long> fromSnapshot() {
        return fromSnapshot;
    }

    public Optional<Long> retentionMs() {
        return retentionMs;
    }

    public boolean replaceIfExists() {
        return replaceIfExists;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorRefMutation)) {
            return false;
        }
        ConnectorRefMutation that = (ConnectorRefMutation) o;
        return replaceIfExists == that.replaceIfExists
                && name.equals(that.name)
                && kind == that.kind
                && fromSnapshot.equals(that.fromSnapshot)
                && retentionMs.equals(that.retentionMs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, kind, fromSnapshot, retentionMs, replaceIfExists);
    }

    @Override
    public String toString() {
        return "ConnectorRefMutation{name=" + name
                + ", kind=" + kind
                + ", fromSnapshot=" + fromSnapshot.orElse(null)
                + ", retentionMs=" + retentionMs.orElse(null)
                + ", replaceIfExists=" + replaceIfExists
                + '}';
    }

    /** Builder for {@link ConnectorRefMutation}. */
    public static final class Builder {
        private String name;
        private RefKind kind;
        private Optional<Long> fromSnapshot = Optional.empty();
        private Optional<Long> retentionMs = Optional.empty();
        private boolean replaceIfExists;

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

        public Builder fromSnapshot(Long fromSnapshot) {
            this.fromSnapshot = Optional.ofNullable(fromSnapshot);
            return this;
        }

        public Builder retentionMs(Long retentionMs) {
            this.retentionMs = Optional.ofNullable(retentionMs);
            return this;
        }

        public Builder replaceIfExists(boolean replaceIfExists) {
            this.replaceIfExists = replaceIfExists;
            return this;
        }

        public ConnectorRefMutation build() {
            return new ConnectorRefMutation(this);
        }
    }
}
