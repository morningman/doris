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

package org.apache.doris.connector.api.write;

import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;

import java.util.Objects;
import java.util.Optional;

/**
 * Result of a successful commit in the future transaction-context-based
 * write path (D1, design doc §8.1).
 *
 * <p>Reserved for the future refactor PR; no {@code ConnectorWriteOps}
 * method returns it yet.</p>
 */
public final class CommitOutcome {

    private final Optional<ConnectorTableVersion> newVersion;
    private final long writtenRows;
    private final long writtenBytes;

    private CommitOutcome(Optional<ConnectorTableVersion> newVersion, long writtenRows, long writtenBytes) {
        this.newVersion = newVersion;
        this.writtenRows = writtenRows;
        this.writtenBytes = writtenBytes;
    }

    /** Returns the new table version produced by the commit, if any. */
    public Optional<ConnectorTableVersion> newVersion() {
        return newVersion;
    }

    /** Returns the total number of rows written (&ge; 0). */
    public long writtenRows() {
        return writtenRows;
    }

    /** Returns the total number of bytes written (&ge; 0). */
    public long writtenBytes() {
        return writtenBytes;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CommitOutcome)) {
            return false;
        }
        CommitOutcome that = (CommitOutcome) o;
        return writtenRows == that.writtenRows
                && writtenBytes == that.writtenBytes
                && newVersion.equals(that.newVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(newVersion, writtenRows, writtenBytes);
    }

    @Override
    public String toString() {
        return "CommitOutcome{"
                + "newVersion=" + newVersion
                + ", writtenRows=" + writtenRows
                + ", writtenBytes=" + writtenBytes
                + '}';
    }

    /** Builder for {@link CommitOutcome}. */
    public static final class Builder {
        private Optional<ConnectorTableVersion> newVersion = Optional.empty();
        private long writtenRows = 0L;
        private long writtenBytes = 0L;

        public Builder newVersion(ConnectorTableVersion version) {
            this.newVersion = Optional.ofNullable(version);
            return this;
        }

        public Builder writtenRows(long rows) {
            this.writtenRows = rows;
            return this;
        }

        public Builder writtenBytes(long bytes) {
            this.writtenBytes = bytes;
            return this;
        }

        public CommitOutcome build() {
            if (writtenRows < 0) {
                throw new IllegalArgumentException("writtenRows must be >= 0");
            }
            if (writtenBytes < 0) {
                throw new IllegalArgumentException("writtenBytes must be >= 0");
            }
            return new CommitOutcome(newVersion, writtenRows, writtenBytes);
        }
    }
}
