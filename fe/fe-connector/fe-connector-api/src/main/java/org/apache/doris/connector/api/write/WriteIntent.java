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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Describes the high-level intent of a write operation (D1).
 *
 * <p>Carries overwrite semantics, upsert flag, optional target branch,
 * optional static partition predicate, delete strategy, and an optional
 * time-travel "write at" version. Immutable; build via {@link Builder}.</p>
 */
public final class WriteIntent {

    /** Overwrite mode for the write. */
    public enum OverwriteMode {
        /** Append / merge — do not remove existing rows. */
        NONE,
        /** INSERT OVERWRITE of a fixed set of partitions (see {@code staticPartitions}). */
        STATIC_PARTITION,
        /** INSERT OVERWRITE of whichever partitions the incoming rows touch. */
        DYNAMIC_PARTITION,
        /** INSERT OVERWRITE of the entire table. */
        FULL_TABLE
    }

    private static final WriteIntent SIMPLE = new Builder().build();

    private final OverwriteMode overwriteMode;
    private final boolean upsert;
    private final Optional<String> branch;
    private final Map<String, String> staticPartitions;
    private final DeleteMode deleteMode;
    private final Optional<ConnectorTableVersion> writeAtVersion;

    private WriteIntent(
            OverwriteMode overwriteMode,
            boolean upsert,
            Optional<String> branch,
            Map<String, String> staticPartitions,
            DeleteMode deleteMode,
            Optional<ConnectorTableVersion> writeAtVersion) {
        this.overwriteMode = overwriteMode;
        this.upsert = upsert;
        this.branch = branch;
        this.staticPartitions = staticPartitions;
        this.deleteMode = deleteMode;
        this.writeAtVersion = writeAtVersion;
    }

    /** Returns the overwrite mode (never null). */
    public OverwriteMode overwriteMode() {
        return overwriteMode;
    }

    /** Returns whether this write is an upsert. */
    public boolean isUpsert() {
        return upsert;
    }

    /** Returns the optional target branch name. */
    public Optional<String> branch() {
        return branch;
    }

    /** Returns the immutable static-partition predicate (possibly empty). */
    public Map<String, String> staticPartitions() {
        return staticPartitions;
    }

    /** Returns the delete strategy (never null). */
    public DeleteMode deleteMode() {
        return deleteMode;
    }

    /** Returns the optional time-travel "write at" version. */
    public Optional<ConnectorTableVersion> writeAtVersion() {
        return writeAtVersion;
    }

    /**
     * Returns the canonical "simple" intent:
     * {@code NONE} / not upsert / no branch / empty static partitions /
     * {@code COW} / no version. Cached singleton.
     */
    public static WriteIntent simple() {
        return SIMPLE;
    }

    /** Returns a new {@link Builder}. */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof WriteIntent)) {
            return false;
        }
        WriteIntent that = (WriteIntent) o;
        return upsert == that.upsert
                && overwriteMode == that.overwriteMode
                && branch.equals(that.branch)
                && staticPartitions.equals(that.staticPartitions)
                && deleteMode == that.deleteMode
                && writeAtVersion.equals(that.writeAtVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(overwriteMode, upsert, branch, staticPartitions, deleteMode, writeAtVersion);
    }

    @Override
    public String toString() {
        return "WriteIntent{"
                + "overwriteMode=" + overwriteMode
                + ", upsert=" + upsert
                + ", branch=" + branch
                + ", staticPartitions=" + staticPartitions
                + ", deleteMode=" + deleteMode
                + ", writeAtVersion=" + writeAtVersion
                + '}';
    }

    /** Builder for {@link WriteIntent}. */
    public static final class Builder {
        private OverwriteMode overwriteMode = OverwriteMode.NONE;
        private boolean upsert = false;
        private Optional<String> branch = Optional.empty();
        private Map<String, String> staticPartitions = Collections.emptyMap();
        private DeleteMode deleteMode = DeleteMode.COW;
        private Optional<ConnectorTableVersion> writeAtVersion = Optional.empty();

        public Builder overwriteMode(OverwriteMode mode) {
            this.overwriteMode = Objects.requireNonNull(mode, "overwriteMode");
            return this;
        }

        public Builder upsert(boolean upsert) {
            this.upsert = upsert;
            return this;
        }

        public Builder branch(String branch) {
            this.branch = Optional.ofNullable(branch);
            return this;
        }

        public Builder staticPartitions(Map<String, String> staticPartitions) {
            Objects.requireNonNull(staticPartitions, "staticPartitions");
            if (staticPartitions.isEmpty()) {
                this.staticPartitions = Collections.emptyMap();
            } else {
                this.staticPartitions = Collections.unmodifiableMap(new LinkedHashMap<>(staticPartitions));
            }
            return this;
        }

        public Builder deleteMode(DeleteMode deleteMode) {
            this.deleteMode = Objects.requireNonNull(deleteMode, "deleteMode");
            return this;
        }

        public Builder writeAtVersion(ConnectorTableVersion version) {
            this.writeAtVersion = Optional.ofNullable(version);
            return this;
        }

        public WriteIntent build() {
            Objects.requireNonNull(overwriteMode, "overwriteMode");
            Objects.requireNonNull(deleteMode, "deleteMode");
            Objects.requireNonNull(branch, "branch");
            Objects.requireNonNull(staticPartitions, "staticPartitions");
            Objects.requireNonNull(writeAtVersion, "writeAtVersion");
            if (overwriteMode == OverwriteMode.STATIC_PARTITION && staticPartitions.isEmpty()) {
                throw new IllegalStateException("STATIC_PARTITION requires staticPartitions");
            }
            if (overwriteMode != OverwriteMode.STATIC_PARTITION && !staticPartitions.isEmpty()) {
                throw new IllegalStateException(
                        "staticPartitions may only be set when overwriteMode == STATIC_PARTITION");
            }
            return new WriteIntent(overwriteMode, upsert, branch, staticPartitions, deleteMode, writeAtVersion);
        }
    }
}
