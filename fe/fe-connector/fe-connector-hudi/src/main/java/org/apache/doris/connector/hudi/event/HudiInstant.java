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

package org.apache.doris.connector.hudi.event;

import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * Description of a single Hudi timeline instant (one file under
 * {@code .hoodie/}). Comparable by lexicographic timestamp ordering —
 * Hudi instant timestamps are zero-padded yyyymmddHHmmssSSS strings, so
 * lexicographic order coincides with chronological order.
 *
 * <p>Hudi has many action names; the SPI translator only branches on
 * {@link Action} below. Anything outside that set degrades to a
 * {@link Action#UNKNOWN} {@code VendorEvent} so a hudi-side new action
 * cannot silently drop a change on the floor.
 */
public final class HudiInstant implements Comparable<HudiInstant> {

    /** Hudi timeline action kinds the translator recognises. */
    public enum Action {
        COMMIT,
        DELTA_COMMIT,
        REPLACE_COMMIT,
        CLEAN,
        ROLLBACK,
        COMPACTION,
        SAVEPOINT,
        UNKNOWN;

        /** Map a hudi action string ({@code "commit"}, {@code "deltacommit"}, ...) to an {@link Action}. */
        public static Action fromHudiName(String name) {
            Objects.requireNonNull(name, "name");
            switch (name.toLowerCase(Locale.ROOT)) {
                case "commit":
                    return COMMIT;
                case "deltacommit":
                    return DELTA_COMMIT;
                case "replacecommit":
                    return REPLACE_COMMIT;
                case "clean":
                    return CLEAN;
                case "rollback":
                    return ROLLBACK;
                case "compaction":
                case "logcompaction":
                    return COMPACTION;
                case "savepoint":
                    return SAVEPOINT;
                default:
                    return UNKNOWN;
            }
        }
    }

    /** Hudi timeline state. The translator only emits events for {@link #COMPLETED}. */
    public enum State {
        REQUESTED,
        INFLIGHT,
        COMPLETED;

        public static State fromHudiName(String name) {
            Objects.requireNonNull(name, "name");
            switch (name.toUpperCase(Locale.ROOT)) {
                case "REQUESTED":
                    return REQUESTED;
                case "INFLIGHT":
                    return INFLIGHT;
                case "COMPLETED":
                    return COMPLETED;
                default:
                    throw new IllegalArgumentException("unknown hudi instant state: " + name);
            }
        }
    }

    private final String timestamp;
    private final Action action;
    private final String rawAction;
    private final State state;
    private final Optional<String> schemaHash;

    public HudiInstant(String timestamp, Action action, String rawAction, State state,
                       Optional<String> schemaHash) {
        this.timestamp = Objects.requireNonNull(timestamp, "timestamp");
        this.action = Objects.requireNonNull(action, "action");
        this.rawAction = Objects.requireNonNull(rawAction, "rawAction");
        this.state = Objects.requireNonNull(state, "state");
        this.schemaHash = Objects.requireNonNull(schemaHash, "schemaHash");
    }

    public static HudiInstant of(String timestamp, String rawAction, String state) {
        return new HudiInstant(timestamp, Action.fromHudiName(rawAction), rawAction,
                State.fromHudiName(state), Optional.empty());
    }

    public static HudiInstant completed(String timestamp, String rawAction) {
        return of(timestamp, rawAction, "COMPLETED");
    }

    public HudiInstant withSchemaHash(String hash) {
        Objects.requireNonNull(hash, "hash");
        return new HudiInstant(timestamp, action, rawAction, state, Optional.of(hash));
    }

    public String timestamp() {
        return timestamp;
    }

    public Action action() {
        return action;
    }

    public String rawAction() {
        return rawAction;
    }

    public State state() {
        return state;
    }

    public Optional<String> schemaHash() {
        return schemaHash;
    }

    @Override
    public int compareTo(HudiInstant other) {
        return this.timestamp.compareTo(other.timestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HudiInstant)) {
            return false;
        }
        HudiInstant that = (HudiInstant) o;
        return timestamp.equals(that.timestamp)
                && action == that.action
                && rawAction.equals(that.rawAction)
                && state == that.state
                && schemaHash.equals(that.schemaHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, action, rawAction, state, schemaHash);
    }

    @Override
    public String toString() {
        return "HudiInstant{" + timestamp + "/" + rawAction + "/" + state + "}";
    }
}
