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
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;
import org.apache.doris.connector.api.timetravel.RefKind;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Bridges the legacy Nereids {@link TableSnapshot} AST node (currently produced by the
 * grammar rules {@code FOR VERSION AS OF} / {@code FOR TIME AS OF}) into a value of the
 * D5 SPI sealed type {@link ConnectorTableVersion}.
 *
 * <p>The resolver is purely additive: legacy code paths still consume the original
 * {@link TableSnapshot}; this resolver produces the SPI-friendly counterpart so that
 * connector plugins (M1-07 / M1-08 onwards) can read time-travel intent from a typed
 * sealed hierarchy without re-parsing strings.</p>
 *
 * <p>Timestamp parsing accepts an explicit zone (typically the session zone via
 * {@link TimeUtils#getDorisZoneId()}) and the following non-ambiguous formats:</p>
 * <ul>
 *     <li>{@code yyyy-MM-dd HH:mm:ss}</li>
 *     <li>{@code yyyy-MM-dd HH:mm:ss.SSS}</li>
 *     <li>ISO local date-time {@code yyyy-MM-ddTHH:mm:ss[.SSS]}</li>
 * </ul>
 * Any other input is rejected with {@link IllegalArgumentException}.
 */
public final class ConnectorTableVersionResolver {

    private static final DateTimeFormatter SQL_DATETIME =
            new DateTimeFormatterBuilder()
                    .appendPattern("yyyy-MM-dd HH:mm:ss")
                    .optionalStart()
                    .appendFraction(ChronoField.MILLI_OF_SECOND, 1, 3, true)
                    .optionalEnd()
                    .toFormatter();

    private static final DateTimeFormatter ISO_DATETIME =
            new DateTimeFormatterBuilder()
                    .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
                    .optionalStart()
                    .appendFraction(ChronoField.MILLI_OF_SECOND, 1, 3, true)
                    .optionalEnd()
                    .toFormatter();

    private static final List<DateTimeFormatter> TIMESTAMP_FORMATS =
            List.of(SQL_DATETIME, ISO_DATETIME);

    private ConnectorTableVersionResolver() {
    }

    /**
     * Resolve a legacy {@link TableSnapshot} produced by the Nereids parser.
     *
     * @param legacy legacy AST snapshot, may be {@code null}
     * @return {@link Optional#empty()} when {@code legacy} is {@code null}, otherwise
     *         the corresponding {@link ConnectorTableVersion} subtype
     * @throws IllegalArgumentException if a timestamp / version literal cannot be parsed
     */
    public static Optional<ConnectorTableVersion> resolve(TableSnapshot legacy) {
        return resolve(legacy, TimeUtils.getDorisZoneId());
    }

    /**
     * Resolve a legacy {@link TableSnapshot} using the supplied zone for timestamp parsing.
     */
    public static Optional<ConnectorTableVersion> resolve(TableSnapshot legacy, ZoneId zone) {
        if (legacy == null) {
            return Optional.empty();
        }
        Objects.requireNonNull(zone, "zone");
        switch (legacy.getType()) {
            case VERSION:
                return Optional.of(new ConnectorTableVersion.BySnapshotId(parseSnapshotId(legacy.getValue())));
            case TIME:
                return Optional.of(new ConnectorTableVersion.ByTimestamp(parseTimestamp(legacy.getValue(), zone)));
            default:
                throw new IllegalArgumentException(
                        "Unsupported legacy TableSnapshot type: " + legacy.getType());
        }
    }

    /**
     * Build a {@link ConnectorTableVersion.ByRef} for a named branch.
     *
     * <p>The current Nereids grammar does not expose {@code FOR BRANCH AS OF} / {@code FOR TAG AS OF};
     * this helper exists so that engine code wired up in a later parser PR (and unit tests covering
     * every sealed subtype) can construct branch-pinned versions without depending on the SPI module
     * directly.</p>
     */
    public static ConnectorTableVersion forBranch(String name) {
        return new ConnectorTableVersion.ByRef(requireNonBlank(name, "branch"), RefKind.BRANCH);
    }

    /** Build a {@link ConnectorTableVersion.ByRef} for a named tag. */
    public static ConnectorTableVersion forTag(String name) {
        return new ConnectorTableVersion.ByRef(requireNonBlank(name, "tag"), RefKind.TAG);
    }

    /** Build a {@link ConnectorTableVersion.ByRefAtTimestamp} pinning a branch at an instant. */
    public static ConnectorTableVersion forBranchAtTimestamp(String name, Instant ts) {
        return new ConnectorTableVersion.ByRefAtTimestamp(
                requireNonBlank(name, "branch"), RefKind.BRANCH, Objects.requireNonNull(ts, "ts"));
    }

    /** Build a {@link ConnectorTableVersion.ByRefAtTimestamp} pinning a tag at an instant. */
    public static ConnectorTableVersion forTagAtTimestamp(String name, Instant ts) {
        return new ConnectorTableVersion.ByRefAtTimestamp(
                requireNonBlank(name, "tag"), RefKind.TAG, Objects.requireNonNull(ts, "ts"));
    }

    /**
     * Build a {@link ConnectorTableVersion.ByOpaque} for plugin-specific tokens (e.g. Hudi instant
     * times or engine-internal MVCC stamps).  This is the SPI hook exercising the
     * {@code MvccSnapshotVersion} slot in the M1-06 brief: the engine carries an opaque stamp end-to-end.
     */
    public static ConnectorTableVersion forOpaque(String token) {
        return new ConnectorTableVersion.ByOpaque(requireNonBlank(token, "token"));
    }

    private static long parseSnapshotId(String value) {
        Objects.requireNonNull(value, "value");
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid snapshot id literal: " + value, e);
        }
    }

    private static Instant parseTimestamp(String value, ZoneId zone) {
        Objects.requireNonNull(value, "value");
        String trimmed = value.trim();
        DateTimeParseException last = null;
        for (DateTimeFormatter fmt : TIMESTAMP_FORMATS) {
            try {
                LocalDateTime ldt = LocalDateTime.parse(trimmed, fmt);
                return ldt.atZone(zone).toInstant();
            } catch (DateTimeParseException e) {
                last = e;
            }
        }
        throw new IllegalArgumentException(
                "Invalid timestamp literal: '" + value + "' (expected 'yyyy-MM-dd HH:mm:ss[.SSS]'"
                        + " or ISO 'yyyy-MM-ddTHH:mm:ss[.SSS]')",
                last);
    }

    private static String requireNonBlank(String s, String field) {
        Objects.requireNonNull(s, field);
        if (s.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return s;
    }
}
