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
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.timetravel.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.timetravel.ConnectorRefSpec;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Aggregating parameter object passed by the engine to a
 * {@link ConnectorScanPlanProvider} when planning a scan. Bundles every
 * coordinate the engine has resolved at planning time (session, table
 * handle, projection, predicate, optional row limit, and any time-travel
 * coordinates: {@link ConnectorTableVersion}, {@link ConnectorRefSpec},
 * {@link ConnectorMvccSnapshot}).
 *
 * <p>This object is the sole input to
 * {@link ConnectorScanPlanProvider#planScan(ConnectorScanRequest)}. The
 * engine fills the bundle once at planning time and the connector reads
 * whichever fields it needs; missing optional coordinates surface as
 * {@link Optional#empty()} (or {@link OptionalLong#empty()} for the row
 * limit) so connectors that ignore them keep working unchanged.</p>
 */
public final class ConnectorScanRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ConnectorSession session;
    private final ConnectorTableHandle table;
    private final List<ConnectorColumnHandle> columns;
    // Optionals are intentionally not stored directly — java.util.Optional
    // is not Serializable, so we hold the unwrapped values (possibly null
    // / OptionalLong-as-long+flag) and re-wrap on access.
    private final ConnectorExpression filter;
    private final boolean limitPresent;
    private final long limit;
    private final ConnectorTableVersion version;
    private final ConnectorRefSpec refSpec;
    private final ConnectorMvccSnapshot mvccSnapshot;

    private ConnectorScanRequest(Builder b) {
        // session may be null; legacy planScan(session, ...) entry points
        // already accept a null session for tests/internal scans without
        // a user session, and the request shape must not be stricter.
        this.session = b.session;
        this.table = Objects.requireNonNull(b.table, "table");
        this.columns = Collections.unmodifiableList(Objects.requireNonNull(b.columns, "columns"));
        this.filter = Objects.requireNonNull(b.filter, "filter").orElse(null);
        OptionalLong lim = Objects.requireNonNull(b.limit, "limit");
        this.limitPresent = lim.isPresent();
        this.limit = lim.orElse(0L);
        this.version = Objects.requireNonNull(b.version, "version").orElse(null);
        this.refSpec = Objects.requireNonNull(b.refSpec, "refSpec").orElse(null);
        this.mvccSnapshot = Objects.requireNonNull(b.mvccSnapshot, "mvccSnapshot").orElse(null);
    }

    public ConnectorSession getSession() {
        return session;
    }

    public ConnectorTableHandle getTable() {
        return table;
    }

    public List<ConnectorColumnHandle> getColumns() {
        return columns;
    }

    public Optional<ConnectorExpression> getFilter() {
        return Optional.ofNullable(filter);
    }

    public OptionalLong getLimit() {
        return limitPresent ? OptionalLong.of(limit) : OptionalLong.empty();
    }

    public Optional<ConnectorTableVersion> getVersion() {
        return Optional.ofNullable(version);
    }

    public Optional<ConnectorRefSpec> getRefSpec() {
        return Optional.ofNullable(refSpec);
    }

    public Optional<ConnectorMvccSnapshot> getMvccSnapshot() {
        return Optional.ofNullable(mvccSnapshot);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Shortcut producing a request mirroring the legacy 4-arg
     * {@code planScan(session, handle, columns, filter)} inputs with all
     * time-travel fields empty and no row limit. Useful for tests and
     * for engine call sites that have not yet plumbed time-travel
     * coordinates into the scan node.
     */
    public static ConnectorScanRequest from(ConnectorSession session,
                                            ConnectorTableHandle table,
                                            List<ConnectorColumnHandle> columns,
                                            Optional<ConnectorExpression> filter) {
        return builder()
                .session(session)
                .table(table)
                .columns(columns)
                .filter(filter)
                .build();
    }

    /**
     * Convenience constructor mirroring the legacy 5-arg
     * {@code planScan(session, handle, columns, filter, limit)} inputs.
     * A {@code limit} of {@code -1} or any non-positive value is treated
     * as "no limit" and surfaces as {@link OptionalLong#empty()}; positive
     * values become {@link OptionalLong#of(long)}.
     */
    public static ConnectorScanRequest of(ConnectorSession session,
                                          ConnectorTableHandle table,
                                          List<ConnectorColumnHandle> columns,
                                          Optional<ConnectorExpression> filter,
                                          long limit) {
        OptionalLong wrapped = limit > 0 ? OptionalLong.of(limit) : OptionalLong.empty();
        return builder()
                .session(session)
                .table(table)
                .columns(columns)
                .filter(filter)
                .limit(wrapped)
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorScanRequest)) {
            return false;
        }
        ConnectorScanRequest that = (ConnectorScanRequest) o;
        return limitPresent == that.limitPresent
                && limit == that.limit
                && Objects.equals(session, that.session)
                && table.equals(that.table)
                && columns.equals(that.columns)
                && Objects.equals(filter, that.filter)
                && Objects.equals(version, that.version)
                && Objects.equals(refSpec, that.refSpec)
                && Objects.equals(mvccSnapshot, that.mvccSnapshot);
    }

    @Override
    public int hashCode() {
        return Objects.hash(session, table, columns, filter,
                limitPresent, limit, version, refSpec, mvccSnapshot);
    }

    @Override
    public String toString() {
        return "ConnectorScanRequest{table=" + table
                + ", columns=" + columns.size()
                + ", filter=" + (filter != null)
                + ", limit=" + getLimit()
                + ", version=" + getVersion()
                + ", refSpec=" + getRefSpec()
                + ", mvccSnapshot=" + getMvccSnapshot()
                + '}';
    }

    /** Builder for {@link ConnectorScanRequest}. */
    public static final class Builder {
        private ConnectorSession session;
        private ConnectorTableHandle table;
        private List<ConnectorColumnHandle> columns = Collections.emptyList();
        private Optional<ConnectorExpression> filter = Optional.empty();
        private OptionalLong limit = OptionalLong.empty();
        private Optional<ConnectorTableVersion> version = Optional.empty();
        private Optional<ConnectorRefSpec> refSpec = Optional.empty();
        private Optional<ConnectorMvccSnapshot> mvccSnapshot = Optional.empty();

        private Builder() {
        }

        public Builder session(ConnectorSession session) {
            this.session = session;
            return this;
        }

        public Builder table(ConnectorTableHandle table) {
            this.table = table;
            return this;
        }

        public Builder columns(List<ConnectorColumnHandle> columns) {
            this.columns = columns == null ? Collections.emptyList() : columns;
            return this;
        }

        public Builder filter(Optional<ConnectorExpression> filter) {
            this.filter = filter == null ? Optional.empty() : filter;
            return this;
        }

        public Builder limit(OptionalLong limit) {
            this.limit = limit == null ? OptionalLong.empty() : limit;
            return this;
        }

        public Builder version(Optional<ConnectorTableVersion> version) {
            this.version = version == null ? Optional.empty() : version;
            return this;
        }

        public Builder refSpec(Optional<ConnectorRefSpec> refSpec) {
            this.refSpec = refSpec == null ? Optional.empty() : refSpec;
            return this;
        }

        public Builder mvccSnapshot(Optional<ConnectorMvccSnapshot> mvccSnapshot) {
            this.mvccSnapshot = mvccSnapshot == null ? Optional.empty() : mvccSnapshot;
            return this;
        }

        public ConnectorScanRequest build() {
            return new ConnectorScanRequest(this);
        }
    }
}
