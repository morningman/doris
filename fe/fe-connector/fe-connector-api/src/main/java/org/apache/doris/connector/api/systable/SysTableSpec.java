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

package org.apache.doris.connector.api.systable;

import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.cache.RefreshPolicy;

import java.util.Objects;
import java.util.Optional;

/**
 * Immutable specification of a single system / metadata table exposed by
 * a connector (e.g. {@code $snapshots}, {@code $partitions}, {@code $files}).
 *
 * <p>Produced by {@link SystemTableOps#listSysTables} / {@link SystemTableOps#getSysTable}.
 * The spec carries three optional execution carriers:</p>
 * <ul>
 *   <li>{@link #nativeFactory()}   — required when {@link #mode()} is
 *       {@link SysTableExecutionMode#NATIVE}.</li>
 *   <li>{@link #tvfInvoker()}      — required when {@link #mode()} is
 *       {@link SysTableExecutionMode#TVF}.</li>
 *   <li>{@link #scanPlanProviderRef()} — required when {@link #mode()} is
 *       {@link SysTableExecutionMode#CONNECTOR_SCAN_PLAN}.</li>
 * </ul>
 *
 * <p>{@link #refreshPolicy()} reuses the D3 cache enum. Sys tables should
 * typically use {@link RefreshPolicy#ALWAYS_REFRESH} — the default via the
 * builder — to avoid entries sitting stale in the binding cache.</p>
 *
 * <p>{@link #acceptsTableVersion()} links to D5 time-travel: when true,
 * fe-core forwards a {@code ConnectorTableVersion} to the selected
 * execution carrier at plan time.</p>
 */
public final class SysTableSpec {

    private final String name;
    private final ConnectorTableSchema schema;
    private final SysTableExecutionMode mode;
    private final boolean dataTable;
    private final RefreshPolicy refreshPolicy;
    private final boolean acceptsTableVersion;
    private final Optional<NativeSysTableScanFactory> nativeFactory;
    private final Optional<TvfSysTableInvoker> tvfInvoker;
    private final Optional<ConnectorScanPlanProviderRef> scanPlanProviderRef;

    private SysTableSpec(Builder b) {
        this.name = Objects.requireNonNull(b.name, "name");
        if (b.name.isEmpty()) {
            throw new IllegalArgumentException("name must not be empty");
        }
        this.schema = Objects.requireNonNull(b.schema, "schema");
        this.mode = Objects.requireNonNull(b.mode, "mode");
        this.dataTable = b.dataTable;
        this.refreshPolicy = Objects.requireNonNull(b.refreshPolicy, "refreshPolicy");
        this.acceptsTableVersion = b.acceptsTableVersion;
        this.nativeFactory = Objects.requireNonNull(b.nativeFactory, "nativeFactory");
        this.tvfInvoker = Objects.requireNonNull(b.tvfInvoker, "tvfInvoker");
        this.scanPlanProviderRef = Objects.requireNonNull(b.scanPlanProviderRef, "scanPlanProviderRef");

        switch (mode) {
            case NATIVE:
                if (nativeFactory.isEmpty()) {
                    throw new IllegalArgumentException(
                            "nativeFactory is required when mode == NATIVE");
                }
                break;
            case TVF:
                if (tvfInvoker.isEmpty()) {
                    throw new IllegalArgumentException(
                            "tvfInvoker is required when mode == TVF");
                }
                break;
            case CONNECTOR_SCAN_PLAN:
                if (scanPlanProviderRef.isEmpty()) {
                    throw new IllegalArgumentException(
                            "scanPlanProviderRef is required when mode == CONNECTOR_SCAN_PLAN");
                }
                break;
            default:
                throw new IllegalArgumentException("unknown mode: " + mode);
        }
    }

    /** Sys table name without the leading {@code '$'} (e.g. {@code "snapshots"}). */
    public String name() {
        return name;
    }

    /** Declared output schema of the sys table. */
    public ConnectorTableSchema schema() {
        return schema;
    }

    /** Which execution path fe-core should take. */
    public SysTableExecutionMode mode() {
        return mode;
    }

    /**
     * Whether this "sys table" actually carries data rows of the main table
     * (e.g. paimon {@code $ro}, iceberg {@code $all_data_files}) rather than
     * purely metadata rows.
     */
    public boolean dataTable() {
        return dataTable;
    }

    /** D3 refresh policy for the binding cache entry of this sys table. */
    public RefreshPolicy refreshPolicy() {
        return refreshPolicy;
    }

    /** Whether fe-core should forward a D5 {@code ConnectorTableVersion} to the execution carrier. */
    public boolean acceptsTableVersion() {
        return acceptsTableVersion;
    }

    public Optional<NativeSysTableScanFactory> nativeFactory() {
        return nativeFactory;
    }

    public Optional<TvfSysTableInvoker> tvfInvoker() {
        return tvfInvoker;
    }

    public Optional<ConnectorScanPlanProviderRef> scanPlanProviderRef() {
        return scanPlanProviderRef;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SysTableSpec)) {
            return false;
        }
        SysTableSpec that = (SysTableSpec) o;
        return dataTable == that.dataTable
                && acceptsTableVersion == that.acceptsTableVersion
                && name.equals(that.name)
                && schema.equals(that.schema)
                && mode == that.mode
                && refreshPolicy == that.refreshPolicy
                && nativeFactory.equals(that.nativeFactory)
                && tvfInvoker.equals(that.tvfInvoker)
                && scanPlanProviderRef.equals(that.scanPlanProviderRef);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, schema, mode, dataTable, refreshPolicy, acceptsTableVersion,
                nativeFactory, tvfInvoker, scanPlanProviderRef);
    }

    @Override
    public String toString() {
        return "SysTableSpec{name=" + name + ", mode=" + mode
                + ", refreshPolicy=" + refreshPolicy
                + ", dataTable=" + dataTable
                + ", acceptsTableVersion=" + acceptsTableVersion + "}";
    }

    /** Mutable builder; all non-primitive setters forbid {@code null}. */
    public static final class Builder {
        private String name;
        private ConnectorTableSchema schema;
        private SysTableExecutionMode mode;
        private boolean dataTable;
        private RefreshPolicy refreshPolicy = RefreshPolicy.ALWAYS_REFRESH;
        private boolean acceptsTableVersion;
        private Optional<NativeSysTableScanFactory> nativeFactory = Optional.empty();
        private Optional<TvfSysTableInvoker> tvfInvoker = Optional.empty();
        private Optional<ConnectorScanPlanProviderRef> scanPlanProviderRef = Optional.empty();

        private Builder() {
        }

        public Builder name(String name) {
            this.name = Objects.requireNonNull(name, "name");
            return this;
        }

        public Builder schema(ConnectorTableSchema schema) {
            this.schema = Objects.requireNonNull(schema, "schema");
            return this;
        }

        public Builder mode(SysTableExecutionMode mode) {
            this.mode = Objects.requireNonNull(mode, "mode");
            return this;
        }

        public Builder dataTable(boolean dataTable) {
            this.dataTable = dataTable;
            return this;
        }

        public Builder refreshPolicy(RefreshPolicy refreshPolicy) {
            this.refreshPolicy = Objects.requireNonNull(refreshPolicy, "refreshPolicy");
            return this;
        }

        public Builder acceptsTableVersion(boolean acceptsTableVersion) {
            this.acceptsTableVersion = acceptsTableVersion;
            return this;
        }

        public Builder nativeFactory(NativeSysTableScanFactory factory) {
            this.nativeFactory = Optional.of(Objects.requireNonNull(factory, "factory"));
            return this;
        }

        public Builder tvfInvoker(TvfSysTableInvoker invoker) {
            this.tvfInvoker = Optional.of(Objects.requireNonNull(invoker, "invoker"));
            return this;
        }

        public Builder scanPlanProviderRef(ConnectorScanPlanProviderRef ref) {
            this.scanPlanProviderRef = Optional.of(Objects.requireNonNull(ref, "ref"));
            return this;
        }

        public SysTableSpec build() {
            return new SysTableSpec(this);
        }
    }
}
