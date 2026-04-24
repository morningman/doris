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

package org.apache.doris.datasource.systable;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.connector.api.systable.ConnectorScanPlanProviderRef;
import org.apache.doris.connector.api.systable.SysTableExecutionMode;
import org.apache.doris.connector.api.systable.SysTableSpec;
import org.apache.doris.connector.api.systable.TvfInvocation;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;
import org.apache.doris.datasource.ConnectorColumnConverter;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheKey;
import org.apache.doris.datasource.SchemaCacheValue;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * fe-core wrapper for a connector-plugin sys table whose execution mode
 * is either {@link SysTableExecutionMode#TVF} or
 * {@link SysTableExecutionMode#CONNECTOR_SCAN_PLAN}.
 *
 * <p>For {@link SysTableExecutionMode#TVF TVF} mode the wrapper resolves a
 * {@link TvfInvocation} via {@link SysTableSpec#tvfInvoker()}; downstream
 * planning translates that invocation into a fe-core metadata TVF call
 * (wired in M1-13/14/15).</p>
 *
 * <p>For {@link SysTableExecutionMode#CONNECTOR_SCAN_PLAN CONNECTOR_SCAN_PLAN}
 * mode the wrapper exposes the plugin-supplied
 * {@link ConnectorScanPlanProviderRef} so the engine can ask the same plugin
 * to materialise its own scan-plan provider at plan time.</p>
 */
public class ConnectorManagedSysExternalTable extends ExternalTable {

    private final ExternalTable sourceTable;
    private final SysTableSpec spec;

    public ConnectorManagedSysExternalTable(ExternalTable sourceTable, SysTableSpec spec) {
        super(generateSysTableId(sourceTable.getId(), spec.name()),
                sourceTable.getName() + "$" + spec.name(),
                sourceTable.getRemoteName() + "$" + spec.name(),
                (ExternalCatalog) sourceTable.getCatalog(),
                (ExternalDatabase) sourceTable.getDb(),
                TableIf.TableType.PLUGIN_EXTERNAL_TABLE);
        this.sourceTable = Objects.requireNonNull(sourceTable, "sourceTable");
        this.spec = Objects.requireNonNull(spec, "spec");
        SysTableExecutionMode mode = spec.mode();
        if (mode != SysTableExecutionMode.TVF && mode != SysTableExecutionMode.CONNECTOR_SCAN_PLAN) {
            throw new IllegalArgumentException(
                    "ConnectorManagedSysExternalTable requires mode=TVF or CONNECTOR_SCAN_PLAN but got " + mode);
        }
    }

    public ExternalTable getSourceTable() {
        return sourceTable;
    }

    public SysTableSpec getSpec() {
        return spec;
    }

    /**
     * Resolves the underlying TVF invocation. Only valid when
     * {@link SysTableSpec#mode()} is {@link SysTableExecutionMode#TVF}.
     */
    public TvfInvocation resolveTvfInvocation(Optional<ConnectorTableVersion> version) {
        Objects.requireNonNull(version, "version");
        if (spec.mode() != SysTableExecutionMode.TVF) {
            throw new IllegalStateException(
                    "resolveTvfInvocation called on non-TVF sys table " + spec.name()
                            + " (mode=" + spec.mode() + ")");
        }
        Optional<ConnectorTableVersion> effective = spec.acceptsTableVersion()
                ? version
                : Optional.empty();
        String dbRemote = sourceTable.getDb() != null ? sourceTable.getDb().getRemoteName() : "";
        return spec.tvfInvoker()
                .orElseThrow(() -> new IllegalStateException(
                        "spec.tvfInvoker missing for TVF sys table " + spec.name()))
                .resolve(dbRemote, sourceTable.getRemoteName(), spec.name(), effective);
    }

    /**
     * Returns the opaque plan-provider reference. Only valid when
     * {@link SysTableSpec#mode()} is {@link SysTableExecutionMode#CONNECTOR_SCAN_PLAN}.
     */
    public ConnectorScanPlanProviderRef getScanPlanProviderRef() {
        if (spec.mode() != SysTableExecutionMode.CONNECTOR_SCAN_PLAN) {
            throw new IllegalStateException(
                    "getScanPlanProviderRef called on non-CONNECTOR_SCAN_PLAN sys table "
                            + spec.name() + " (mode=" + spec.mode() + ")");
        }
        return spec.scanPlanProviderRef()
                .orElseThrow(() -> new IllegalStateException(
                        "spec.scanPlanProviderRef missing for CONNECTOR_SCAN_PLAN sys table "
                                + spec.name()));
    }

    @Override
    public Optional<SchemaCacheValue> initSchema(SchemaCacheKey key) {
        return Optional.of(buildSchemaCacheValue());
    }

    @Override
    public Optional<SchemaCacheValue> getSchemaCacheValue() {
        return Optional.of(buildSchemaCacheValue());
    }

    @Override
    public List<Column> getFullSchema() {
        return buildSchemaCacheValue().getSchema();
    }

    @Override
    public long fetchRowCount() {
        return UNKNOWN_ROW_COUNT;
    }

    @Override
    public String getComment() {
        return "Plugin system table: " + spec.name() + " for " + sourceTable.getName();
    }

    private SchemaCacheValue buildSchemaCacheValue() {
        List<Column> columns = ConnectorColumnConverter.convertColumns(spec.schema().getColumns());
        return new SchemaCacheValue(columns);
    }

    private static long generateSysTableId(long sourceTableId, String sysTableName) {
        return sourceTableId ^ (sysTableName.hashCode() * 31L);
    }
}
