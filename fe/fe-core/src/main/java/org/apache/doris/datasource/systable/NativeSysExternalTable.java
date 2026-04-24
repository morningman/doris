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
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.systable.SysTableExecutionMode;
import org.apache.doris.connector.api.systable.SysTableSpec;
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
 * fe-core wrapper for a {@link SysTableExecutionMode#NATIVE} system table
 * exposed by a connector plugin via {@link SysTableSpec}.
 *
 * <p>Holds the plugin-supplied {@link SysTableSpec} and the original main
 * {@link ExternalTable}. Lazily materialises a
 * {@link ConnectorScanPlanProvider} via
 * {@link SysTableSpec#nativeFactory()} when fe-core asks for a scan plan.
 *
 * <p>Schema is derived from the spec's {@code ConnectorTableSchema} via
 * the existing {@link ConnectorColumnConverter}; no I/O happens at
 * construction time.
 */
public class NativeSysExternalTable extends ExternalTable {

    private final ExternalTable sourceTable;
    private final SysTableSpec spec;

    public NativeSysExternalTable(ExternalTable sourceTable, SysTableSpec spec) {
        super(generateSysTableId(sourceTable.getId(), spec.name()),
                sourceTable.getName() + "$" + spec.name(),
                sourceTable.getRemoteName() + "$" + spec.name(),
                (ExternalCatalog) sourceTable.getCatalog(),
                (ExternalDatabase) sourceTable.getDb(),
                TableIf.TableType.PLUGIN_EXTERNAL_TABLE);
        this.sourceTable = Objects.requireNonNull(sourceTable, "sourceTable");
        this.spec = Objects.requireNonNull(spec, "spec");
        if (spec.mode() != SysTableExecutionMode.NATIVE) {
            throw new IllegalArgumentException(
                    "NativeSysExternalTable requires mode=NATIVE but got " + spec.mode());
        }
    }

    public ExternalTable getSourceTable() {
        return sourceTable;
    }

    public SysTableSpec getSpec() {
        return spec;
    }

    /**
     * Materialises the scan-plan provider for this sys table using the
     * plugin-supplied factory. Forwarded {@code version} is dropped to
     * {@link Optional#empty()} when the spec opts out of time travel.
     */
    public ConnectorScanPlanProvider createScanPlanProvider(Optional<ConnectorTableVersion> version) {
        Objects.requireNonNull(version, "version");
        Optional<ConnectorTableVersion> effective = spec.acceptsTableVersion()
                ? version
                : Optional.empty();
        String dbRemote = sourceTable.getDb() != null ? sourceTable.getDb().getRemoteName() : "";
        return spec.nativeFactory()
                .orElseThrow(() -> new IllegalStateException(
                        "spec.nativeFactory missing for NATIVE sys table " + spec.name()))
                .create(dbRemote, sourceTable.getRemoteName(), spec.name(), effective);
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
