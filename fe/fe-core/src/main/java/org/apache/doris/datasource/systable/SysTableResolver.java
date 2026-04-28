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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableId;
import org.apache.doris.connector.api.systable.SysTableExecutionMode;
import org.apache.doris.connector.api.systable.SysTableSpec;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.info.TableValuedFunctionRefInfo;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;

import com.google.common.base.Preconditions;

import java.util.Objects;
import java.util.Optional;

/**
 * Central resolver for external system tables.
 *
 * <p>Provides a single place to resolve system table types and build the
 * execution artifacts needed by planners or describe commands.
 */
public final class SysTableResolver {

    private SysTableResolver() {
    }

    public static Optional<SysTable> resolveType(TableIf table, String tableNameWithSysTableName) {
        return table.findSysTable(tableNameWithSysTableName);
    }

    /**
     * Resolve system table for planning (Nereids). TVF path only needs TableValuedFunction.
     */
    public static Optional<SysTablePlan> resolveForPlan(TableIf table, String ctlName, String dbName,
            String tableNameWithSysTableName) {
        Optional<SysTablePlan> pluginPlan = resolvePluginForPlan(table, tableNameWithSysTableName);
        if (pluginPlan.isPresent()) {
            return pluginPlan;
        }
        Optional<SysTable> typeOpt = resolveType(table, tableNameWithSysTableName);
        if (!typeOpt.isPresent()) {
            return Optional.empty();
        }
        SysTable type = typeOpt.get();
        if (type instanceof NativeSysTable && table instanceof ExternalTable) {
            ExternalTable sysExternalTable = ((NativeSysTable) type).createSysExternalTable((ExternalTable) table);
            return Optional.of(SysTablePlan.forNative(type, sysExternalTable));
        }
        if (type instanceof TvfSysTable) {
            TableValuedFunction tvf = ((TvfSysTable) type)
                    .createFunction(ctlName, dbName, tableNameWithSysTableName);
            return Optional.of(SysTablePlan.forTvf(type, tvf));
        }
        return Optional.empty();
    }

    /**
     * Resolve system table for DESCRIBE. TVF path needs TableValuedFunctionRefInfo.
     */
    public static Optional<SysTableDescribe> resolveForDescribe(TableIf table, String ctlName, String dbName,
            String tableNameWithSysTableName) {
        Optional<SysTableDescribe> pluginDescribe = resolvePluginForDescribe(table, tableNameWithSysTableName);
        if (pluginDescribe.isPresent()) {
            return pluginDescribe;
        }
        Optional<SysTable> typeOpt = resolveType(table, tableNameWithSysTableName);
        if (!typeOpt.isPresent()) {
            return Optional.empty();
        }
        SysTable type = typeOpt.get();
        if (type instanceof NativeSysTable && table instanceof ExternalTable) {
            ExternalTable sysExternalTable = ((NativeSysTable) type).createSysExternalTable((ExternalTable) table);
            return Optional.of(SysTableDescribe.forNative(type, sysExternalTable));
        }
        if (type instanceof TvfSysTable) {
            TableValuedFunctionRefInfo tvfRef = ((TvfSysTable) type)
                    .createFunctionRef(ctlName, dbName, tableNameWithSysTableName);
            return Optional.of(SysTableDescribe.forTvf(type, tvfRef));
        }
        return Optional.empty();
    }

    /**
     * Validate system table existence. For TVF path, ensures the TVF can be created.
     */
    public static boolean validateForQuery(TableIf table, String ctlName, String dbName,
            String tableNameWithSysTableName) {
        if (lookupPluginSpec(table, tableNameWithSysTableName).isPresent()) {
            return true;
        }
        Optional<SysTable> typeOpt = resolveType(table, tableNameWithSysTableName);
        if (!typeOpt.isPresent()) {
            return false;
        }
        SysTable type = typeOpt.get();
        if (!type.useNativeTablePath()) {
            if (!(type instanceof TvfSysTable)) {
                return false;
            }
            ((TvfSysTable) type).createFunction(ctlName, dbName, tableNameWithSysTableName);
        }
        return true;
    }

    // -----------------------------------------------------------------
    //  Plugin route (D6 / M1-12)
    // -----------------------------------------------------------------

    /**
     * Looks up a {@link SysTableSpec} from the plugin attached to {@code table}.
     * Returns empty when {@code table} is not a plugin-driven external table or
     * when the plugin does not publish a sys table for the requested name.
     */
    public static Optional<SysTableSpec> lookupPluginSpec(TableIf table, String tableNameWithSysTableName) {
        Objects.requireNonNull(tableNameWithSysTableName, "tableNameWithSysTableName");
        if (!(table instanceof ExternalTable)) {
            return Optional.empty();
        }
        ExternalTable extTable = (ExternalTable) table;
        ExternalCatalog catalog = extTable.getCatalog();
        if (!(catalog instanceof PluginDrivenExternalCatalog)) {
            return Optional.empty();
        }
        Pair<String, String> parsed = SysTable.getTableNameWithSysTableName(tableNameWithSysTableName);
        if (parsed.second.isEmpty()) {
            return Optional.empty();
        }
        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        Connector connector = pluginCatalog.getConnector();
        if (connector == null) {
            return Optional.empty();
        }
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);
        if (metadata == null) {
            return Optional.empty();
        }
        String dbRemote = extTable.getDb() != null ? extTable.getDb().getRemoteName() : "";
        return metadata.getSysTable(ConnectorTableId.of(dbRemote, parsed.first), parsed.second);
    }

    private static Optional<SysTablePlan> resolvePluginForPlan(TableIf table, String tableNameWithSysTableName) {
        Optional<SysTableSpec> specOpt = lookupPluginSpec(table, tableNameWithSysTableName);
        if (!specOpt.isPresent()) {
            return Optional.empty();
        }
        return Optional.of(SysTablePlan.forPlugin(buildPluginWrapper((ExternalTable) table, specOpt.get())));
    }

    private static Optional<SysTableDescribe> resolvePluginForDescribe(TableIf table,
            String tableNameWithSysTableName) {
        Optional<SysTableSpec> specOpt = lookupPluginSpec(table, tableNameWithSysTableName);
        if (!specOpt.isPresent()) {
            return Optional.empty();
        }
        return Optional.of(SysTableDescribe.forPlugin(buildPluginWrapper((ExternalTable) table, specOpt.get())));
    }

    private static ExternalTable buildPluginWrapper(ExternalTable sourceTable, SysTableSpec spec) {
        if (spec.mode() == SysTableExecutionMode.NATIVE) {
            return new NativeSysExternalTable(sourceTable, spec);
        }
        return new ConnectorManagedSysExternalTable(sourceTable, spec);
    }

    public static final class SysTablePlan {
        private final SysTable type;
        private final ExternalTable sysExternalTable;
        private final TableValuedFunction tvf;
        private final SysTableSpec spec;

        private SysTablePlan(SysTable type, ExternalTable sysExternalTable, TableValuedFunction tvf,
                SysTableSpec spec) {
            this.type = type;
            this.sysExternalTable = sysExternalTable;
            this.tvf = tvf;
            this.spec = spec;
        }

        public static SysTablePlan forNative(SysTable type, ExternalTable sysExternalTable) {
            Preconditions.checkNotNull(sysExternalTable, "sysExternalTable is null");
            return new SysTablePlan(type, sysExternalTable, null, null);
        }

        public static SysTablePlan forTvf(SysTable type, TableValuedFunction tvf) {
            Preconditions.checkNotNull(tvf, "tvf is null");
            return new SysTablePlan(type, null, tvf, null);
        }

        public static SysTablePlan forPlugin(ExternalTable wrapper) {
            Preconditions.checkNotNull(wrapper, "wrapper is null");
            SysTableSpec spec;
            if (wrapper instanceof NativeSysExternalTable) {
                spec = ((NativeSysExternalTable) wrapper).getSpec();
            } else if (wrapper instanceof ConnectorManagedSysExternalTable) {
                spec = ((ConnectorManagedSysExternalTable) wrapper).getSpec();
            } else {
                throw new IllegalArgumentException(
                        "forPlugin requires NativeSysExternalTable or ConnectorManagedSysExternalTable, got "
                                + wrapper.getClass().getSimpleName());
            }
            return new SysTablePlan(null, wrapper, null, spec);
        }

        public boolean isNative() {
            if (spec != null) {
                return spec.mode() == SysTableExecutionMode.NATIVE;
            }
            return sysExternalTable != null;
        }

        public boolean isPluginManaged() {
            return spec != null;
        }

        /**
         * True iff this plan is plugin-managed and the plugin spec declares
         * {@link SysTableExecutionMode#TVF TVF} execution mode. M1-15 hive
         * {@code $partitions} hits this branch; the BindRelation TVF path
         * handles it via {@link PluginTvfDispatcher}.
         */
        public boolean isPluginTvf() {
            return spec != null && spec.mode() == SysTableExecutionMode.TVF;
        }

        public SysTable getSysTable() {
            return type;
        }

        public Optional<SysTableSpec> getSysTableSpec() {
            return Optional.ofNullable(spec);
        }

        public ExternalTable getSysExternalTable() {
            Preconditions.checkState(sysExternalTable != null, "Not a sys-external-table plan");
            return sysExternalTable;
        }

        public TableValuedFunction getTvf() {
            Preconditions.checkState(tvf != null, "Not a TVF system table");
            return tvf;
        }
    }

    public static final class SysTableDescribe {
        private final SysTable type;
        private final ExternalTable sysExternalTable;
        private final TableValuedFunctionRefInfo tvfRef;
        private final SysTableSpec spec;

        private SysTableDescribe(SysTable type, ExternalTable sysExternalTable,
                TableValuedFunctionRefInfo tvfRef, SysTableSpec spec) {
            this.type = type;
            this.sysExternalTable = sysExternalTable;
            this.tvfRef = tvfRef;
            this.spec = spec;
        }

        public static SysTableDescribe forNative(SysTable type, ExternalTable sysExternalTable) {
            Preconditions.checkNotNull(sysExternalTable, "sysExternalTable is null");
            return new SysTableDescribe(type, sysExternalTable, null, null);
        }

        public static SysTableDescribe forTvf(SysTable type, TableValuedFunctionRefInfo tvfRef) {
            Preconditions.checkNotNull(tvfRef, "tvfRef is null");
            return new SysTableDescribe(type, null, tvfRef, null);
        }

        public static SysTableDescribe forPlugin(ExternalTable wrapper) {
            Preconditions.checkNotNull(wrapper, "wrapper is null");
            SysTableSpec spec;
            if (wrapper instanceof NativeSysExternalTable) {
                spec = ((NativeSysExternalTable) wrapper).getSpec();
            } else if (wrapper instanceof ConnectorManagedSysExternalTable) {
                spec = ((ConnectorManagedSysExternalTable) wrapper).getSpec();
            } else {
                throw new IllegalArgumentException(
                        "forPlugin requires NativeSysExternalTable or ConnectorManagedSysExternalTable, got "
                                + wrapper.getClass().getSimpleName());
            }
            return new SysTableDescribe(null, wrapper, null, spec);
        }

        public boolean isNative() {
            if (spec != null) {
                return spec.mode() == SysTableExecutionMode.NATIVE;
            }
            return sysExternalTable != null;
        }

        public boolean isPluginManaged() {
            return spec != null;
        }

        /**
         * Mirrors {@link SysTablePlan#isPluginTvf()} for the describe path.
         */
        public boolean isPluginTvf() {
            return spec != null && spec.mode() == SysTableExecutionMode.TVF;
        }

        public SysTable getSysTable() {
            return type;
        }

        public Optional<SysTableSpec> getSysTableSpec() {
            return Optional.ofNullable(spec);
        }

        public ExternalTable getSysExternalTable() {
            Preconditions.checkState(sysExternalTable != null, "Not a sys-external-table describe");
            return sysExternalTable;
        }

        public TableValuedFunctionRefInfo getTvfRef() {
            Preconditions.checkState(tvfRef != null, "Not a TVF system table");
            return tvfRef;
        }
    }
}
