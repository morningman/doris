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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableId;
import org.apache.doris.connector.api.action.ActionArgument;
import org.apache.doris.connector.api.action.ActionDescriptor;
import org.apache.doris.connector.api.action.ActionInvocation;
import org.apache.doris.connector.api.action.ActionResult;
import org.apache.doris.connector.api.action.ActionTarget;
import org.apache.doris.connector.api.action.ConnectorActionOps;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NumericLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Command for {@code CALL <catalog>.<db>.<table>.<action>(...)}.
 *
 * <p>Resolves the target table to a {@link PluginDrivenExternalTable}, looks
 * up the connector's {@link ActionDescriptor} via the
 * {@link ConnectorActionOps} SPI, validates / coerces arguments, then invokes
 * {@link ConnectorActionOps#executeAction(ActionInvocation)}.</p>
 *
 * <p>Privilege requirement: {@code LOAD_PRIV} on the target table — same as
 * INSERT / DELETE / UPDATE — because actions mutate table data or metadata.</p>
 */
public class PluginDrivenExecuteActionCommand extends Command implements ForwardWithSync {

    private final TableNameInfo tableNameInfo;
    private final String actionName;
    private final List<Expression> positionalArguments;
    private final Map<String, Expression> namedArguments;
    private final String originSql;

    /** Constructor. */
    public PluginDrivenExecuteActionCommand(TableNameInfo tableNameInfo, String actionName,
            List<Expression> positionalArguments, Map<String, Expression> namedArguments,
            String originSql) {
        super(PlanType.PLUGIN_DRIVEN_EXECUTE_ACTION_COMMAND);
        this.tableNameInfo = Objects.requireNonNull(tableNameInfo, "tableNameInfo");
        this.actionName = Objects.requireNonNull(actionName, "actionName");
        this.positionalArguments = new ArrayList<>(
                Objects.requireNonNull(positionalArguments, "positionalArguments"));
        this.namedArguments = new LinkedHashMap<>(
                Objects.requireNonNull(namedArguments, "namedArguments"));
        this.originSql = originSql;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        tableNameInfo.analyze(ctx.getNameSpaceContext());

        CatalogIf<?> catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(tableNameInfo.getCtl());
        if (catalog == null) {
            throw new AnalysisException("Catalog " + tableNameInfo.getCtl() + " does not exist");
        }
        if (!(catalog instanceof PluginDrivenExternalCatalog)) {
            throw new AnalysisException(
                    "CALL is only supported for plugin-driven catalogs in this version, but catalog '"
                            + tableNameInfo.getCtl() + "' is " + catalog.getClass().getSimpleName());
        }

        DatabaseIf<?> database = catalog.getDbNullable(tableNameInfo.getDb());
        if (database == null) {
            throw new AnalysisException("Database " + tableNameInfo.getDb() + " does not exist");
        }
        TableIf table = database.getTableNullable(tableNameInfo.getTbl());
        if (table == null) {
            throw new AnalysisException("Table " + tableNameInfo.getTbl() + " does not exist");
        }
        if (!(table instanceof PluginDrivenExternalTable)) {
            throw new AnalysisException(
                    "CALL is only supported for plugin-driven catalogs in this version, but table '"
                            + tableNameInfo.getTbl() + "' is " + table.getClass().getSimpleName());
        }

        // LOAD_PRIV check, mirrors INSERT / DELETE / UPDATE.
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(
                ctx, tableNameInfo.getCtl(), tableNameInfo.getDb(), tableNameInfo.getTbl(),
                PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                    ctx.getQualifiedUser(), ctx.getRemoteIP(),
                    tableNameInfo.getDb() + "." + tableNameInfo.getTbl());
        }

        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        Connector connector = pluginCatalog.getConnector();
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);

        Optional<ConnectorActionOps> actionOpsOpt = metadata.actionOps();
        if (!actionOpsOpt.isPresent()) {
            throw new AnalysisException("Connector for catalog '" + tableNameInfo.getCtl()
                    + "' does not expose any actions");
        }
        ConnectorActionOps actionOps = actionOpsOpt.get();

        PluginDrivenExternalTable pluginTable = (PluginDrivenExternalTable) table;
        String remoteDb = pluginTable.getRemoteDbName();
        String remoteTbl = pluginTable.getRemoteName();
        ConnectorTableId tableId = ConnectorTableId.of(remoteDb, remoteTbl);
        ActionTarget target = ActionTarget.ofTable(tableId);

        ActionDescriptor descriptor = resolveDescriptor(actionOps, target);
        Map<String, Object> coercedArgs = coerceArguments(descriptor);

        ActionInvocation invocation = ActionInvocation.builder()
                .name(descriptor.name())
                .scope(ConnectorActionOps.Scope.TABLE)
                .target(target)
                .arguments(coercedArgs)
                .build();
        ActionResult result = actionOps.executeAction(invocation);

        if (result.lifecycle() == ActionResult.Lifecycle.FAILED) {
            throw new AnalysisException("Action '" + actionName + "' failed: "
                    + result.errorMessage().orElse("(no message)"));
        }

        ShowResultSet resultSet = toResultSet(descriptor, result);
        executor.sendResultSet(resultSet);
    }

    private ActionDescriptor resolveDescriptor(ConnectorActionOps actionOps, ActionTarget target) {
        List<ActionDescriptor> descriptors = actionOps.listActions(
                ConnectorActionOps.Scope.TABLE, Optional.of(target));
        for (ActionDescriptor d : descriptors) {
            if (d.name().equalsIgnoreCase(actionName)) {
                return d;
            }
        }
        throw new AnalysisException("Unknown action '" + actionName + "' for table "
                + tableNameInfo.getDb() + "." + tableNameInfo.getTbl());
    }

    private Map<String, Object> coerceArguments(ActionDescriptor descriptor) {
        List<ActionArgument> formals = descriptor.arguments();

        // Reject positional-after-named is enforced at parse time; here we only validate counts.
        if (positionalArguments.size() > formals.size()) {
            throw new AnalysisException("Action '" + actionName + "' accepts at most "
                    + formals.size() + " positional arguments, but got "
                    + positionalArguments.size());
        }

        // Detect duplicates: positional[i] vs namedArgs[formal[i].name()]
        Map<String, Object> bound = new LinkedHashMap<>();
        for (int i = 0; i < positionalArguments.size(); i++) {
            ActionArgument formal = formals.get(i);
            Object coerced = coerceLiteral(formal, positionalArguments.get(i));
            bound.put(formal.name(), coerced);
        }
        for (Map.Entry<String, Expression> entry : namedArguments.entrySet()) {
            ActionArgument formal = findFormal(formals, entry.getKey());
            if (formal == null) {
                throw new AnalysisException("Unknown argument '" + entry.getKey()
                        + "' for action '" + actionName + "'");
            }
            if (bound.containsKey(formal.name())) {
                throw new AnalysisException("Argument '" + formal.name()
                        + "' for action '" + actionName + "' specified more than once");
            }
            bound.put(formal.name(), coerceLiteral(formal, entry.getValue()));
        }

        // Apply defaults / check required.
        for (ActionArgument formal : formals) {
            if (bound.containsKey(formal.name())) {
                continue;
            }
            if (formal.defaultValue().isPresent()) {
                bound.put(formal.name(), parseDefault(formal));
            } else if (formal.required()) {
                throw new AnalysisException("Missing required argument '" + formal.name()
                        + "' for action '" + actionName + "'");
            } else {
                bound.put(formal.name(), null);
            }
        }
        return bound;
    }

    private static ActionArgument findFormal(List<ActionArgument> formals, String name) {
        for (ActionArgument f : formals) {
            if (f.name().equalsIgnoreCase(name)) {
                return f;
            }
        }
        return null;
    }

    private Object coerceLiteral(ActionArgument formal, Expression expr) {
        if (!(expr instanceof Literal)) {
            throw new AnalysisException("Argument '" + formal.name() + "' for action '"
                    + actionName + "' must be a literal, got " + expr.getClass().getSimpleName());
        }
        Literal literal = (Literal) expr;
        try {
            switch (formal.type()) {
                case STRING:
                case JSON:
                    return literal.getStringValue();
                case LONG:
                    if (literal instanceof NumericLiteral) {
                        return ((Number) literal.getValue()).longValue();
                    }
                    if (literal instanceof StringLikeLiteral) {
                        return Long.parseLong(literal.getStringValue());
                    }
                    break;
                case DOUBLE:
                    if (literal instanceof NumericLiteral) {
                        return ((Number) literal.getValue()).doubleValue();
                    }
                    if (literal instanceof StringLikeLiteral) {
                        return Double.parseDouble(literal.getStringValue());
                    }
                    break;
                case BOOLEAN:
                    if (literal instanceof BooleanLiteral) {
                        return literal.getValue();
                    }
                    if (literal instanceof StringLikeLiteral) {
                        return Boolean.parseBoolean(literal.getStringValue());
                    }
                    break;
                default:
                    break;
            }
        } catch (NumberFormatException e) {
            throw new AnalysisException("Cannot coerce argument '" + formal.name()
                    + "' to " + formal.type() + ": " + e.getMessage());
        }
        throw new AnalysisException("Cannot coerce literal " + literal + " to " + formal.type()
                + " for argument '" + formal.name() + "'");
    }

    private Object parseDefault(ActionArgument formal) {
        String raw = formal.defaultValue().get();
        try {
            switch (formal.type()) {
                case STRING:
                case JSON:
                    return raw;
                case LONG:
                    return Long.parseLong(raw);
                case DOUBLE:
                    return Double.parseDouble(raw);
                case BOOLEAN:
                    return Boolean.parseBoolean(raw);
                default:
                    return raw;
            }
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid default value '" + raw + "' for argument '"
                    + formal.name() + "': " + e.getMessage());
        }
    }

    private ShowResultSet toResultSet(ActionDescriptor descriptor, ActionResult result) {
        ShowResultSetMetaData.Builder meta = ShowResultSetMetaData.builder();
        List<ActionDescriptor.ResultColumn> schema = descriptor.resultSchema();
        for (ActionDescriptor.ResultColumn col : schema) {
            meta.addColumn(new Column(col.name(), toScalarType(col.type())));
        }
        List<List<String>> rows = Lists.newArrayList();
        for (Map<String, Object> row : result.rows()) {
            List<String> stringRow = new ArrayList<>(schema.size());
            for (ActionDescriptor.ResultColumn col : schema) {
                Object value = row.get(col.name());
                stringRow.add(value == null ? "NULL" : String.valueOf(value));
            }
            rows.add(stringRow);
        }
        return new ShowResultSet(meta.build(), rows);
    }

    private static ScalarType toScalarType(
            org.apache.doris.connector.api.action.ActionArgumentType type) {
        switch (type) {
            case LONG:
                return Type.BIGINT;
            case DOUBLE:
                return Type.DOUBLE;
            case BOOLEAN:
                return Type.BOOLEAN;
            case STRING:
            case JSON:
            default:
                return ScalarType.createStringType();
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPluginDrivenExecuteActionCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CALL;
    }

    public TableNameInfo getTableNameInfo() {
        return tableNameInfo;
    }

    public String getActionName() {
        return actionName;
    }

    public List<Expression> getPositionalArguments() {
        return positionalArguments;
    }

    public Map<String, Expression> getNamedArguments() {
        return namedArguments;
    }

    public String getOriginSql() {
        return originSql;
    }
}
