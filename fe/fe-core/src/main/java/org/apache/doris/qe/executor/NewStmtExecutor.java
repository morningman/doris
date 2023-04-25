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

package org.apache.doris.qe.executor;

import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.SetStmt;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.NereidsException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.common.util.SqlUtils;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.stats.StatsErrorEstimator;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.CreatePolicyCommand;
import org.apache.doris.nereids.trees.plans.commands.Forward;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.MasterOpExecutor;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.QueryStateException;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.StringReader;
import java.util.List;

/**
 * StmtExecutor is used to execute a raw SQL.
 * It will parse the SQL, and execute the parsed stmts one by one.
 */
public class NewStmtExecutor {
    private static final Logger LOG = LogManager.getLogger(NewStmtExecutor.class);
    private String sql;
    private ExecContext execContext;
    private MasterOpExecutor masterOpExecutor = null;
    private boolean isProxy = false;
    private Planner planner;

    public NewStmtExecutor(String sql, ExecContext execContext) {
        this.sql = sql;
        this.execContext = execContext;
    }

    public void execute() throws UserException {
        execContext.setThreadLocalInfo();
        try {
            beforeAll();
            List<StatementBase> parsedStmts = parse();
            for (StatementBase parsedStmt : parsedStmts) {
                executeSingleStmt(parsedStmt);
            }
            afterAll();
        } finally {
            ExecContext.remove();
        }
    }

    // Do some preparation work before execute
    private void beforeAll() {
    }

    // Do some clean work after execute
    private void afterAll() {

    }

    /**
     * Parse the raw sql into ParsedStmt.
     * Doris supports multi-statement sql, so the raw sql may be parsed into multiple ParsedStmt.
     * It will first try to parse the sql by Nereids, if failed, it will try to parse the sql by legacy parser.
     *
     * @return
     * @throws AnalysisException
     */
    private List<StatementBase> parse() throws AnalysisException {
        LOG.debug("the raw sql string is: {}", sql);
        // try parse by nereids
        List<StatementBase> stmts = parseByNereids();
        // if nereids parse failed, try parse by legacy
        if (stmts == null) {
            stmts = parseByLegacy();
        }
        // split raw SQLs
        splitRawSQLs(stmts);
        return stmts;
    }

    @NotNull
    private void splitRawSQLs(List<StatementBase> parsedStmts) throws AnalysisException {
        List<String> rawSQLs;
        if (parsedStmts.size() > 1) {
            try {
                rawSQLs = SqlUtils.splitMultiStmts(sql);
            } catch (Exception e) {
                throw new AnalysisException("try split multi statement failed: \n" + sql, e);
            }
            if (rawSQLs.size() != parsedStmts.size()) {
                throw new AnalysisException(
                        "split mutil statement failed, split size not equal. expected:" + parsedStmts.size()
                                + ", actual: " + rawSQLs.size() + "\n" + sql);
            }
        } else {
            rawSQLs = Lists.newArrayList(sql);
        }

        for (int i = 0; i < parsedStmts.size(); i++) {
            StatementBase stmt = parsedStmts.get(i);
            String rawSQL = rawSQLs.get(i);
            stmt.setOrigStmt(new OriginStatement(rawSQL, i));
        }
    }

    @Nullable
    private List<StatementBase> parseByNereids() throws AnalysisException {
        List<StatementBase> stmts = null;
        if (execContext.getSessionVariable().isEnableNereidsPlanner()) {
            try {
                stmts = new NereidsParser().parseSQL(sql);
                for (StatementBase stmt : stmts) {
                    LogicalPlanAdapter logicalPlanAdapter = (LogicalPlanAdapter) stmt;
                    // TODO: remove this after we could process CreatePolicyCommand
                    if (logicalPlanAdapter.getLogicalPlan() instanceof CreatePolicyCommand) {
                        stmts = null;
                        break;
                    }
                }
            } catch (Throwable t) {
                LOG.info("Nereids parse sql failed. Reason: {}. Statement: \"{}\".", t.getMessage(), sql);
                if (execContext.getSessionVariable().enableFallbackToOriginalPlanner) {
                    stmts = null;
                } else {
                    throw new AnalysisException(t.getMessage());
                }
            }
        }
        return stmts;
    }

    private List<StatementBase> parseByLegacy() throws AnalysisException {
        SqlScanner input = new SqlScanner(new StringReader(sql), execContext.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);
        try {
            return SqlParserUtils.getMultiStmts(parser);
        } catch (Error e) {
            throw new AnalysisException("Please check your sql, we meet an error when parsing: " + e.getMessage(), e);
        } catch (AnalysisException e) {
            String errorMessage = parser.getErrorMsg(sql);
            LOG.debug("analyze error message: {}", parser.getErrorMsg(sql), e);
            if (errorMessage == null) {
                throw e;
            } else {
                throw new AnalysisException(errorMessage, e);
            }
        } catch (ArrayStoreException e) {
            throw new AnalysisException("Sql parser can't convert the result to array, please check your sql.", e);
        } catch (Exception e) {
            // TODO(lingbin): we catch 'Exception' to prevent unexpected error,
            // should be removed this try-catch clause future.
            throw new AnalysisException("Internal Error, maybe syntax error or this is a bug: " + e.getMessage(), e);
        }
    }

    private void executeSingleStmt(StatementBase parsedStmt) throws Exception {
        prepare(parsedStmt);
        try {
            SessionVariable sessionVariable = execContext.getSessionVariable();
            Span executeSpan = execContext.getTracer().spanBuilder("execute").setParent(Context.current()).startSpan();
            try (Scope scope = executeSpan.makeCurrent()) {
                if (parsedStmt instanceof LogicalPlanAdapter || sessionVariable.isEnableNereidsPlanner()) {
                    try {
                        executeByNereids(parsedStmt);
                    } catch (NereidsException e) {
                        // try to fall back to legacy planner
                        LOG.warn("nereids cannot process statement\n" + parsedStmt.getOrigStmt().originStmt
                                + "\n because of " + e.getMessage(), e);
                        if (!execContext.getSessionVariable().enableFallbackToOriginalPlanner) {
                            LOG.warn("Analyze failed. {}", DebugUtil.printId(parsedStmt.getQueryId()), e);
                            throw e.getException();
                        }
                        LOG.info("fall back to legacy planner: {}", DebugUtil.printId(parsedStmt.getQueryId()));
                        execContext.setNereids(false);
                        executeByLegacy(parsedStmt);
                    }
                } else {
                    executeByLegacy(parsedStmt);
                }
            } finally {
                executeSpan.end();
            }
        } finally {
            afterExecute(parsedStmt);
        }
    }

    private void executeByNereids(StatementBase parsedStmt) throws Exception {
        LOG.info("Nereids start to execute query:\n {}", parsedStmt.getOrigStmt().originStmt);
        Preconditions.checkState(parsedStmt instanceof LogicalPlanAdapter,
                "Nereids only process LogicalPlanAdapter, but parsedStmt is " + parsedStmt.getClass().getName());
        execContext.getState().setNereids(true);
        LogicalPlan logicalPlan = ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();
        if (logicalPlan instanceof Command) {
            if (logicalPlan instanceof Forward) {
                if (isForwardToMaster(((Forward) logicalPlan).toRedirectStatus(), isQuery(parsedStmt))) {
                    if (isProxy) {
                        // This is already a stmt forwarded from other FE.
                        // If goes here, which means we can't find a valid Master FE(some error happens).
                        // To avoid endless forward, throw exception here.
                        throw new NereidsException(new UserException("The statement has been forwarded to master FE("
                                + Env.getCurrentEnv().getSelfNode().getIp() + ") and failed to execute"
                                + " because Master FE is not ready. You may need to check FE's status"));
                    }
                    forwardToMaster(parsedStmt, ((Forward) logicalPlan).toRedirectStatus());
                    if (masterOpExecutor != null && masterOpExecutor.getQueryId() != null) {
                        execContext.getCtx().setQueryId(masterOpExecutor.getQueryId());
                    }
                    return;
                }
            }
            try {
                ((Command) logicalPlan).run(execContext.getCtx(), this);
            } catch (QueryStateException e) {
                LOG.warn("", e);
                execContext.getCtx().setState(e.getQueryState());
                throw new NereidsException(e);
            } catch (UserException e) {
                // Return message to info client what happened.
                LOG.warn("DDL statement({}) process failed.", parsedStmt.getOrigStmt().originStmt, e);
                execContext.getCtx().getState().setError(e.getMysqlErrorCode(), e.getMessage());
                throw new NereidsException("DDL statement(" + parsedStmt.getOrigStmt().originStmt + ") process failed",
                        e);
            } catch (Exception e) {
                // Maybe our bug
                LOG.warn("DDL statement(" + parsedStmt.getOrigStmt().originStmt + ") process failed.", e);
                execContext.getCtx().getState()
                        .setError(ErrorCode.ERR_UNKNOWN_ERROR, "Unexpected exception: " + e.getMessage());
                throw new NereidsException("DDL statement(" + parsedStmt.getOrigStmt().originStmt + ") process failed.",
                        e);
            }
        } else {
            execContext.getCtx().getState().setIsQuery(true);
            if (execContext.getSessionVariable().enableProfile) {
                ConnectContext.get().setStatsErrorEstimator(new StatsErrorEstimator());
            }
            // create plan
            Planner planner = new NereidsPlanner(statementContext);
            try {
                planner.plan(parsedStmt, execContext.getSessionVariable().toThrift());
            } catch (Exception e) {
                LOG.warn("Nereids plan query failed:\n{}", parsedStmt.getOrigStmt().originStmt);
                throw new NereidsException(new AnalysisException("Unexpected exception: " + e.getMessage(), e));
            }
            if (checkBlockRules(parsedStmt)) {
                return;
            }
            execContext.getPlannerProfile().setQueryPlanFinishTime();
            handleQueryWithRetry(queryId);
        }
    }

    private void executeByLegacy(StatementBase parsedStmt) {

    }

    private void prepare(StatementBase parsedStmt) {
        execContext.prepareParsedStmt(parsedStmt);

    }

    private void afterExecute(StatementBase parsedStmt) {
        // revert Session Value
        try {
            VariableMgr.revertSessionValue(execContext.getSessionVariable());
            // origin value init
            execContext.getSessionVariable().setIsSingleSetVar(false);
            execContext.getSessionVariable().clearSessionOriginValue();
        } catch (DdlException e) {
            LOG.warn("failed to revert Session value. {}", DebugUtil.printId(parsedStmt.getQueryId()), e);
        }
    }

    private boolean isForwardToMaster(RedirectStatus redirectStatus, boolean isQuery) {
        if (Env.getCurrentEnv().isMaster()) {
            return false;
        }
        // this is a query stmt, but this non-master FE can not read, forward it to master
        if (isQuery && !Env.getCurrentEnv().isMaster() && !Env.getCurrentEnv().canRead()) {
            return true;
        }
        return redirectStatus == null ? false : redirectStatus.isForwardToMaster();
    }

    private void forwardToMaster(StatementBase parsedStmt, RedirectStatus redirectStatus) throws Exception {
        masterOpExecutor = new MasterOpExecutor(parsedStmt.getOrigStmt(), execContext.getCtx(),
                redirectStatus, isQuery(parsedStmt));
        LOG.debug("need to transfer to Master. stmt: {}", execContext.getCtx().getStmtId());
        masterOpExecutor.execute();
        if (parsedStmt instanceof SetStmt) {
            SetStmt setStmt = (SetStmt) parsedStmt;
            setStmt.modifySetVarsForExecute();
            for (SetVar var : setStmt.getSetVars()) {
                VariableMgr.setVarForNonMasterFE(execContext.getSessionVariable(), var);
            }
        }
    }

    private boolean isQuery(StatementBase parsedStmt) {
        return parsedStmt instanceof QueryStmt
                || (parsedStmt instanceof LogicalPlanAdapter
                && !(((LogicalPlanAdapter) parsedStmt).getLogicalPlan() instanceof Command));
    }

    private boolean checkBlockRules(StatementBase parsedStmt) throws AnalysisException {
        Env.getCurrentEnv().getSqlBlockRuleMgr().matchSql(
                parsedStmt.getOrigStmt().originStmt, execContext.getCtx().getSqlHash(),
                execContext.getCtx().getQualifiedUser());

        // limitations: partition_num, tablet_num, cardinality
        List<ScanNode> scanNodeList = planner.getScanNodes();
        for (ScanNode scanNode : scanNodeList) {
            if (scanNode instanceof OlapScanNode) {
                OlapScanNode olapScanNode = (OlapScanNode) scanNode;
                Env.getCurrentEnv().getSqlBlockRuleMgr().checkLimitations(
                        olapScanNode.getSelectedPartitionNum().longValue(),
                        olapScanNode.getSelectedTabletsNum(),
                        olapScanNode.getCardinality(),
                        execContext.getCtx().getQualifiedUser());
            }
        }

        return false;
    }
}
