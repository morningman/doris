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

import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.common.util.SqlUtils;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreatePolicyCommand;
import org.apache.doris.qe.OriginStatement;

import com.google.common.collect.Lists;
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
        execContext.prepareAudit();
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

    private void executeSingleStmt(StatementBase parsedStmt) {
        prepare(parsedStmt);
    }

    private void prepare(StatementBase parsedStmt) {
        execContext.prepareParsedStmt(parsedStmt);
    }
}
