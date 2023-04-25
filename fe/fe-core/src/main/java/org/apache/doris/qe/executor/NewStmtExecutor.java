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

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.StringReader;
import java.util.List;

public class NewStmtExecutor {
    private static final Logger LOG = LogManager.getLogger(NewStmtExecutor.class);
    private String sql;
    private ExecContext execContext;
    private ResultProcessor resultProcessor;

    public NewStmtExecutor(String sql, ExecContext execContext, ResultProcessor resultProcessor) {
        this.sql = sql;
        this.execContext = execContext;
        this.resultProcessor = resultProcessor;
    }

    public void execute() throws UserException {
        execContext.setThreadLocalInfo();
        try {
            beforeAll();

            List<ParsedStmt> parsedStmts = parse();

            for (ParsedStmt parsedStmt : parsedStmts) {
                executeSingleStmt(parsedStmt);
            }

            afterAll();
        } finally {
            ExecContext.remove();
        }
    }

    private List<ParsedStmt>  parse() throws AnalysisException {
        List<StatementBase> stmts = null;
        // try parse by nereids
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
                LOG.info("Nereids parse sql failed. Reason: {}. Statement: \"{}\".",
                        t.getMessage(), sql);
                if (execContext.getSessionVariable().enableFallbackToOriginalPlanner) {
                    stmts = null;
                } else {
                    throw new AnalysisException(t.getMessage());
                }
            }
        }

        // if nereids parse failed, try parse by legacy
        if (stmts == null) {
            stmts = parseByLegacy();
        }

        List<String> origSingleStmtList = null;
        // if stmts.size() > 1, split originStmt to multi singleStmts
        if (stmts.size() > 1) {
            try {
                origSingleStmtList = SqlUtils.splitMultiStmts(sql);
            } catch (Exception e) {
                throw new AnalysisException("try split mutil statement failed: \n" + sql, e);
            }

            if (origSingleStmtList.size() != stmts.size()) {
                throw new AnalysisException("split mutil statement failed, split size not equal. expected:" + stmts.size()
                        ", actual: " + origSingleStmtList.size() + "\n"+ sql);
            }
        } else {
            origSingleStmtList = Lists.newArrayList(sql);
        }
        // create ParsedStmt
        List<ParsedStmt> parsedStmts = Lists.newArrayListWithCapacity(stmts.size());
        for (int i = 0; i < stmts.size(); i++) {
            StatementBase stmt = stmts.get(i);
            String origSingleStmt = origSingleStmtList.get(i);
            ParsedStmt parsedStmt = new ParsedStmt(i, origSingleStmt, stmt);
            parsedStmts.add(parsedStmt);
        }

        return parsedStmts;
    }

    private List<StatementBase> parseByLegacy() throws AnalysisException {
        LOG.debug("the origin sql string is: {}", sql);
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

    private void executeSingleStmt(ParsedStmt parsedStmt) {
        beforeStmt(parsedStmt);
    }
}
