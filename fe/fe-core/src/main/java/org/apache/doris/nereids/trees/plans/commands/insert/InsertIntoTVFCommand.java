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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Status;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.ForwardWithSync;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.QueryInfo;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;

/**
 * Command for INSERT INTO tvf_name(properties) SELECT ...
 * This command is independent from InsertIntoTableCommand since TVF sink
 * has no real table, no transaction, and no table lock.
 */
public class InsertIntoTVFCommand extends Command implements ForwardWithSync, Explainable {

    private static final Logger LOG = LogManager.getLogger(InsertIntoTVFCommand.class);

    private final LogicalPlan logicalQuery;
    private final Optional<String> labelName;
    private final Optional<LogicalPlan> cte;

    public InsertIntoTVFCommand(LogicalPlan logicalQuery, Optional<String> labelName,
            Optional<LogicalPlan> cte) {
        super(PlanType.INSERT_INTO_TVF_COMMAND);
        this.logicalQuery = logicalQuery;
        this.labelName = labelName;
        this.cte = cte;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // 1. Check privilege
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.ADMIN)
                && !Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    "INSERT INTO TVF requires ADMIN or LOAD privilege");
        }

        // 2. Prepare the plan
        LogicalPlan plan = logicalQuery;
        if (cte.isPresent()) {
            plan = (LogicalPlan) cte.get().withChildren(plan);
        }

        LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(plan, ctx.getStatementContext());
        NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
        planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());

        executor.setPlanner(planner);
        executor.checkBlockRules();

        if (ctx.getConnectType() == ConnectType.MYSQL && ctx.getMysqlChannel() != null) {
            ctx.getMysqlChannel().reset();
        }

        // 3. Create coordinator
        Coordinator coordinator = EnvFactory.getInstance().createCoordinator(
                ctx, planner, ctx.getStatsErrorEstimator());

        TUniqueId queryId = ctx.queryId();
        QeProcessorImpl.INSTANCE.registerQuery(queryId,
                new QueryInfo(ctx, "INSERT INTO TVF", coordinator));

        try {
            coordinator.exec();

            // Wait for completion
            if (coordinator.join(ctx.getExecTimeout())) {
                if (!coordinator.isDone()) {
                    coordinator.cancel(new Status(TStatusCode.INTERNAL_ERROR, "Insert into TVF timeout"));
                    ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Insert into TVF timeout");
                    return;
                }
            }

            if (coordinator.getExecStatus().ok()) {
                String label = labelName.orElse(
                        String.format("tvf_insert_%x_%x", ctx.queryId().hi, ctx.queryId().lo));
                ctx.getState().setOk(0, 0, "Insert into TVF succeeded. label: " + label);
            } else {
                String errMsg = coordinator.getExecStatus().getErrorMsg();
                LOG.warn("insert into TVF failed, error: {}", errMsg);
                ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, errMsg);
            }
        } catch (Exception e) {
            LOG.warn("insert into TVF failed", e);
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR,
                    e.getMessage() == null ? "unknown error" : e.getMessage());
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
            coordinator.close();
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        return this.logicalQuery;
    }

    @Override
    public List<? extends TreeNode<?>> children() {
        return List.of(logicalQuery);
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new InsertIntoTVFCommand((LogicalPlan) children.get(0), labelName, cte);
    }
}
