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

package org.apache.doris.nereids.trees.plans.commands.utils;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.FEOpExecutor;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Frontend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;

public class KillUtils {
    private static final Logger LOG = LogManager.getLogger(KillUtils.class);

    public static void kill(ConnectContext ctx, String queryId, int connectionId, OriginStatement stmt)
            throws Exception {
        if (connectionId == -1) {
            // kill by query id
            Preconditions.checkState(!Strings.isNullOrEmpty(queryId));
            killByQueryId(ctx, queryId, stmt);
        } else {
            // kill by connection id
            Preconditions.checkState(connectionId >= 0, connectionId);
            killByConnection(ctx, connectionId);
        }
    }

    private static void killByQueryId(ConnectContext ctx, String queryId, OriginStatement stmt) throws UserException {
        // 1. First, try to find the query in the current FE and kill it
        if (killByQueryIdOnCurrentNode(ctx, queryId)) {
            return;
        }

        if (ctx.isProxy()) {
            // The query is not found in the current FE, and the command is forwarded from other FE.
            // return error to let the proxy FE to handle it.
            ErrorReport.reportDdlException(ErrorCode.ERR_NO_SUCH_QUERY, queryId);
        }

        // 2. Query not found in current FE, try to kill the query in other FE.
        for (Frontend fe : Env.getCurrentEnv().getFrontends(null /* all */)) {
            if (!fe.isAlive() || fe.getHost().equals(Env.getCurrentEnv().getSelfNode().getHost())) {
                continue;
            }

            TNetworkAddress feAddr = new TNetworkAddress(fe.getHost(), fe.getRpcPort());
            FEOpExecutor executor = new FEOpExecutor(feAddr, stmt, ConnectContext.get(), false);
            try {
                executor.execute();
            } catch (Exception e) {
                throw new DdlException(e.getMessage(), e);
            }
            if (executor.getStatusCode() != TStatusCode.OK.getValue()) {
                throw new DdlException(String.format("failed to apply to fe %s:%s, error message: %s",
                        fe.getHost(), fe.getRpcPort(), executor.getErrMsg()));
            } else {
                // Find query in other FE, just return
                ctx.getState().setOk();
                return;
            }
        }

        // 3. Query not found in any FE, try cancel the query in BE.
        killToBackend(queryId);
    }

    private static boolean killByQueryIdOnCurrentNode(ConnectContext ctx, String queryId) throws DdlException {
        ConnectContext killCtx = ctx.getConnectScheduler().getContextWithQueryId(queryId);
        if (killCtx != null) {
            // Check auth. Only user itself and user with admin priv can kill connection
            if (!killCtx.getQualifiedUser().equals(ctx.getQualifiedUser())
                    && !Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.ADMIN)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_KILL_DENIED_ERROR, queryId);
            }
            killCtx.kill(false);
            ctx.getState().setOk();
            return true;
        }
        return false;
    }

    private static void killByConnection(ConnectContext ctx, int connectionId) throws DdlException {
        ConnectContext killCtx = ctx.getConnectScheduler().getContext(connectionId);
        if (killCtx == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_NO_SUCH_THREAD, connectionId);
        }
        if (ctx == killCtx) {
            // Suicide
            ctx.setKilled();
        } else {
            // Check auth
            // Only user itself and user with admin priv can kill connection
            if (!killCtx.getQualifiedUser().equals(ctx.getQualifiedUser())
                    && !Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.ADMIN)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_KILL_DENIED_ERROR, connectionId);
            }
            killCtx.kill(true);
        }
    }

    private static void killToBackend(String queryId) throws UserException {
        Preconditions.checkState(!Strings.isNullOrEmpty(queryId));
        TUniqueId tQueryId = null;
        try {
            tQueryId = DebugUtil.parseTUniqueIdFromString(queryId);
        } catch (NumberFormatException e) {
            throw new UserException(e.getMessage());
        }

        LOG.info("kill query {}", queryId);
        Collection<Backend> nodesToPublish = Env.getCurrentSystemInfo()
                .getAllBackendsByAllCluster().values();
        for (Backend be : nodesToPublish) {
            if (be.isAlive()) {
                try {
                    Status cancelReason = new Status(TStatusCode.CANCELLED, "user kill query");
                    BackendServiceProxy.getInstance()
                            .cancelPipelineXPlanFragmentAsync(be.getBrpcAddress(), tQueryId,
                                    cancelReason);
                } catch (Throwable t) {
                    LOG.info("send kill query {} rpc to be {} failed", queryId, be);
                }
            }
        }
    }
}
