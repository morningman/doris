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
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;

public class KillUtils {
    private static final Logger LOG = LogManager.getLogger(KillUtils.class);

    public static void kill(ConnectContext ctx, String queryId, int connectionId) throws UserException {
        ConnectContext killCtx = null;
        boolean killConnection = false;
        if (connectionId == -1) {
            // kill by query id
            Preconditions.checkState(!Strings.isNullOrEmpty(queryId));
            killCtx = ctx.getConnectScheduler().getContextWithQueryId(queryId);
        } else {
            // kill by connection id
            Preconditions.checkState(connectionId >= 0, connectionId);
            killCtx = ctx.getConnectScheduler().getContext(connectionId);
            if (killCtx == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_NO_SUCH_THREAD, connectionId);
            }
            killConnection = true;
        }

        if (killCtx == null) {
            // if kill by query id but killCtx == null, this means the query not in FE,
            // then we send kill signal to BE
            killToBackend(queryId);
        } else if (ctx == killCtx) {
            // Suicide
            ctx.setKilled();
        } else {
            // Check auth
            // Only user itself and user with admin priv can kill connection
            if (!killCtx.getQualifiedUser().equals(ctx.getQualifiedUser())
                    && !Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.ADMIN)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_KILL_DENIED_ERROR, connectionId);
            }
            killCtx.kill(killConnection);
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
