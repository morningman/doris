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

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;

import org.apache.commons.codec.digest.DigestUtils;

public class ConnectionExecContext extends ExecContext {

    private ConnectContext ctx;
    private QueryState queryState = new QueryState();

    @Override
    public void prepareParsedStmt(StatementBase parsedStmt) {
        parsedStmt.setUserInfo(ctx.getCurrentUserIdentity());
        ctx.setQueryId(parsedStmt.getQueryId());
        ctx.setStartTime();
        ctx.setStmtId(STMT_ID_GENERATOR.incrementAndGet());
        plannerProfile.setQueryBeginTime();
    }

    @Override
    public void prepareAudit(StatementBase parsedStmt) {
        super.prepareAudit(parsedStmt);
        String sqlHash = DigestUtils.md5Hex(parsedStmt.getOrigStmt().originStmt);
        ctx.setSqlHash(sqlHash);
        ctx.getAuditEventBuilder().reset();
        ctx.getAuditEventBuilder().setTimestamp(System.currentTimeMillis())
                .setClientIp(ctx.getMysqlChannel().getRemoteHostPortString())
                .setUser(ClusterNamespace.getNameFromFullName(ctx.getQualifiedUser())).setSqlHash(ctx.getSqlHash());
    }

    @Override
    public QueryState getState() {
        return queryState;
    }

    @Override
    public ConnectContext getCtx() {
        return ctx;
    }
}
