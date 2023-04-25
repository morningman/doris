package org.apache.doris.qe.executor;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.qe.ConnectContext;

import org.apache.commons.codec.digest.DigestUtils;

public class ConnectionExecContext extends ExecContext {

    private ConnectContext ctx;

    @Override
    public void prepareParsedStmt(StatementBase parsedStmt) {
        parsedStmt.setUserInfo(ctx.getCurrentUserIdentity());
    }

    @Override
    public void prepareAudit(StatementBase parsedStmt) {
        super.prepareAudit();
        String sqlHash = DigestUtils.md5Hex(parsedStmt.getOrigStmt().originStmt);
        ctx.setSqlHash(sqlHash);
        ctx.getAuditEventBuilder().reset();
        ctx.getAuditEventBuilder().setTimestamp(System.currentTimeMillis())
                .setClientIp(ctx.getMysqlChannel().getRemoteHostPortString())
                .setUser(ClusterNamespace.getNameFromFullName(ctx.getQualifiedUser())).setSqlHash(ctx.getSqlHash());
    }
}
