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

package org.apache.doris.plugin.audit;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.util.DigitalVersion;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.plugin.AuditPlugin;
import org.apache.doris.plugin.Plugin;
import org.apache.doris.plugin.PluginContext;
import org.apache.doris.plugin.PluginException;
import org.apache.doris.plugin.PluginInfo;
import org.apache.doris.plugin.PluginInfo.PluginType;
import org.apache.doris.plugin.PluginMgr;
import org.apache.doris.qe.GlobalVariable;

import com.google.common.collect.Queues;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/*
 * This plugin will load audit log to specified doris table at specified interval
 */
public class AuditLoader extends Plugin implements AuditPlugin {
    private static final Logger LOG = LogManager.getLogger(AuditLoader.class);

    public static final String AUDIT_LOG_TABLE = "audit_log";

    // the "0x1F" and "0x1E" are used to separate columns and lines in audit log data
    public static final char AUDIT_TABLE_COL_SEPARATOR = 0x1F;
    public static final char AUDIT_TABLE_LINE_DELIMITER = 0x1E;
    // the "\\x1F" and "\\x1E" are used to specified column and line delimiter in stream load request
    // which is corresponding to the "\\u001F" and "\\u001E" in audit log data.
    public static final String AUDIT_TABLE_COL_SEPARATOR_STR = "\\x1F";
    public static final String AUDIT_TABLE_LINE_DELIMITER_STR = "\\x1E";

    private StringBuilder auditLogBuffer = new StringBuilder();
    private int auditLogNum = 0;
    private long lastLoadTimeAuditLog = 0;
    // sometimes the audit log may fail to load to doris, count it to observe.
    private long discardLogNum = 0;

    private BlockingQueue<AuditEvent> auditEventQueue;
    private AuditStreamLoader streamLoader;
    private Thread loadThread;

    private volatile boolean isClosed = false;
    private volatile boolean isInit = false;

    private final PluginInfo pluginInfo;

    public AuditLoader() {
        pluginInfo = new PluginInfo(PluginMgr.BUILTIN_PLUGIN_PREFIX + "AuditLoader", PluginType.AUDIT,
                "builtin audit loader, to load audit log to internal table", DigitalVersion.fromString("2.1.0"),
                DigitalVersion.fromString("1.8.31"), AuditLoader.class.getName(), null, null);
    }

    public PluginInfo getPluginInfo() {
        return pluginInfo;
    }

    @Override
    public void init(PluginInfo info, PluginContext ctx) throws PluginException {
        super.init(info, ctx);

        synchronized (this) {
            if (isInit) {
                return;
            }
            this.lastLoadTimeAuditLog = System.currentTimeMillis();
            // make capacity large enough to avoid blocking.
            // and it will not be too large because the audit log will flush if num in queue is larger than
            // GlobalVariable.audit_plugin_max_batch_bytes.
            this.auditEventQueue = Queues.newLinkedBlockingDeque(100000);
            this.streamLoader = new AuditStreamLoader();
            this.loadThread = new Thread(new LoadWorker(), "audit loader thread");
            this.loadThread.start();

            isInit = true;
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        isClosed = true;
        if (loadThread != null) {
            try {
                loadThread.join();
            } catch (InterruptedException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("encounter exception when closing the audit loader", e);
                }
            }
        }
    }

    public boolean eventFilter(AuditEvent.EventType type) {
        return type == AuditEvent.EventType.AFTER_QUERY;
    }

    public void exec(AuditEvent event) {
        if (!GlobalVariable.enableAuditLoader) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("builtin audit loader is disabled, discard current audit event");
            }
            return;
        }
        try {
            auditEventQueue.add(event);
        } catch (Exception e) {
            // In order to ensure that the system can run normally, here we directly
            // discard the current audit_event. If this problem occurs frequently,
            // improvement can be considered.
            ++discardLogNum;
            if (LOG.isDebugEnabled()) {
                LOG.debug("encounter exception when putting current audit batch, discard current audit event."
                        + " total discard num: {}", discardLogNum, e);
            }
        }
    }

    private void assembleAudit(AuditEvent event) {
        fillLogBuffer(event, auditLogBuffer);
        ++auditLogNum;
    }

    private void fillLogBuffer(AuditEvent event, StringBuilder logBuffer) {
        // should be same order as InternalSchema.AUDIT_SCHEMA

        // uuid and time
        logBuffer.append(event.queryId).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(TimeUtils.longToTimeStringWithms(event.timestamp)).append(AUDIT_TABLE_COL_SEPARATOR);

        // cs info
        logBuffer.append(event.clientIp).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.user).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.feIp).append(AUDIT_TABLE_COL_SEPARATOR);

        // default ctl and db
        logBuffer.append(event.ctl).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.db).append(AUDIT_TABLE_COL_SEPARATOR);

        // query state
        logBuffer.append(event.state).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.errorCode).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.errorMessage).append(AUDIT_TABLE_COL_SEPARATOR);

        // execution info
        logBuffer.append(event.queryTime).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.cpuTimeMs).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.peakMemoryBytes).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.scanBytes).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.scanRows).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.returnRows).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.shuffleSendRows).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.shuffleSendBytes).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.spillWriteBytesToLocalStorage).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.spillReadBytesFromLocalStorage).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.scanBytesFromLocalStorage).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.scanBytesFromRemoteStorage).append(AUDIT_TABLE_COL_SEPARATOR);

        // plan info
        logBuffer.append(event.parseTimeMs).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.planTimesMs).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.getMetaTimesMs).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.scheduleTimesMs).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.hitSqlCache ? 1 : 0).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.isHandledInFe ? 1 : 0).append(AUDIT_TABLE_COL_SEPARATOR);

        // queried tables, views and m-views
        logBuffer.append(event.queriedTablesAndViews).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.chosenMViews).append(AUDIT_TABLE_COL_SEPARATOR);

        // variable and configs
        logBuffer.append(event.changedVariables).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.sqlMode).append(AUDIT_TABLE_COL_SEPARATOR);


        // type and digest
        logBuffer.append(event.stmtType).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.stmtId).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.sqlHash).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.sqlDigest).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.isQuery ? 1 : 0).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.isNereids ? 1 : 0).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.isInternal ? 1 : 0).append(AUDIT_TABLE_COL_SEPARATOR);

        // resource
        logBuffer.append(event.workloadGroup).append(AUDIT_TABLE_COL_SEPARATOR);
        logBuffer.append(event.cloudClusterName).append(AUDIT_TABLE_COL_SEPARATOR);

        // already trim the query in org.apache.doris.qe.AuditLogHelper#logAuditLog
        String stmt = event.stmt;
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive audit event with stmt: {}", stmt);
        }
        logBuffer.append(stmt).append(AUDIT_TABLE_LINE_DELIMITER);
    }

    // public for external call.
    // synchronized to avoid concurrent load.
    public synchronized void loadIfNecessary(boolean force) {
        long currentTime = System.currentTimeMillis();

        if (auditLogBuffer.length() != 0 && (force || auditLogBuffer.length() >= GlobalVariable.auditPluginMaxBatchBytes
                || currentTime - lastLoadTimeAuditLog >= GlobalVariable.auditPluginMaxBatchInternalSec * 1000)) {
            // begin to load
            try {
                String token = "";
                try {
                    // Acquire token from master
                    token = Env.getCurrentEnv().getTokenManager().acquireToken();
                } catch (Exception e) {
                    LOG.warn("Failed to get auth token: {}", e);
                    discardLogNum += auditLogNum;
                    return;
                }
                AuditStreamLoader.LoadResponse response = streamLoader.loadBatch(auditLogBuffer, token);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("audit loader response: {}", response);
                }
            } catch (Exception e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("encounter exception when putting current audit batch, discard current batch", e);
                }
                discardLogNum += auditLogNum;
            } finally {
                // make a new string builder to receive following events.
                resetBatch(currentTime);
                if (discardLogNum > 0) {
                    LOG.info("num of total discarded audit logs: {}", discardLogNum);
                }
            }
        }
    }

    private void resetBatch(long currentTime) {
        this.auditLogBuffer = new StringBuilder();
        this.lastLoadTimeAuditLog = currentTime;
        this.auditLogNum = 0;
    }

    private class LoadWorker implements Runnable {

        public LoadWorker() {
        }

        public void run() {
            while (!isClosed) {
                try {
                    AuditEvent event = auditEventQueue.poll(5, TimeUnit.SECONDS);
                    if (event != null) {
                        assembleAudit(event);
                    }
                    // process all audit logs
                    loadIfNecessary(false);
                } catch (InterruptedException ie) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("encounter exception when loading current audit batch", ie);
                    }
                } catch (Exception e) {
                    LOG.error("run audit logger error:", e);
                }
            }
        }
    }
}

