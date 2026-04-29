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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.write.WriteIntent;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsPartitionSpec;
import org.apache.doris.connector.hms.HmsTableInfo;
import org.apache.doris.connector.hms.HmsWriteOps;
import org.apache.doris.thrift.THiveLocationParams;
import org.apache.doris.thrift.THivePartitionUpdate;
import org.apache.doris.thrift.TUpdateMode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Plugin-side commit driver for Hive non-ACID writes.
 *
 * <p>Mirrors the legacy {@code HMSTransaction#finishInsertTable} branch
 * structure (switch on {@link TUpdateMode} per partition, with a
 * separate path for unpartitioned tables), but only emits HMS metadata
 * mutations through {@link HmsWriteOps} — file movement is the BE
 * writer's responsibility (BE writes directly into the partition
 * target path; the {@code write_path != target_path} S3 staging move
 * remains scoped to the M3-hive-cutover follow-up).</p>
 *
 * <p>Branch coverage:</p>
 * <ul>
 *   <li>unpartitioned + APPEND → no HMS mutation (table already exists)</li>
 *   <li>unpartitioned + OVERWRITE → no HMS mutation (table location is
 *       reused; existing file overwrite handled by BE/file system layer)</li>
 *   <li>partitioned + APPEND on existing partition → no HMS mutation</li>
 *   <li>partitioned + APPEND with NEW (cache miss) partition → check HMS,
 *       call {@link HmsWriteOps#addPartitions} when truly new</li>
 *   <li>partitioned + NEW → check HMS, add when truly new</li>
 *   <li>partitioned + OVERWRITE → drop existing partition (deleteData=false,
 *       BE owns the data files) then add the new partition</li>
 * </ul>
 *
 * <p>The driver never throws on partition-already-exists during NEW —
 * the legacy code falls back to APPEND in that case ("Doris cache miss
 * but HMS has the partition") and so does this implementation.</p>
 */
public final class HiveCommitDriver {

    private static final Logger LOG = LogManager.getLogger(HiveCommitDriver.class);

    private final HmsClient readClient;
    private final HmsWriteOps writeOps;

    public HiveCommitDriver(HmsClient readClient, HmsWriteOps writeOps) {
        this.readClient = readClient;
        this.writeOps = writeOps;
    }

    /**
     * Drives non-ACID commit. Returns the merged partition updates so
     * the caller can pass them to the cache-refresh layer (mirrors the
     * fe-core {@code HiveInsertExecutor#doAfterCommit} contract).
     */
    public List<THivePartitionUpdate> commitNonAcid(HiveTableHandle handle, WriteIntent intent,
            List<THivePartitionUpdate> updates) {
        HmsTableInfo tableInfo = readClient.getTable(handle.getDbName(), handle.getTableName());
        boolean partitioned = !handle.getPartitionKeyNames().isEmpty();
        List<THivePartitionUpdate> merged = HiveCommitDataConverter.mergeByPartition(
                synthesiseEmptyOverwriteIfNeeded(updates, intent, partitioned, tableInfo));

        if (!partitioned) {
            commitUnpartitioned(handle, tableInfo, merged);
            return merged;
        }
        for (THivePartitionUpdate update : merged) {
            commitPartition(handle, tableInfo, update);
        }
        return merged;
    }

    private void commitUnpartitioned(HiveTableHandle handle, HmsTableInfo tableInfo,
            List<THivePartitionUpdate> merged) {
        if (merged.size() > 1) {
            throw new DorisConnectorException(
                    "Unpartitioned hive table " + handle.getDbName() + "." + handle.getTableName()
                            + " received " + merged.size() + " partition updates");
        }
        if (merged.isEmpty()) {
            return;
        }
        THivePartitionUpdate pu = merged.get(0);
        TUpdateMode mode = pu.getUpdateMode();
        if (mode == TUpdateMode.APPEND || mode == null) {
            return;
        }
        if (mode == TUpdateMode.OVERWRITE) {
            // BE has staged files at table location; nothing to do at HMS
            // level beyond logging — the table descriptor itself is
            // unchanged. The legacy path drops + recreates the table to
            // refresh the file list, but doing that here would race with
            // the BE-staged files if the table location and write path
            // coincide (which they do for FILE_S3 / FILE_HDFS in the new
            // plugin write path); rely on cache invalidation in the
            // caller to surface the new file list.
            LOG.debug("Non-ACID OVERWRITE on unpartitioned table {}.{} (location={})",
                    handle.getDbName(), handle.getTableName(), tableInfo.getLocation());
            return;
        }
        throw new DorisConnectorException(
                "Unsupported update mode " + mode + " for unpartitioned hive table "
                        + handle.getDbName() + "." + handle.getTableName());
    }

    private void commitPartition(HiveTableHandle handle, HmsTableInfo tableInfo,
            THivePartitionUpdate update) {
        TUpdateMode mode = update.getUpdateMode();
        String partitionName = update.getName();
        if (partitionName == null || partitionName.isEmpty()) {
            throw new DorisConnectorException(
                    "Partition update missing name for partitioned hive table "
                            + handle.getDbName() + "." + handle.getTableName());
        }
        List<String> values = parsePartitionValues(partitionName);
        String location = locationOf(update);

        if (mode == TUpdateMode.APPEND) {
            // Existing partition — no HMS mutation. (Legacy HMSTransaction
            // tracks statistics here; the new plugin cache layer regenerates
            // them from the partition update list in the cache-refresh
            // step, so no statistics push-back is needed.)
            return;
        }
        if (mode == TUpdateMode.NEW) {
            if (partitionExists(handle, values)) {
                LOG.info("Partition {} already exists in HMS for {}.{}, treating NEW as APPEND",
                        partitionName, handle.getDbName(), handle.getTableName());
                return;
            }
            writeOps.addPartitions(handle.getDbName(), handle.getTableName(),
                    Collections.singletonList(buildSpec(tableInfo, values, location)));
            return;
        }
        if (mode == TUpdateMode.OVERWRITE) {
            if (partitionExists(handle, values)) {
                writeOps.dropPartition(handle.getDbName(), handle.getTableName(), values, false);
            }
            writeOps.addPartitions(handle.getDbName(), handle.getTableName(),
                    Collections.singletonList(buildSpec(tableInfo, values, location)));
            return;
        }
        throw new DorisConnectorException(
                "Unsupported update mode " + mode + " for partitioned hive table "
                        + handle.getDbName() + "." + handle.getTableName());
    }

    private boolean partitionExists(HiveTableHandle handle, List<String> values) {
        try {
            HmsPartitionInfo p = readClient.getPartition(handle.getDbName(), handle.getTableName(), values);
            return p != null;
        } catch (RuntimeException e) {
            return false;
        }
    }

    private static HmsPartitionSpec buildSpec(HmsTableInfo tableInfo,
            List<String> values, String location) {
        return new HmsPartitionSpec(values, location,
                tableInfo.getInputFormat(),
                tableInfo.getOutputFormat(),
                tableInfo.getSerializationLib(),
                new HashMap<>());
    }

    private static String locationOf(THivePartitionUpdate update) {
        THiveLocationParams loc = update.getLocation();
        if (loc == null) {
            return null;
        }
        return loc.getTargetPath() != null ? loc.getTargetPath() : loc.getWritePath();
    }

    /**
     * Parses {@code "k1=v1/k2=v2"} into {@code [v1, v2]}; mirrors
     * {@code HiveUtil.toPartitionValues} from fe-core but kept local to
     * avoid the dependency.
     */
    static List<String> parsePartitionValues(String partitionName) {
        if (partitionName == null || partitionName.isEmpty()) {
            return Collections.emptyList();
        }
        String[] parts = partitionName.split("/");
        List<String> out = new ArrayList<>(parts.length);
        for (String p : parts) {
            int idx = p.indexOf('=');
            if (idx < 0) {
                throw new DorisConnectorException(
                        "Malformed partition name '" + partitionName + "': segment '" + p
                                + "' is missing '='");
            }
            out.add(p.substring(idx + 1));
        }
        return out;
    }

    /**
     * Mirrors {@code HMSTransaction#finishInsertTable} pre-amble: when
     * INSERT OVERWRITE on an unpartitioned table produced zero partition
     * updates (empty source), inject one empty {@link THivePartitionUpdate}
     * so the OVERWRITE branch still fires.
     */
    private static List<THivePartitionUpdate> synthesiseEmptyOverwriteIfNeeded(
            List<THivePartitionUpdate> updates, WriteIntent intent,
            boolean partitioned, HmsTableInfo tableInfo) {
        if (!updates.isEmpty() || partitioned
                || intent.overwriteMode() != WriteIntent.OverwriteMode.FULL_TABLE) {
            return updates;
        }
        THivePartitionUpdate empty = new THivePartitionUpdate();
        empty.setUpdateMode(TUpdateMode.OVERWRITE);
        empty.setRowCount(0);
        empty.setFileSize(0);
        empty.setFileNames(new ArrayList<>());
        THiveLocationParams loc = new THiveLocationParams();
        loc.setWritePath(tableInfo.getLocation());
        loc.setTargetPath(tableInfo.getLocation());
        empty.setLocation(loc);
        return new ArrayList<>(Arrays.asList(empty));
    }

    // visible for test
    HmsWriteOps writeOps() {
        return writeOps;
    }

    // visible for test — keeps the signature stable across refactors
    static Map<String, String> emptyParameters() {
        return Collections.emptyMap();
    }
}
