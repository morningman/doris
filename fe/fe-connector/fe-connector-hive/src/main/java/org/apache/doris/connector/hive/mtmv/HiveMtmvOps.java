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

package org.apache.doris.connector.hive.mtmv;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.mtmv.ConnectorMtmvSnapshot;
import org.apache.doris.connector.api.mtmv.ConnectorPartitionItem;
import org.apache.doris.connector.api.mtmv.ConnectorPartitionType;
import org.apache.doris.connector.api.mtmv.MtmvOps;
import org.apache.doris.connector.api.mtmv.MtmvRefreshHint;
import org.apache.doris.connector.api.timetravel.ConnectorMvccSnapshot;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * D8 — Hive {@link MtmvOps} implementation built on top of {@link HmsClient}.
 *
 * <p>Wraps the SPI-clean HMS client so that materialized views can use Hive
 * tables as base tables without depending on any fe-core type. The
 * {@code PluginDrivenMtmvBridge} in fe-core adapts the values returned here
 * into the legacy {@code MTMVRelatedTableIf} surface.</p>
 *
 * <p>Snapshot semantics mirror the legacy {@code HiveDlaTable}:
 * <ul>
 *   <li>Partition snapshot uses {@code transient_lastDdlTime} from partition
 *       parameters (seconds since epoch, multiplied by 1000), falling back
 *       to the partition's HMS create time when the parameter is absent.</li>
 *   <li>Table snapshot uses {@code transient_lastDdlTime} from table
 *       parameters; missing key yields {@code 0L} (matches legacy behaviour).</li>
 * </ul>
 * </p>
 *
 * <p>Per the M2-06 bridge contract, {@link #isValidRelatedTable} and
 * {@link #needAutoRefresh} never throw — any HMS error is logged and
 * collapsed to {@code false}. Hive partitioning is always reported as
 * {@link ConnectorPartitionType#LIST} when the table has any partition keys
 * (Hive partitions are physically enumerated values, never ranges).</p>
 *
 * <p>SPI gap (tracked in M2-06 handoff): the bridge currently throws
 * {@code UnsupportedOperationException} for {@link ConnectorPartitionItem.ListPartitionItem}
 * because it cannot build typed {@code PartitionKey}s from the SPI's plain
 * string values. The MV-on-hive milestone must close that gap; this class
 * still emits {@link ConnectorPartitionItem.ListPartitionItem} as the
 * SPI-correct representation so that a future bridge enhancement does not
 * need plugin-side changes.</p>
 */
public final class HiveMtmvOps implements MtmvOps {

    private static final Logger LOG = LogManager.getLogger(HiveMtmvOps.class);

    /** HMS table parameter holding the last DDL time in seconds since epoch. */
    static final String TBL_PROP_TRANSIENT_LAST_DDL_TIME = "transient_lastDdlTime";

    /** HMS table parameter marking ACID/transactional tables. */
    static final String TBL_PROP_TRANSACTIONAL = "transactional";

    private final HmsClient hmsClient;

    public HiveMtmvOps(HmsClient hmsClient) {
        this.hmsClient = Objects.requireNonNull(hmsClient, "hmsClient");
    }

    @Override
    public Map<String, ConnectorPartitionItem> listPartitions(
            String database, String table, Optional<ConnectorMvccSnapshot> snapshot) {
        HmsTableInfo tableInfo = hmsClient.getTable(database, table);
        List<ConnectorColumn> partKeys = tableInfo.getPartitionKeys();
        if (partKeys == null || partKeys.isEmpty()) {
            return Collections.singletonMap("",
                    new ConnectorPartitionItem.UnpartitionedItem());
        }
        List<String> partNames = hmsClient.listPartitionNames(database, table, Integer.MAX_VALUE);
        if (partNames.isEmpty()) {
            return Collections.emptyMap();
        }
        List<HmsPartitionInfo> infos = hmsClient.getPartitions(database, table, partNames);
        Map<String, ConnectorPartitionItem> out = new LinkedHashMap<>(infos.size());
        for (HmsPartitionInfo info : infos) {
            String name = buildPartitionName(partKeys, info.getValues());
            // Hive partitions are physical enumerations of values: each HMS
            // partition becomes a single-tuple list partition.
            List<List<String>> values = Collections.singletonList(
                    Collections.unmodifiableList(info.getValues()));
            out.put(name, new ConnectorPartitionItem.ListPartitionItem(values));
        }
        return out;
    }

    @Override
    public ConnectorPartitionType getPartitionType(
            String database, String table, Optional<ConnectorMvccSnapshot> snapshot) {
        List<ConnectorColumn> partKeys = hmsClient.getTable(database, table).getPartitionKeys();
        if (partKeys == null || partKeys.isEmpty()) {
            return ConnectorPartitionType.UNPARTITIONED;
        }
        return ConnectorPartitionType.LIST;
    }

    @Override
    public Set<String> getPartitionColumnNames(
            String database, String table, Optional<ConnectorMvccSnapshot> snapshot) {
        List<ConnectorColumn> partKeys = hmsClient.getTable(database, table).getPartitionKeys();
        if (partKeys == null || partKeys.isEmpty()) {
            return Collections.emptySet();
        }
        return partKeys.stream()
                .map(c -> c.getName().toLowerCase())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @Override
    public List<ConnectorColumn> getPartitionColumns(
            String database, String table, Optional<ConnectorMvccSnapshot> snapshot) {
        List<ConnectorColumn> partKeys = hmsClient.getTable(database, table).getPartitionKeys();
        if (partKeys == null) {
            return Collections.emptyList();
        }
        return partKeys;
    }

    @Override
    public ConnectorMtmvSnapshot getPartitionSnapshot(
            String database, String table, String partitionName,
            MtmvRefreshHint hint, Optional<ConnectorMvccSnapshot> snapshot) {
        Objects.requireNonNull(partitionName, "partitionName");
        HmsTableInfo tableInfo = hmsClient.getTable(database, table);
        List<String> values = parsePartitionValues(partitionName, tableInfo.getPartitionKeys());
        HmsPartitionInfo partition = hmsClient.getPartition(database, table, values);
        long ddlTimeSec = readLongParameter(partition.getParameters(),
                TBL_PROP_TRANSIENT_LAST_DDL_TIME, partition.getCreateTime());
        return new ConnectorMtmvSnapshot.TimestampMtmvSnapshot(ddlTimeSec * 1000L);
    }

    @Override
    public ConnectorMtmvSnapshot getTableSnapshot(
            String database, String table,
            MtmvRefreshHint hint, Optional<ConnectorMvccSnapshot> snapshot) {
        long ddlMillis = readTableLastDdlMillis(database, table);
        return new ConnectorMtmvSnapshot.MaxTimestampMtmvSnapshot(ddlMillis);
    }

    @Override
    public long getNewestUpdateVersionOrTime(String database, String table) {
        return readTableLastDdlMillis(database, table);
    }

    @Override
    public boolean isPartitionColumnAllowNull(String database, String table) {
        // Hive renders NULL partition values as the sentinel
        // "__HIVE_DEFAULT_PARTITION__"; partition columns always allow NULL.
        return true;
    }

    @Override
    public boolean isValidRelatedTable(String database, String table) {
        try {
            HmsTableInfo info = hmsClient.getTable(database, table);
            Map<String, String> params = info.getParameters();
            if (params != null) {
                String txn = params.get(TBL_PROP_TRANSACTIONAL);
                if (txn != null && "true".equalsIgnoreCase(txn)) {
                    return false;
                }
            }
            return true;
        } catch (RuntimeException e) {
            LOG.debug("isValidRelatedTable({}.{}) returning false due to error: {}",
                    database, table, e.getMessage());
            return false;
        }
    }

    @Override
    public boolean needAutoRefresh(String database, String table) {
        // Hive partition mutations are the common case; refresh is always desirable.
        return true;
    }

    // ---------------------------------------------------------------- helpers

    private long readTableLastDdlMillis(String database, String table) {
        HmsTableInfo info = hmsClient.getTable(database, table);
        return readLongParameter(info.getParameters(),
                TBL_PROP_TRANSIENT_LAST_DDL_TIME, 0) * 1000L;
    }

    private static long readLongParameter(Map<String, String> params, String key, long defaultValue) {
        if (params == null) {
            return defaultValue;
        }
        String raw = params.get(key);
        if (raw == null || raw.isEmpty()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException e) {
            LOG.debug("Cannot parse parameter '{}'='{}' as long, using default {}", key, raw, defaultValue);
            return defaultValue;
        }
    }

    private static String buildPartitionName(List<ConnectorColumn> partKeys, List<String> values) {
        StringBuilder sb = new StringBuilder();
        int n = Math.min(partKeys.size(), values.size());
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                sb.append('/');
            }
            sb.append(partKeys.get(i).getName()).append('=').append(values.get(i));
        }
        return sb.toString();
    }

    private static List<String> parsePartitionValues(String partitionName, List<ConnectorColumn> partKeys) {
        if (partitionName.isEmpty()) {
            return Collections.emptyList();
        }
        String[] parts = partitionName.split("/");
        ArrayList<String> values = new ArrayList<>(parts.length);
        for (String part : parts) {
            int eq = part.indexOf('=');
            if (eq < 0) {
                values.add(part);
            } else {
                values.add(part.substring(eq + 1));
            }
        }
        if (partKeys != null && !partKeys.isEmpty() && values.size() != partKeys.size()) {
            throw new IllegalArgumentException("partition name '" + partitionName
                    + "' does not match " + partKeys.size() + " partition columns");
        }
        return values;
    }
}
