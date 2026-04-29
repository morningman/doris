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

import org.apache.doris.connector.hms.HmsAcidOperation;
import org.apache.doris.connector.hms.HmsTableInfo;

import java.util.Locale;
import java.util.Map;

/**
 * Hive ACID helpers: table-level ACID detection + delta / delete-delta
 * directory naming used by the BE writer when staging row-level changes.
 *
 * <p>Naming follows Hive ACID v2:</p>
 * <ul>
 *   <li>{@code delta_<minWriteId>_<maxWriteId>_<stmtId>} for INSERT
 *       and the insert-side of UPDATE / MERGE</li>
 *   <li>{@code delete_delta_<minWriteId>_<maxWriteId>_<stmtId>} for
 *       DELETE and the delete-side of UPDATE / MERGE; bucket files
 *       inside carry the {@code (operation, originalTransaction,
 *       bucket, rowId, currentTransaction)} tuple BE produces.</li>
 * </ul>
 */
public final class HiveAcidUtil {

    /** Hive table parameter holding the {@code transactional} flag. */
    public static final String TRANSACTIONAL = "transactional";
    /** Hive table parameter holding the optional ACID variant (e.g. {@code insert_only}). */
    public static final String TRANSACTIONAL_PROPERTIES = "transactional_properties";

    private HiveAcidUtil() {
    }

    /**
     * Returns {@code true} iff the table parameters mark the table as
     * a Hive ACID transactional table (full ACID — {@code insert_only}
     * tables are not yet supported on the plugin write path and are
     * treated as non-ACID by callers).
     */
    public static boolean isFullAcidTable(Map<String, String> tableParameters) {
        if (tableParameters == null) {
            return false;
        }
        String transactional = tableParameters.get(TRANSACTIONAL);
        if (transactional == null || !"true".equalsIgnoreCase(transactional)) {
            return false;
        }
        String props = tableParameters.get(TRANSACTIONAL_PROPERTIES);
        return props == null || !"insert_only".equalsIgnoreCase(props);
    }

    /** Convenience overload for {@link HmsTableInfo}. */
    public static boolean isFullAcidTable(HmsTableInfo tableInfo) {
        return tableInfo != null && isFullAcidTable(tableInfo.getParameters());
    }

    /**
     * Builds the relative directory name for an ACID write under a
     * given write id and statement id.
     *
     * @param op delete-side ops produce {@code delete_delta_*}, INSERT
     *           and the insert-side of UPDATE / MERGE produce
     *           {@code delta_*}
     */
    public static String deltaDirectory(HmsAcidOperation op, long writeId, int stmtId) {
        String prefix = op == HmsAcidOperation.DELETE ? "delete_delta_" : "delta_";
        return String.format(Locale.ROOT, "%s%07d_%07d_%04d", prefix, writeId, writeId, stmtId);
    }
}
