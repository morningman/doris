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
import org.apache.doris.thrift.THivePartitionUpdate;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Decodes the BE → FE write fragments (binary thrift
 * {@link THivePartitionUpdate} payloads) into in-memory objects, and
 * provides the merge logic the legacy {@code HMSTransaction#mergePartitions}
 * applied before driving HMS commit.
 *
 * <p>The wire format is a single {@link THivePartitionUpdate} per
 * fragment serialized with {@link TBinaryProtocol}; both sides depend
 * on {@code gensrc/thrift/DataSinks.thrift} so version negotiation is
 * not required at this layer (kept aligned with §2.6 of D1-write-path:
 * "no thrift change for M0–M4").</p>
 */
public final class HiveCommitDataConverter {

    private HiveCommitDataConverter() {
    }

    /**
     * Decodes a (possibly empty) collection of fragments. Empty is
     * tolerated — Hive non-ACID INSERT OVERWRITE on an empty source
     * still needs an entry to drive the table-overwrite branch — but
     * is reported up-stack so callers can fast-path the no-op case.
     */
    public static List<THivePartitionUpdate> decodeFragments(Collection<byte[]> fragments) {
        if (fragments == null || fragments.isEmpty()) {
            return new ArrayList<>();
        }
        List<THivePartitionUpdate> out = new ArrayList<>(fragments.size());
        TDeserializer deserializer = newDeserializer();
        int index = 0;
        for (byte[] payload : fragments) {
            if (payload == null || payload.length == 0) {
                throw new DorisConnectorException(
                        "Hive commit fragment #" + index + " is null or empty");
            }
            THivePartitionUpdate update = new THivePartitionUpdate();
            try {
                deserializer.deserialize(update, payload);
            } catch (TException e) {
                throw new DorisConnectorException(
                        "Failed to deserialize hive commit fragment #" + index, e);
            }
            out.add(update);
            index++;
        }
        return out;
    }

    private static TDeserializer newDeserializer() {
        try {
            return new TDeserializer(new TBinaryProtocol.Factory());
        } catch (TException e) {
            throw new DorisConnectorException("Failed to create thrift deserializer for THivePartitionUpdate", e);
        }
    }

    /**
     * Mirrors {@code HMSTransaction#mergePartitions} from the legacy
     * fe-core path: collapse multiple updates targeting the same
     * partition (one per BE shard) into a single update with summed
     * row-counts / file-sizes and concatenated file lists.
     */
    public static List<THivePartitionUpdate> mergeByPartition(List<THivePartitionUpdate> updates) {
        Map<String, THivePartitionUpdate> merged = new HashMap<>();
        for (THivePartitionUpdate update : updates) {
            String key = update.getName() == null ? "" : update.getName();
            THivePartitionUpdate existing = merged.get(key);
            if (existing == null) {
                merged.put(key, update);
                continue;
            }
            existing.setFileSize(existing.getFileSize() + update.getFileSize());
            existing.setRowCount(existing.getRowCount() + update.getRowCount());
            if (existing.getFileNames() == null) {
                existing.setFileNames(new ArrayList<>());
            }
            if (update.getFileNames() != null) {
                existing.getFileNames().addAll(update.getFileNames());
            }
            if (existing.getS3MpuPendingUploads() != null && update.getS3MpuPendingUploads() != null) {
                existing.getS3MpuPendingUploads().addAll(update.getS3MpuPendingUploads());
            }
        }
        return new ArrayList<>(merged.values());
    }
}
