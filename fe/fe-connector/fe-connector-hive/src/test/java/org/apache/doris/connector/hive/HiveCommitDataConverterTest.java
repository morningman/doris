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
import org.apache.doris.connector.hms.HmsAcidOperation;
import org.apache.doris.thrift.THiveLocationParams;
import org.apache.doris.thrift.THivePartitionUpdate;
import org.apache.doris.thrift.TUpdateMode;

import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class HiveCommitDataConverterTest {

    private static THivePartitionUpdate buildUpdate(String name, TUpdateMode mode, long rows, long bytes) {
        THivePartitionUpdate u = new THivePartitionUpdate();
        u.setName(name);
        u.setUpdateMode(mode);
        u.setRowCount(rows);
        u.setFileSize(bytes);
        u.setFileNames(new java.util.ArrayList<>(Arrays.asList("000000_0")));
        THiveLocationParams loc = new THiveLocationParams();
        loc.setWritePath("file:///x");
        loc.setTargetPath("file:///x");
        u.setLocation(loc);
        return u;
    }

    @Test
    void decodeFragmentsRoundTripsThriftPayload() throws Exception {
        TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
        byte[] payload = serializer.serialize(buildUpdate("dt=1", TUpdateMode.NEW, 5, 50));
        List<THivePartitionUpdate> updates = HiveCommitDataConverter.decodeFragments(
                Collections.singletonList(payload));
        Assertions.assertEquals(1, updates.size());
        Assertions.assertEquals("dt=1", updates.get(0).getName());
        Assertions.assertEquals(TUpdateMode.NEW, updates.get(0).getUpdateMode());
        Assertions.assertEquals(5L, updates.get(0).getRowCount());
    }

    @Test
    void decodeEmptyFragmentsReturnsEmptyList() {
        Assertions.assertTrue(HiveCommitDataConverter.decodeFragments(null).isEmpty());
        Assertions.assertTrue(HiveCommitDataConverter.decodeFragments(Collections.emptyList()).isEmpty());
    }

    @Test
    void decodeRejectsEmptyPayload() {
        Assertions.assertThrows(DorisConnectorException.class,
                () -> HiveCommitDataConverter.decodeFragments(Collections.singletonList(new byte[0])));
    }

    @Test
    void mergeByPartitionSumsCountsAndConcatenatesFiles() {
        THivePartitionUpdate a = buildUpdate("dt=1", TUpdateMode.APPEND, 3, 30);
        THivePartitionUpdate b = buildUpdate("dt=1", TUpdateMode.APPEND, 4, 40);
        b.setFileNames(new java.util.ArrayList<>(Arrays.asList("000001_0")));
        THivePartitionUpdate other = buildUpdate("dt=2", TUpdateMode.NEW, 1, 10);
        List<THivePartitionUpdate> merged = HiveCommitDataConverter.mergeByPartition(Arrays.asList(a, b, other));
        Assertions.assertEquals(2, merged.size());
        THivePartitionUpdate mergedDt1 = merged.stream().filter(u -> "dt=1".equals(u.getName()))
                .findFirst().orElseThrow();
        Assertions.assertEquals(7L, mergedDt1.getRowCount());
        Assertions.assertEquals(70L, mergedDt1.getFileSize());
        Assertions.assertEquals(2, mergedDt1.getFileNames().size());
    }

    @Test
    void deltaDirectoryNamingFollowsHiveAcidV2() {
        Assertions.assertEquals("delta_0000005_0000005_0001",
                HiveAcidUtil.deltaDirectory(HmsAcidOperation.INSERT, 5L, 1));
        Assertions.assertEquals("delete_delta_0000007_0000007_0000",
                HiveAcidUtil.deltaDirectory(HmsAcidOperation.DELETE, 7L, 0));
        Assertions.assertEquals("delta_0000007_0000007_0002",
                HiveAcidUtil.deltaDirectory(HmsAcidOperation.UPDATE, 7L, 2));
    }

    @Test
    void isFullAcidTableDistinguishesFullFromInsertOnly() {
        Map<String, String> full = new HashMap<>();
        full.put(HiveAcidUtil.TRANSACTIONAL, "true");
        Assertions.assertTrue(HiveAcidUtil.isFullAcidTable(full));

        Map<String, String> insertOnly = new HashMap<>();
        insertOnly.put(HiveAcidUtil.TRANSACTIONAL, "true");
        insertOnly.put(HiveAcidUtil.TRANSACTIONAL_PROPERTIES, "insert_only");
        Assertions.assertFalse(HiveAcidUtil.isFullAcidTable(insertOnly));

        Map<String, String> nonAcid = new HashMap<>();
        nonAcid.put(HiveAcidUtil.TRANSACTIONAL, "false");
        Assertions.assertFalse(HiveAcidUtil.isFullAcidTable(nonAcid));
        Assertions.assertFalse(HiveAcidUtil.isFullAcidTable((Map<String, String>) null));
    }

    @Test
    void parsePartitionValuesParsesMultiKeyName() {
        Assertions.assertEquals(Arrays.asList("2024-01-01", "us"),
                HiveCommitDriver.parsePartitionValues("dt=2024-01-01/region=us"));
    }

    @Test
    void parsePartitionValuesRejectsMalformedSegment() {
        Assertions.assertThrows(DorisConnectorException.class,
                () -> HiveCommitDriver.parsePartitionValues("dt-2024"));
    }
}
