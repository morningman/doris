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

package org.apache.doris.planner.external.kafka;

import org.apache.doris.common.DdlException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaTopicInspector {

    private final AdminClient adminClient;

    public KafkaTopicInspector(Properties prop) {
        this.adminClient = AdminClient.create(prop);
    }

    public Map<TopicPartition, Long> getEarliestOffsets(String topic) throws DdlException {
        try {
            Map<String, org.apache.kafka.clients.admin.TopicDescription> map = adminClient.describeTopics(
                    Collections.singletonList(topic)).allTopicNames().get();
            List<TopicPartitionInfo> partitions = map.get(topic).partitions();

            Map<TopicPartition, OffsetSpec> request = new HashMap<>();
            for (TopicPartitionInfo partitionInfo : partitions) {
                TopicPartition tp = new TopicPartition(topic, partitionInfo.partition());
                request.put(tp, OffsetSpec.earliest());
            }

            ListOffsetsResult result = adminClient.listOffsets(request);
            Map<TopicPartition, Long> earliestOffsets = new HashMap<>();
            result.all().get().forEach((tp, offset) -> earliestOffsets.put(tp, offset.offset()));

            return earliestOffsets;
        } catch (Exception e) {
            throw new DdlException("failed to get partition offsets: " + e.getMessage(), e);
        }
    }
}

