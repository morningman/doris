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

package org.apache.doris.catalog;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.planner.external.kafka.KafkaTopicInspector;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * ES resource
 * <p>
 * Syntax:
 * CREATE RESOURCE "kafka_resource"
 * PROPERTIES
 * (
 * "type" = "es",
 * "hosts" = "http://192.168.0.1:8200,http://192.168.0.2:8200",
 * "index" = "test",
 * "type" = "doc",
 * "user" = "root",
 * "password" = "root"
 * );
 */
public class KafkaResource extends Resource {
    private static final Logger LOG = LogManager.getLogger(KafkaResource.class);

    public static final String KAFKA_PREFIX = "kafka.";

    public static final String BROKER_LIST = "broker_list";
    public static final String TOPIC = "topic";
    public static final String PARTITIONS = "partitions";
    public static final String OFFSETS = "start_offsets";
    public static final String MAX_ROWS = "max_rows";
    public static final String GROUP_ID = "group.id";

    public static final String SECURITY_PROTOCOL = "security.protocol";
    public static final String SASL_MECHANISM = "sasl.mechanism";
    public static final String SASL_JAAS_CONFIG = "sasl.jaas.config";

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    public KafkaResource(String name) {
        super(name, ResourceType.KAFKA);
        properties = Maps.newHashMap();
    }

    @Override
    public void modifyProperties(Map<String, String> properties) throws DdlException {
        for (Map.Entry<String, String> kv : properties.entrySet()) {
            replaceIfEffectiveValue(this.properties, kv.getKey(), kv.getValue());
        }
        super.modifyProperties(properties);
        tryUpdateKafkaPartitionOffsets();
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        this.properties = properties;
        tryUpdateKafkaPartitionOffsets();
    }

    private void tryUpdateKafkaPartitionOffsets() throws DdlException {
        if (Strings.isNullOrEmpty(this.properties.get(PARTITIONS))
                || Strings.isNullOrEmpty(this.properties.get(OFFSETS))) {
            Properties prop = buildProp();
            KafkaTopicInspector inspector = new KafkaTopicInspector(prop);
            Map<TopicPartition, Long> partitionOffsets = inspector.getEarliestOffsets(this.properties.get(TOPIC));
            List<Integer> partitions = Lists.newArrayList();
            List<Long> offsets = Lists.newArrayList();
            for (Map.Entry<TopicPartition, Long> entry : partitionOffsets.entrySet()) {
                partitions.add(entry.getKey().partition());
                offsets.add(entry.getValue());
            }
            this.properties.put(PARTITIONS, Joiner.on(",").join(partitions));
            this.properties.put(OFFSETS, Joiner.on(",").join(offsets));
        }
    }

    private Properties buildProp() {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", properties.get(BROKER_LIST));
        prop.put("enable.auto.commit", "false");
        prop.put("group.id", properties.getOrDefault(GROUP_ID, "doris"));
        if (properties.containsKey(SECURITY_PROTOCOL)) {
            prop.put(SECURITY_PROTOCOL, properties.get(SECURITY_PROTOCOL));
            prop.put(SASL_MECHANISM, properties.get(SASL_MECHANISM));
            prop.put(SASL_JAAS_CONFIG, properties.get(SASL_JAAS_CONFIG));
        }
        return prop;
    }

    @Override
    public Map<String, String> getCopiedProperties() {
        return Maps.newHashMap(properties);
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
        }
    }

    public synchronized void updateOffset(Map<String, Long> newPartitionOffsets) {
        // make old map
        Map<String, Long> oldPartitionOffsets = Maps.newHashMap();
        String[] partitions = properties.get(PARTITIONS).split(",");
        String[] offsets = properties.get(OFFSETS).split(",");
        Preconditions.checkState(partitions.length == offsets.length);
        for (int i = 0; i < partitions.length; i++) {
            oldPartitionOffsets.put(partitions[i], Long.valueOf(offsets[i]));
        }

        // merge old map and new map
        // if old map contains partition, use old offset + new offset
        // else use new offset
        for (Map.Entry<String, Long> entry : newPartitionOffsets.entrySet()) {
            if (oldPartitionOffsets.containsKey(entry.getKey())) {
                oldPartitionOffsets.put(entry.getKey(), oldPartitionOffsets.get(entry.getKey()) + entry.getValue());
            } else {
                oldPartitionOffsets.put(entry.getKey(), entry.getValue());
            }
        }

        // make new map
        Map<String, String> updatedProperties = Maps.newHashMap();
        List<String> newpPartitions = Lists.newArrayList();
        List<Long> newOffsets = Lists.newArrayList();
        for (Map.Entry<String, Long> entry : oldPartitionOffsets.entrySet()) {
            newpPartitions.add(entry.getKey());
            newOffsets.add(entry.getValue());
        }
        updatedProperties.put(PARTITIONS, Joiner.on(",").join(newpPartitions));
        updatedProperties.put(OFFSETS, Joiner.on(",").join(newOffsets));
        LOG.info("kafka resource[{}] update offset to: {}", name, updatedProperties);
        try {
            modifyProperties(updatedProperties);
        } catch (DdlException e) {
            LOG.warn("kafka resource[{}] update offset failed: {}", name, e.getMessage(), e);
        }
    }
}

