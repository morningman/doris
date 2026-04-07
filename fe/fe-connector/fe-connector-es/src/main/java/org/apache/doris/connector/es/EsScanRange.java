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

package org.apache.doris.connector.es;

import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Scan range for Elasticsearch — represents one shard to scan.
 *
 * <p>Each EsScanRange maps to one TEsScanRange in Thrift. It carries
 * the ES shard routing info needed by BE's ES HTTP scanner.</p>
 *
 * <p>Properties include: es.index, es.type, es.shard_id, and
 * es.hosts (comma-separated host:port list).</p>
 */
public class EsScanRange implements ConnectorScanRange {

    private static final long serialVersionUID = 1L;

    public static final String PROP_INDEX = "es.index";
    public static final String PROP_TYPE = "es.type";
    public static final String PROP_SHARD_ID = "es.shard_id";
    public static final String PROP_HOSTS = "es.hosts";

    private final String indexName;
    private final String mappingType;
    private final int shardId;
    private final List<String> esHosts;

    public EsScanRange(String indexName, String mappingType,
            int shardId, List<String> esHosts) {
        this.indexName = indexName;
        this.mappingType = mappingType;
        this.shardId = shardId;
        this.esHosts = esHosts != null
                ? Collections.unmodifiableList(new ArrayList<>(esHosts))
                : Collections.emptyList();
    }

    @Override
    public ConnectorScanRangeType getRangeType() {
        return ConnectorScanRangeType.ES_SCAN;
    }

    @Override
    public List<String> getHosts() {
        return esHosts;
    }

    @Override
    public Map<String, String> getProperties() {
        Map<String, String> props = new HashMap<>();
        props.put(PROP_INDEX, indexName);
        if (mappingType != null) {
            props.put(PROP_TYPE, mappingType);
        }
        props.put(PROP_SHARD_ID, String.valueOf(shardId));
        props.put(PROP_HOSTS, String.join(",", esHosts));
        return props;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getMappingType() {
        return mappingType;
    }

    public int getShardId() {
        return shardId;
    }

    public List<String> getEsHosts() {
        return esHosts;
    }

    @Override
    public String toString() {
        return "EsScanRange{index='" + indexName
                + "', shard=" + shardId
                + ", hosts=" + esHosts + "}";
    }
}
