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

package org.apache.doris.connector.hive.event;

import org.apache.doris.connector.api.event.ConnectorMetaChangeEvent;
import org.apache.doris.connector.api.event.PartitionSpec;
import org.apache.doris.connector.hms.HmsNotificationEvent;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.AddPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.DropPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.json.JSONMessageDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Translates HMS {@link HmsNotificationEvent}s into the SPI-level
 * {@link ConnectorMetaChangeEvent} sealed hierarchy.
 *
 * <p>Mirrors the legacy fe-core {@code MetastoreEventFactory} +
 * {@code CreateTableEvent / DropTableEvent / ...} chain, but produces
 * SPI events directly so the engine dispatcher (see
 * {@code ConnectorEventDispatcher}) can route them through the plugin
 * path. Unknown / unhandled HMS event types are mapped to
 * {@link ConnectorMetaChangeEvent.VendorEvent} so that the engine still
 * advances its cursor and bookkeeping.</p>
 *
 * <p>Partition payloads (ADD/DROP/ALTER_PARTITION) are decoded via an
 * injectable {@link PartitionPayloadDecoder} so unit tests can stub out
 * the Hive JSON message format.</p>
 */
final class HiveEventTranslator {

    private static final Logger LOG = LogManager.getLogger(HiveEventTranslator.class);

    static final String TYPE_CREATE_TABLE = "CREATE_TABLE";
    static final String TYPE_DROP_TABLE = "DROP_TABLE";
    static final String TYPE_ALTER_TABLE = "ALTER_TABLE";
    static final String TYPE_CREATE_DATABASE = "CREATE_DATABASE";
    static final String TYPE_DROP_DATABASE = "DROP_DATABASE";
    static final String TYPE_ALTER_DATABASE = "ALTER_DATABASE";
    static final String TYPE_ADD_PARTITION = "ADD_PARTITION";
    static final String TYPE_DROP_PARTITION = "DROP_PARTITION";
    static final String TYPE_ALTER_PARTITION = "ALTER_PARTITION";
    static final String TYPE_INSERT = "INSERT";

    private final String catalogName;
    private final PartitionPayloadDecoder partitionDecoder;

    HiveEventTranslator(String catalogName) {
        this(catalogName, new HivePartitionPayloadDecoder());
    }

    HiveEventTranslator(String catalogName, PartitionPayloadDecoder partitionDecoder) {
        this.catalogName = Objects.requireNonNull(catalogName, "catalogName");
        this.partitionDecoder = Objects.requireNonNull(partitionDecoder, "partitionDecoder");
    }

    /**
     * Translate a single HMS notification event to one or more SPI
     * events. Multi-partition ADD/DROP events expand to one SPI event
     * per partition (all sharing the same eventId, which is permitted
     * by {@code EventBatch}'s ascending invariant).
     *
     * <p>If translation of the payload fails (e.g. malformed JSON), a
     * {@link ConnectorMetaChangeEvent.VendorEvent} is emitted as a
     * fallback so the cursor still advances.</p>
     */
    List<ConnectorMetaChangeEvent> translate(HmsNotificationEvent event) {
        Objects.requireNonNull(event, "event");
        String type = event.getEventType();
        if (type == null) {
            return List.of(buildVendor(event, "UNKNOWN_EVENT_TYPE", "null event type"));
        }
        Instant ts = Instant.ofEpochSecond(event.getEventTimeSeconds());
        String dbName = event.getDbName();
        String tblName = event.getTableName();
        try {
            switch (type) {
                case TYPE_CREATE_DATABASE:
                    return List.of(new ConnectorMetaChangeEvent.DatabaseCreated(
                            event.getEventId(), ts, catalogName, requireDb(dbName, type), type));
                case TYPE_DROP_DATABASE:
                    return List.of(new ConnectorMetaChangeEvent.DatabaseDropped(
                            event.getEventId(), ts, catalogName, requireDb(dbName, type), type));
                case TYPE_ALTER_DATABASE:
                    return List.of(new ConnectorMetaChangeEvent.DatabaseAltered(
                            event.getEventId(), ts, catalogName, requireDb(dbName, type), type));
                case TYPE_CREATE_TABLE:
                    return List.of(new ConnectorMetaChangeEvent.TableCreated(
                            event.getEventId(), ts, catalogName,
                            requireDb(dbName, type), requireTbl(tblName, type), type));
                case TYPE_DROP_TABLE:
                    return List.of(new ConnectorMetaChangeEvent.TableDropped(
                            event.getEventId(), ts, catalogName,
                            requireDb(dbName, type), requireTbl(tblName, type), type));
                case TYPE_ALTER_TABLE:
                    return translateAlterTable(event, ts);
                case TYPE_INSERT:
                    return List.of(new ConnectorMetaChangeEvent.DataChanged(
                            event.getEventId(), ts, catalogName,
                            requireDb(dbName, type), requireTbl(tblName, type),
                            Optional.empty(), Optional.empty(), type));
                case TYPE_ADD_PARTITION:
                    return translatePartitions(event, ts, PartitionOp.ADD);
                case TYPE_DROP_PARTITION:
                    return translatePartitions(event, ts, PartitionOp.DROP);
                case TYPE_ALTER_PARTITION:
                    return translatePartitions(event, ts, PartitionOp.ALTER);
                default:
                    return List.of(buildVendor(event, type, "unhandled HMS event type"));
            }
        } catch (RuntimeException e) {
            LOG.warn("HiveEventTranslator failed on event {} (type={}); emitting VendorEvent fallback",
                    event.getEventId(), type, e);
            return List.of(buildVendor(event, type, "translation failed: " + e.getMessage()));
        }
    }

    private List<ConnectorMetaChangeEvent> translateAlterTable(HmsNotificationEvent event, Instant ts) {
        String dbName = event.getDbName();
        String tblName = event.getTableName();
        AlterTableInfo info = partitionDecoder.decodeAlterTable(event);
        if (info != null && info.isRename()) {
            return List.of(new ConnectorMetaChangeEvent.TableRenamed(
                    event.getEventId(), ts, catalogName,
                    requireDb(info.dbBefore != null ? info.dbBefore : dbName, TYPE_ALTER_TABLE),
                    requireTbl(info.tblBefore != null ? info.tblBefore : tblName, TYPE_ALTER_TABLE),
                    info.tblAfter,
                    TYPE_ALTER_TABLE));
        }
        return List.of(new ConnectorMetaChangeEvent.TableAltered(
                event.getEventId(), ts, catalogName,
                requireDb(dbName, TYPE_ALTER_TABLE),
                requireTbl(tblName, TYPE_ALTER_TABLE),
                TYPE_ALTER_TABLE));
    }

    private List<ConnectorMetaChangeEvent> translatePartitions(HmsNotificationEvent event,
                                                                Instant ts, PartitionOp op) {
        String db = requireDb(event.getDbName(), event.getEventType());
        String tbl = requireTbl(event.getTableName(), event.getEventType());
        List<LinkedHashMap<String, String>> specs = partitionDecoder.decodePartitions(event, op);
        if (specs.isEmpty()) {
            return List.of(buildVendor(event, event.getEventType(), "no partitions decoded"));
        }
        List<ConnectorMetaChangeEvent> out = new ArrayList<>(specs.size());
        for (LinkedHashMap<String, String> kv : specs) {
            PartitionSpec spec = new PartitionSpec(kv);
            switch (op) {
                case ADD:
                    out.add(new ConnectorMetaChangeEvent.PartitionAdded(
                            event.getEventId(), ts, catalogName, db, tbl, spec, event.getEventType()));
                    break;
                case DROP:
                    out.add(new ConnectorMetaChangeEvent.PartitionDropped(
                            event.getEventId(), ts, catalogName, db, tbl, spec, event.getEventType()));
                    break;
                case ALTER:
                default:
                    out.add(new ConnectorMetaChangeEvent.PartitionAltered(
                            event.getEventId(), ts, catalogName, db, tbl, spec, event.getEventType()));
                    break;
            }
        }
        return out;
    }

    private ConnectorMetaChangeEvent buildVendor(HmsNotificationEvent event, String type, String reason) {
        return new ConnectorMetaChangeEvent.VendorEvent(
                event.getEventId(),
                Instant.ofEpochSecond(event.getEventTimeSeconds()),
                catalogName,
                Optional.ofNullable(event.getDbName()),
                Optional.ofNullable(event.getTableName()),
                "hive",
                Collections.singletonMap("hmsType", type == null ? "" : type),
                reason);
    }

    private static String requireDb(String db, String type) {
        if (db == null || db.isEmpty()) {
            throw new IllegalArgumentException("HMS event " + type + " missing dbName");
        }
        return db.toLowerCase(Locale.ROOT);
    }

    private static String requireTbl(String tbl, String type) {
        if (tbl == null || tbl.isEmpty()) {
            throw new IllegalArgumentException("HMS event " + type + " missing tableName");
        }
        return tbl.toLowerCase(Locale.ROOT);
    }

    enum PartitionOp { ADD, DROP, ALTER }

    /**
     * Decodes the JSON payload of partition / alter-table notification
     * events. Default implementation uses Hive's
     * {@link JSONMessageDeserializer}; tests inject a fake.
     */
    interface PartitionPayloadDecoder {
        List<LinkedHashMap<String, String>> decodePartitions(HmsNotificationEvent event, PartitionOp op);

        AlterTableInfo decodeAlterTable(HmsNotificationEvent event);
    }

    /** Result of decoding an ALTER_TABLE message. */
    static final class AlterTableInfo {
        final String dbBefore;
        final String tblBefore;
        final String dbAfter;
        final String tblAfter;

        AlterTableInfo(String dbBefore, String tblBefore, String dbAfter, String tblAfter) {
            this.dbBefore = dbBefore;
            this.tblBefore = tblBefore;
            this.dbAfter = dbAfter;
            this.tblAfter = tblAfter;
        }

        boolean isRename() {
            if (tblBefore == null || tblAfter == null) {
                return false;
            }
            if (!tblBefore.equalsIgnoreCase(tblAfter)) {
                return true;
            }
            if (dbBefore == null || dbAfter == null) {
                return false;
            }
            return !dbBefore.equalsIgnoreCase(dbAfter);
        }
    }

    /** Default JSON-based decoder backed by Hive's message deserializer. */
    static final class HivePartitionPayloadDecoder implements PartitionPayloadDecoder {

        private static final MessageDeserializer JSON_DESERIALIZER = new JSONMessageDeserializer();

        @Override
        public List<LinkedHashMap<String, String>> decodePartitions(HmsNotificationEvent event, PartitionOp op) {
            String message = event.getMessage();
            if (message == null || message.isEmpty()) {
                return Collections.emptyList();
            }
            try {
                switch (op) {
                    case ADD: {
                        AddPartitionMessage msg = JSON_DESERIALIZER.getAddPartitionMessage(message);
                        Table tbl = msg.getTableObj();
                        List<String> keys = partitionKeyNames(tbl);
                        List<LinkedHashMap<String, String>> out = new ArrayList<>();
                        Iterable<Partition> parts = msg.getPartitionObjs();
                        if (parts != null) {
                            for (Partition p : parts) {
                                out.add(toSpec(keys, p.getValues()));
                            }
                        }
                        return out;
                    }
                    case DROP: {
                        DropPartitionMessage msg = JSON_DESERIALIZER.getDropPartitionMessage(message);
                        List<LinkedHashMap<String, String>> out = new ArrayList<>();
                        List<java.util.Map<String, String>> dropped = msg.getPartitions();
                        if (dropped != null) {
                            for (java.util.Map<String, String> p : dropped) {
                                out.add(new LinkedHashMap<>(p));
                            }
                        }
                        return out;
                    }
                    case ALTER:
                    default: {
                        AlterPartitionMessage msg = JSON_DESERIALIZER.getAlterPartitionMessage(message);
                        Table tbl = msg.getTableObj();
                        Partition after = msg.getPtnObjAfter();
                        List<String> keys = partitionKeyNames(tbl);
                        if (after == null) {
                            return Collections.emptyList();
                        }
                        return Collections.singletonList(toSpec(keys, after.getValues()));
                    }
                }
            } catch (Exception e) {
                LOG.debug("decodePartitions failed for event {}", event.getEventId(), e);
                return Collections.emptyList();
            }
        }

        @Override
        public AlterTableInfo decodeAlterTable(HmsNotificationEvent event) {
            String message = event.getMessage();
            if (message == null || message.isEmpty()) {
                return null;
            }
            try {
                AlterTableMessage msg = JSON_DESERIALIZER.getAlterTableMessage(message);
                Table before = msg.getTableObjBefore();
                Table after = msg.getTableObjAfter();
                if (before == null || after == null) {
                    return null;
                }
                return new AlterTableInfo(
                        before.getDbName(), before.getTableName(),
                        after.getDbName(), after.getTableName().toLowerCase(Locale.ROOT));
            } catch (Exception e) {
                LOG.debug("decodeAlterTable failed for event {}", event.getEventId(), e);
                return null;
            }
        }

        private static List<String> partitionKeyNames(Table tbl) {
            if (tbl == null || tbl.getPartitionKeys() == null) {
                return Collections.emptyList();
            }
            return tbl.getPartitionKeys().stream().map(FieldSchema::getName).collect(Collectors.toList());
        }

        private static LinkedHashMap<String, String> toSpec(List<String> keys, List<String> values) {
            LinkedHashMap<String, String> map = new LinkedHashMap<>();
            if (values == null) {
                return map;
            }
            int n = keys == null ? 0 : Math.min(keys.size(), values.size());
            for (int i = 0; i < n; i++) {
                map.put(keys.get(i), values.get(i));
            }
            // when key list is shorter than values, fall back to positional naming
            for (int i = n; i < values.size(); i++) {
                map.put("col" + i, values.get(i));
            }
            return map;
        }
    }
}
