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

package org.apache.doris.connector.hms;

import java.util.Objects;

/**
 * SPI-clean wrapper around an HMS {@code NotificationEvent}. Captures
 * just the fields needed for downstream translation into the connector
 * event SPI; concrete Hive Thrift types do not leak past this class.
 */
public final class HmsNotificationEvent {

    private final long eventId;
    private final int eventTimeSeconds;
    private final String eventType;
    private final String dbName;
    private final String tableName;
    private final String message;
    private final String messageFormat;

    public HmsNotificationEvent(long eventId, int eventTimeSeconds, String eventType,
                                String dbName, String tableName,
                                String message, String messageFormat) {
        this.eventId = eventId;
        this.eventTimeSeconds = eventTimeSeconds;
        this.eventType = Objects.requireNonNull(eventType, "eventType");
        this.dbName = dbName;
        this.tableName = tableName;
        this.message = message;
        this.messageFormat = messageFormat;
    }

    public long getEventId() {
        return eventId;
    }

    /** HMS notification event time, in seconds since the unix epoch. */
    public int getEventTimeSeconds() {
        return eventTimeSeconds;
    }

    public String getEventType() {
        return eventType;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getMessage() {
        return message;
    }

    public String getMessageFormat() {
        return messageFormat;
    }

    @Override
    public String toString() {
        return "HmsNotificationEvent{eventId=" + eventId + ", type=" + eventType
                + ", db=" + dbName + ", tbl=" + tableName + "}";
    }
}
