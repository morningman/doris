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

import org.apache.doris.connector.api.event.EventCursor;

/**
 * {@link EventCursor} implementation backed by the monotonically
 * increasing HMS notification event id.
 */
public final class HiveEventCursor implements EventCursor {

    private final long eventId;

    public HiveEventCursor(long eventId) {
        this.eventId = eventId;
    }

    public long getEventId() {
        return eventId;
    }

    @Override
    public int compareTo(EventCursor other) {
        if (!(other instanceof HiveEventCursor)) {
            throw new IllegalArgumentException(
                    "cannot compare HiveEventCursor against " + other.getClass().getName());
        }
        return Long.compare(this.eventId, ((HiveEventCursor) other).eventId);
    }

    @Override
    public String describe() {
        return "HiveEventCursor{eventId=" + eventId + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HiveEventCursor)) {
            return false;
        }
        return eventId == ((HiveEventCursor) o).eventId;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(eventId);
    }

    @Override
    public String toString() {
        return describe();
    }
}
