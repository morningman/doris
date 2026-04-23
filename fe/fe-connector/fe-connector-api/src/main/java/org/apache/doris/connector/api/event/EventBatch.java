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

package org.apache.doris.connector.api.event;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable result of a single
 * {@link EventSourceOps#poll(EventCursor, int, java.time.Duration, EventFilter)}
 * call. Events are guaranteed to be sorted ascending by
 * {@link ConnectorMetaChangeEvent#eventId()}; the constructor enforces
 * this invariant.
 */
public final class EventBatch {

    private final List<ConnectorMetaChangeEvent> events;
    private final EventCursor nextCursor;
    private final boolean hasMore;
    private final Optional<Duration> recommendedNextPollDelay;

    public EventBatch(List<ConnectorMetaChangeEvent> events, EventCursor nextCursor,
                      boolean hasMore, Optional<Duration> recommendedNextPollDelay) {
        Objects.requireNonNull(events, "events");
        Objects.requireNonNull(nextCursor, "nextCursor");
        Objects.requireNonNull(recommendedNextPollDelay, "recommendedNextPollDelay");
        List<ConnectorMetaChangeEvent> copy = List.copyOf(events);
        for (int i = 1; i < copy.size(); i++) {
            if (copy.get(i).eventId() < copy.get(i - 1).eventId()) {
                throw new IllegalArgumentException("events must be sorted ascending by eventId");
            }
        }
        this.events = copy;
        this.nextCursor = nextCursor;
        this.hasMore = hasMore;
        this.recommendedNextPollDelay = recommendedNextPollDelay;
    }

    public static EventBatch empty(EventCursor cursor) {
        return new EventBatch(List.of(), cursor, false, Optional.empty());
    }

    public List<ConnectorMetaChangeEvent> events() {
        return events;
    }

    public EventCursor nextCursor() {
        return nextCursor;
    }

    public boolean hasMore() {
        return hasMore;
    }

    public Optional<Duration> recommendedNextPollDelay() {
        return recommendedNextPollDelay;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final List<ConnectorMetaChangeEvent> events = new ArrayList<>();
        private EventCursor nextCursor;
        private boolean hasMore;
        private Optional<Duration> recommendedNextPollDelay = Optional.empty();

        public Builder add(ConnectorMetaChangeEvent e) {
            events.add(Objects.requireNonNull(e, "e"));
            return this;
        }

        public Builder nextCursor(EventCursor c) {
            this.nextCursor = c;
            return this;
        }

        public Builder hasMore(boolean v) {
            this.hasMore = v;
            return this;
        }

        public Builder recommendedNextPollDelay(Duration d) {
            this.recommendedNextPollDelay = Optional.ofNullable(d);
            return this;
        }

        public EventBatch build() {
            return new EventBatch(events, nextCursor, hasMore, recommendedNextPollDelay);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EventBatch)) {
            return false;
        }
        EventBatch that = (EventBatch) o;
        return hasMore == that.hasMore
                && events.equals(that.events)
                && nextCursor.equals(that.nextCursor)
                && recommendedNextPollDelay.equals(that.recommendedNextPollDelay);
    }

    @Override
    public int hashCode() {
        return Objects.hash(events, nextCursor, hasMore, recommendedNextPollDelay);
    }

    @Override
    public String toString() {
        return "EventBatch{events=" + events.size() + ", nextCursor=" + nextCursor
                + ", hasMore=" + hasMore + ", recommendedNextPollDelay=" + recommendedNextPollDelay + "}";
    }
}
