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
import java.util.Optional;

/**
 * Connector-side event source. Used by the engine dispatcher to pull
 * metadata change events from an external system (e.g. HMS notification
 * log, Iceberg snapshot history, Kafka topic, ...).
 *
 * <p>Connectors that do not produce events return {@link #NONE}.
 * Connectors that drive their own event loop return
 * {@link #isSelfManaged()} {@code = true} and call
 * {@link org.apache.doris.connector.spi.ConnectorContext#publishExternalEvent(ConnectorMetaChangeEvent)}
 * directly.</p>
 */
public interface EventSourceOps {

    /** No-op singleton returned by {@code ConnectorMetadata#getEventSourceOps()} by default. */
    EventSourceOps NONE = new NoOpEventSourceOps();

    /** Initial cursor to start polling from (e.g. "now"). Empty means no event history available. */
    Optional<EventCursor> initialCursor();

    /**
     * Poll the next batch of events.
     *
     * @param cursor    last persisted cursor; plugin returns events strictly after this position
     * @param maxEvents soft cap on returned event count
     * @param timeout   max time to block waiting for new events
     * @param filter    engine-supplied filter
     * @return a batch (possibly empty) and the new cursor to persist
     */
    EventBatch poll(EventCursor cursor, int maxEvents, Duration timeout, EventFilter filter)
            throws EventSourceException;

    /** Decode a previously persisted cursor blob. */
    EventCursor parseCursor(byte[] persisted);

    /** Encode a cursor for engine-side persistence. */
    byte[] serializeCursor(EventCursor cursor);

    /**
     * Whether this source manages its own event loop / dispatch and
     * publishes via
     * {@code ConnectorContext#publishExternalEvent(ConnectorMetaChangeEvent)}
     * rather than being polled.
     */
    default boolean isSelfManaged() {
        return false;
    }

    /** Hook to gracefully shut down a self-managed source. */
    default void shutdownSelfManaged(Duration grace) {
    }
}
