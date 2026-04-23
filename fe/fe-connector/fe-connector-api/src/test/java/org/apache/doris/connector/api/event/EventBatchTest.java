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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

class EventBatchTest {

    private static final EventCursor CURSOR = new EventCursor() {
        @Override
        public String describe() {
            return "cur";
        }

        @Override
        public int compareTo(EventCursor o) {
            return 0;
        }
    };

    @Test
    void emptyFactory() {
        EventBatch b = EventBatch.empty(CURSOR);
        Assertions.assertTrue(b.events().isEmpty());
        Assertions.assertEquals(CURSOR, b.nextCursor());
        Assertions.assertFalse(b.hasMore());
        Assertions.assertEquals(Optional.empty(), b.recommendedNextPollDelay());
    }

    @Test
    void orderingEnforced() {
        ConnectorMetaChangeEvent e1 = new ConnectorMetaChangeEvent.DatabaseCreated(2, Instant.now(), "c", "d", "x");
        ConnectorMetaChangeEvent e2 = new ConnectorMetaChangeEvent.DatabaseCreated(1, Instant.now(), "c", "d", "x");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new EventBatch(List.of(e1, e2), CURSOR, false, Optional.empty()));
    }

    @Test
    void recommendedDelayOptional() {
        EventBatch b = EventBatch.builder()
                .nextCursor(CURSOR)
                .hasMore(true)
                .recommendedNextPollDelay(Duration.ofSeconds(5))
                .build();
        Assertions.assertEquals(Optional.of(Duration.ofSeconds(5)), b.recommendedNextPollDelay());
        Assertions.assertTrue(b.hasMore());
    }

    @Test
    void builderAscendingAccepted() {
        ConnectorMetaChangeEvent e1 = new ConnectorMetaChangeEvent.DatabaseCreated(1, Instant.now(), "c", "d", "x");
        ConnectorMetaChangeEvent e2 = new ConnectorMetaChangeEvent.DatabaseCreated(2, Instant.now(), "c", "d", "x");
        EventBatch b = EventBatch.builder().add(e1).add(e2).nextCursor(CURSOR).build();
        Assertions.assertEquals(2, b.events().size());
    }
}
