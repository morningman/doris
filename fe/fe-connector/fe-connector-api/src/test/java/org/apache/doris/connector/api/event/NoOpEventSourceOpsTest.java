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
import java.util.Optional;

class NoOpEventSourceOpsTest {

    @Test
    void noneSingleton() {
        Assertions.assertNotNull(EventSourceOps.NONE);
        Assertions.assertSame(EventSourceOps.NONE, EventSourceOps.NONE);
    }

    @Test
    void initialCursorEmpty() {
        Assertions.assertTrue(EventSourceOps.NONE.initialCursor().isEmpty());
    }

    @Test
    void pollThrows() {
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> EventSourceOps.NONE.poll(null, 1, Duration.ZERO, EventFilter.ALL));
    }

    @Test
    void parseAndSerializeThrow() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> EventSourceOps.NONE.parseCursor(new byte[0]));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> EventSourceOps.NONE.serializeCursor(null));
    }

    @Test
    void isSelfManagedFalse() {
        Assertions.assertFalse(EventSourceOps.NONE.isSelfManaged());
    }

    @Test
    void shutdownNoOp() {
        EventSourceOps.NONE.shutdownSelfManaged(Duration.ofSeconds(1));
        // pull a cheap assertion to keep junit happy
        Assertions.assertSame(Optional.empty(), EventSourceOps.NONE.initialCursor().or(Optional::empty));
    }
}
