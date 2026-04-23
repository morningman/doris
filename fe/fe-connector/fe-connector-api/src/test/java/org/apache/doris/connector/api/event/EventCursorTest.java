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

class EventCursorTest {

    @Test
    void compareToConsistentWithDescribe() {
        EventCursor a = new IntCursor(1);
        EventCursor b = new IntCursor(2);
        EventCursor c = new IntCursor(2);
        Assertions.assertTrue(a.compareTo(b) < 0);
        Assertions.assertTrue(b.compareTo(a) > 0);
        Assertions.assertEquals(0, b.compareTo(c));
        Assertions.assertEquals("c1", a.describe());
        Assertions.assertEquals("c2", b.describe());
    }

    private static final class IntCursor implements EventCursor {
        private final int order;

        IntCursor(int order) {
            this.order = order;
        }

        @Override
        public String describe() {
            return "c" + order;
        }

        @Override
        public int compareTo(EventCursor o) {
            return Integer.compare(order, ((IntCursor) o).order);
        }
    }
}
