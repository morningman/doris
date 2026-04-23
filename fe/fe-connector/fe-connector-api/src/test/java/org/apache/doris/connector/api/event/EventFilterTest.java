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

import java.util.HashSet;
import java.util.Set;

class EventFilterTest {

    @Test
    void allSingletonCatchAllAndEmpty() {
        Assertions.assertTrue(EventFilter.ALL.catchAll());
        Assertions.assertTrue(EventFilter.ALL.databases().isEmpty());
        Assertions.assertTrue(EventFilter.ALL.tables().isEmpty());
    }

    @Test
    void builderHappyPath() {
        EventFilter f = EventFilter.builder()
                .database("db1")
                .table(new TableIdentifier("db1", "t1"))
                .catchAll(false)
                .build();
        Assertions.assertEquals(Set.of("db1"), f.databases());
        Assertions.assertEquals(Set.of(new TableIdentifier("db1", "t1")), f.tables());
        Assertions.assertEquals(false, f.catchAll());
    }

    @Test
    void inputMutationDoesNotLeak() {
        Set<String> dbs = new HashSet<>();
        dbs.add("db1");
        Set<TableIdentifier> tbls = new HashSet<>();
        tbls.add(new TableIdentifier("db1", "t1"));
        EventFilter f = new EventFilter(dbs, tbls, false);

        dbs.add("db2");
        tbls.add(new TableIdentifier("db1", "t2"));

        Assertions.assertEquals(1, f.databases().size());
        Assertions.assertEquals(1, f.tables().size());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> f.databases().add("x"));
    }

    @Test
    void equalsAndHashCode() {
        EventFilter a = EventFilter.builder().database("d").build();
        EventFilter b = EventFilter.builder().database("d").build();
        EventFilter c = EventFilter.builder().database("e").build();
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        Assertions.assertNotEquals(a, c);
        Assertions.assertTrue(a.toString().contains("databases"));
    }
}
