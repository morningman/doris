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

package org.apache.doris.connector.api.mtmv;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ConnectorPartitionItemTest {

    @Test
    public void rangeSerialized() {
        ConnectorPartitionItem item = new ConnectorPartitionItem.RangePartitionItem("MIN", "2024-01-01");
        Assertions.assertEquals("RANGE[MIN,2024-01-01)", item.serialized());
        Assertions.assertTrue(item instanceof ConnectorPartitionItem.RangePartitionItem);
    }

    @Test
    public void rangeRejectsNullBounds() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorPartitionItem.RangePartitionItem(null, "x"));
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorPartitionItem.RangePartitionItem("x", null));
    }

    @Test
    public void listSerializedAndImmutable() {
        List<List<String>> values = new ArrayList<>();
        values.add(new ArrayList<>(Arrays.asList("a", "1")));
        values.add(new ArrayList<>(Arrays.asList("b", "2")));
        ConnectorPartitionItem.ListPartitionItem item =
                new ConnectorPartitionItem.ListPartitionItem(values);

        Assertions.assertEquals("LIST[a,1;b,2]", item.serialized());
        // mutating the source list must not affect the stored value
        values.clear();
        Assertions.assertEquals(2, item.values().size());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> item.values().add(new ArrayList<>()));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> item.values().get(0).set(0, "z"));
    }

    @Test
    public void listRejectsNullTuple() {
        List<List<String>> values = new ArrayList<>();
        values.add(null);
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorPartitionItem.ListPartitionItem(values));
    }

    @Test
    public void unpartitioned() {
        ConnectorPartitionItem item = new ConnectorPartitionItem.UnpartitionedItem();
        Assertions.assertEquals("UNPARTITIONED", item.serialized());
        Assertions.assertEquals(new ConnectorPartitionItem.UnpartitionedItem(), item);
    }

    @Test
    public void sealedHierarchyMatrix() {
        ConnectorPartitionItem[] items = new ConnectorPartitionItem[] {
                new ConnectorPartitionItem.RangePartitionItem("a", "b"),
                new ConnectorPartitionItem.ListPartitionItem(new ArrayList<>()),
                new ConnectorPartitionItem.UnpartitionedItem(),
        };
        int matched = 0;
        for (ConnectorPartitionItem it : items) {
            if (it instanceof ConnectorPartitionItem.RangePartitionItem) {
                matched++;
            } else if (it instanceof ConnectorPartitionItem.ListPartitionItem) {
                matched++;
            } else if (it instanceof ConnectorPartitionItem.UnpartitionedItem) {
                matched++;
            }
        }
        Assertions.assertEquals(3, matched);
    }
}
