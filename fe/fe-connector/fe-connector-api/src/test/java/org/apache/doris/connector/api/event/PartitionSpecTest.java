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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

class PartitionSpecTest {

    @Test
    void preservesInsertionOrder() {
        LinkedHashMap<String, String> in = new LinkedHashMap<>();
        in.put("year", "2024");
        in.put("month", "01");
        in.put("day", "15");
        PartitionSpec s = new PartitionSpec(in);

        List<String> keys = new ArrayList<>(s.values().keySet());
        Assertions.assertEquals(List.of("year", "month", "day"), keys);
    }

    @Test
    void immutability() {
        LinkedHashMap<String, String> in = new LinkedHashMap<>();
        in.put("a", "1");
        PartitionSpec s = new PartitionSpec(in);

        in.put("b", "2");
        Assertions.assertEquals(1, s.values().size());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> s.values().put("c", "3"));
    }
}
