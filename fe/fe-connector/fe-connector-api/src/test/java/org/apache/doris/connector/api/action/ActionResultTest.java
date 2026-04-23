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

package org.apache.doris.connector.api.action;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ActionResultTest {

    @Test
    public void completedWithRows() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("files_rewritten", 3L);
        List<Map<String, Object>> rows = new ArrayList<>();
        rows.add(row);

        ActionResult r = ActionResult.completed(rows);
        Assertions.assertEquals(ActionResult.Lifecycle.COMPLETED, r.lifecycle());
        Assertions.assertEquals(1, r.rows().size());
        Assertions.assertEquals(3L, r.rows().get(0).get("files_rewritten"));
        Assertions.assertTrue(r.handleId().isEmpty());
        Assertions.assertTrue(r.errorMessage().isEmpty());
    }

    @Test
    public void completedEmpty() {
        ActionResult r = ActionResult.completedEmpty();
        Assertions.assertEquals(ActionResult.Lifecycle.COMPLETED, r.lifecycle());
        Assertions.assertTrue(r.rows().isEmpty());
    }

    @Test
    public void startedAsync() {
        ActionResult r = ActionResult.startedAsync("h-42");
        Assertions.assertEquals(ActionResult.Lifecycle.STARTED_ASYNC, r.lifecycle());
        Assertions.assertTrue(r.handleId().isPresent());
        Assertions.assertEquals("h-42", r.handleId().get());
        Assertions.assertTrue(r.errorMessage().isEmpty());
    }

    @Test
    public void startedAsyncRejectsBlankHandle() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ActionResult.startedAsync(" "));
        Assertions.assertThrows(NullPointerException.class,
                () -> ActionResult.startedAsync(null));
    }

    @Test
    public void failedCarriesMessage() {
        ActionResult r = ActionResult.failed("boom");
        Assertions.assertEquals(ActionResult.Lifecycle.FAILED, r.lifecycle());
        Assertions.assertTrue(r.errorMessage().isPresent());
        Assertions.assertEquals("boom", r.errorMessage().get());
    }

    @Test
    public void failedRejectsNull() {
        Assertions.assertThrows(NullPointerException.class,
                () -> ActionResult.failed(null));
    }

    @Test
    public void rowsDefensivelyCopied() {
        Map<String, Object> row = new HashMap<>();
        row.put("a", 1);
        List<Map<String, Object>> rows = new ArrayList<>();
        rows.add(row);

        ActionResult r = ActionResult.completed(rows);
        // mutating the source list must not leak
        rows.add(new HashMap<>());
        // mutating the source row must not leak
        row.put("b", 2);

        Assertions.assertEquals(1, r.rows().size());
        Assertions.assertEquals(1, r.rows().get(0).size());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> r.rows().add(new HashMap<>()));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> r.rows().get(0).put("c", 3));
    }

    @Test
    public void lifecycleHasThreeValues() {
        Assertions.assertEquals(3, ActionResult.Lifecycle.values().length);
    }
}
