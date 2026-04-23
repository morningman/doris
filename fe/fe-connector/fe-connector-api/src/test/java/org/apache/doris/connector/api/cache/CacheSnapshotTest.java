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

package org.apache.doris.connector.api.cache;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CacheSnapshotTest {

    @Test
    public void roundTrip() {
        MetaCacheHandle.CacheStats stats = new MetaCacheHandle.CacheStats(1, 2, 3, 4, 5, 6, 7);
        CacheSnapshot snap = new CacheSnapshot("entry", stats);
        Assertions.assertEquals("entry", snap.getBindingName());
        Assertions.assertSame(stats, snap.getStats());
        Assertions.assertEquals(snap, new CacheSnapshot("entry", stats));
        Assertions.assertEquals(snap.hashCode(), new CacheSnapshot("entry", stats).hashCode());
        Assertions.assertTrue(snap.toString().contains("entry"));
    }

    @Test
    public void rejectsNullArgs() {
        MetaCacheHandle.CacheStats empty = MetaCacheHandle.CacheStats.empty();
        Assertions.assertThrows(IllegalArgumentException.class, () -> new CacheSnapshot(null, empty));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new CacheSnapshot("", empty));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new CacheSnapshot("e", null));
    }

    @Test
    public void emptyStatsAllZero() {
        MetaCacheHandle.CacheStats empty = MetaCacheHandle.CacheStats.empty();
        Assertions.assertEquals(0L, empty.getHitCount());
        Assertions.assertEquals(0L, empty.getCurrentSize());
        Assertions.assertEquals(empty, MetaCacheHandle.CacheStats.empty());
    }
}
