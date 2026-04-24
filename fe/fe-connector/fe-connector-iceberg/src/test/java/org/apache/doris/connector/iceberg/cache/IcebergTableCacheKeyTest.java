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

package org.apache.doris.connector.iceberg.cache;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class IcebergTableCacheKeyTest {

    @Test
    void equalityAndHashCode() {
        IcebergTableCacheKey a = new IcebergTableCacheKey("db", "t");
        IcebergTableCacheKey b = new IcebergTableCacheKey("db", "t");
        IcebergTableCacheKey c = new IcebergTableCacheKey("db2", "t");
        IcebergTableCacheKey d = new IcebergTableCacheKey("db", "t2");
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        Assertions.assertNotEquals(a, c);
        Assertions.assertNotEquals(a, d);
        Assertions.assertNotEquals(a, "db.t");
    }

    @Test
    void rejectsNulls() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new IcebergTableCacheKey(null, "t"));
        Assertions.assertThrows(NullPointerException.class,
                () -> new IcebergTableCacheKey("db", null));
    }

    @Test
    void toStringRendersDbDotTable() {
        Assertions.assertEquals("db.t", new IcebergTableCacheKey("db", "t").toString());
    }
}
