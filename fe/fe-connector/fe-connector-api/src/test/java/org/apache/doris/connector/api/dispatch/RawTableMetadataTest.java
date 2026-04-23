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

package org.apache.doris.connector.api.dispatch;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class RawTableMetadataTest {

    @Test
    void factoryReturnsImmutableViewAndDoesNotLeak() {
        Map<String, String> tp = new HashMap<>();
        tp.put("table_type", "ICEBERG");
        Map<String, String> sp = new HashMap<>();
        sp.put("serialization.format", "1");

        RawTableMetadata raw = RawTableMetadata.of(tp, "ParquetInputFormat", "s3://b/x", sp);

        Assertions.assertEquals("ICEBERG", raw.tableParameters().get("table_type"));
        Assertions.assertEquals("ParquetInputFormat", raw.inputFormat());
        Assertions.assertEquals("s3://b/x", raw.storageLocation());
        Assertions.assertEquals("1", raw.serdeParameters().get("serialization.format"));

        // mutating the input maps must not leak through
        tp.put("table_type", "HUDI");
        sp.put("serialization.format", "9");
        Assertions.assertEquals("ICEBERG", raw.tableParameters().get("table_type"));
        Assertions.assertEquals("1", raw.serdeParameters().get("serialization.format"));

        // returned maps are immutable
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> raw.tableParameters().put("k", "v"));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> raw.serdeParameters().put("k", "v"));
    }

    @Test
    void rejectsNullArgs() {
        Map<String, String> empty = new HashMap<>();
        Assertions.assertThrows(NullPointerException.class,
                () -> RawTableMetadata.of(null, "fmt", "loc", empty));
        Assertions.assertThrows(NullPointerException.class,
                () -> RawTableMetadata.of(empty, null, "loc", empty));
        Assertions.assertThrows(NullPointerException.class,
                () -> RawTableMetadata.of(empty, "fmt", null, empty));
        Assertions.assertThrows(NullPointerException.class,
                () -> RawTableMetadata.of(empty, "fmt", "loc", null));
    }
}
