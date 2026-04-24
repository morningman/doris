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

package org.apache.doris.connector.iceberg.api;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class IcebergBackendContextTest {

    @Test
    void roundTripsAccessors() {
        Map<String, String> props = new HashMap<>();
        props.put("iceberg.catalog.type", "hms");
        props.put("warehouse", "s3://b/w");
        Configuration conf = new Configuration(false);
        conf.set("fs.defaultFS", "s3://b");

        IcebergBackendContext ctx = new IcebergBackendContext("ice_cat", props, conf);

        Assertions.assertEquals("ice_cat", ctx.catalogName());
        Assertions.assertEquals("hms", ctx.properties().get("iceberg.catalog.type"));
        Assertions.assertEquals("s3://b/w", ctx.properties().get("warehouse"));
        Assertions.assertSame(conf, ctx.hadoopConf());
    }

    @Test
    void propertiesMapIsImmutable() {
        Map<String, String> src = new HashMap<>();
        src.put("k", "v");
        IcebergBackendContext ctx = new IcebergBackendContext(
                "n", src, new Configuration(false));

        // Mutating the source map after construction must not bleed in.
        src.put("k2", "v2");
        Assertions.assertNull(ctx.properties().get("k2"));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> ctx.properties().put("x", "y"));
    }

    @Test
    void rejectsNullArguments() {
        Configuration conf = new Configuration(false);
        Assertions.assertThrows(NullPointerException.class,
                () -> new IcebergBackendContext(null, new HashMap<>(), conf));
        Assertions.assertThrows(NullPointerException.class,
                () -> new IcebergBackendContext("n", null, conf));
        Assertions.assertThrows(NullPointerException.class,
                () -> new IcebergBackendContext("n", new HashMap<>(), null));
    }
}
