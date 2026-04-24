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

package org.apache.doris.connector.paimon.api;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class PaimonBackendContextTest {

    @Test
    void roundTripsAccessors() {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.catalog.type", "hms");
        props.put("warehouse", "s3://b/w");

        PaimonBackendContext ctx = new PaimonBackendContext("paimon_cat", props);

        Assertions.assertEquals("paimon_cat", ctx.catalogName());
        Assertions.assertEquals("hms", ctx.properties().get("paimon.catalog.type"));
        Assertions.assertEquals("s3://b/w", ctx.properties().get("warehouse"));
    }

    @Test
    void propertiesMapIsImmutable() {
        Map<String, String> src = new HashMap<>();
        src.put("k", "v");
        PaimonBackendContext ctx = new PaimonBackendContext("n", src);

        // Mutating the source map after construction must not bleed in.
        src.put("k2", "v2");
        Assertions.assertNull(ctx.properties().get("k2"));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> ctx.properties().put("x", "y"));
    }

    @Test
    void rejectsNullArguments() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new PaimonBackendContext(null, new HashMap<>()));
        Assertions.assertThrows(NullPointerException.class,
                () -> new PaimonBackendContext("n", null));
    }
}
