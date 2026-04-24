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

package org.apache.doris.connector.paimon;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

class PaimonBackendRegistryTest {

    private static final Set<String> EXPECTED_BACKENDS = Set.of(
            "filesystem", "hms", "rest", "aliyun-dlf");

    @Test
    void allFourBackendsDiscoveredViaServiceLoader() {
        Set<String> available = PaimonBackendRegistry.availableTypes();
        for (String name : EXPECTED_BACKENDS) {
            Assertions.assertTrue(available.contains(name),
                    "missing paimon backend on classpath: " + name
                            + "; got: " + available);
            Assertions.assertTrue(PaimonBackendRegistry.get(name).isPresent());
        }
    }

    @Test
    void lookupIsCaseInsensitive() {
        Assertions.assertTrue(PaimonBackendRegistry.get("FILESYSTEM").isPresent());
        Assertions.assertTrue(PaimonBackendRegistry.get("Hms").isPresent());
        Assertions.assertTrue(PaimonBackendRegistry.get("Aliyun-DLF").isPresent());
    }

    @Test
    void unknownBackendReturnsEmpty() {
        Assertions.assertTrue(PaimonBackendRegistry.get("nope-xyz").isEmpty());
    }
}
