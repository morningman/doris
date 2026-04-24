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

package org.apache.doris.connector.paimon.backend.rest;

import org.apache.doris.connector.paimon.api.PaimonBackendFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

class RestPaimonBackendFactoryTest {

    @Test
    void factoryDiscoveredViaServiceLoader() {
        List<PaimonBackendFactory> found = new ArrayList<>();
        for (PaimonBackendFactory f : ServiceLoader.load(PaimonBackendFactory.class)) {
            found.add(f);
        }
        boolean present = found.stream()
                .anyMatch(f -> f instanceof RestPaimonBackendFactory);
        Assertions.assertTrue(present,
                "ServiceLoader did not find RestPaimonBackendFactory; got: " + found);
    }

    @Test
    void factoryNameMatchesBackendName() {
        RestPaimonBackendFactory f = new RestPaimonBackendFactory();
        Assertions.assertEquals("rest", f.name());
        Assertions.assertEquals(f.name(), f.create().name());
    }
}
