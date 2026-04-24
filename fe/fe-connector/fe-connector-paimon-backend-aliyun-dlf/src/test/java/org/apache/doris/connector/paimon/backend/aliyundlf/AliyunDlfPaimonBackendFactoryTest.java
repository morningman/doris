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

package org.apache.doris.connector.paimon.backend.aliyundlf;

import org.apache.doris.connector.paimon.api.PaimonBackend;
import org.apache.doris.connector.paimon.api.PaimonBackendContext;
import org.apache.doris.connector.paimon.api.PaimonBackendException;
import org.apache.doris.connector.paimon.api.PaimonBackendFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ServiceLoader;

class AliyunDlfPaimonBackendFactoryTest {

    @Test
    void factoryDiscoveredViaServiceLoader() {
        List<PaimonBackendFactory> found = new ArrayList<>();
        for (PaimonBackendFactory f : ServiceLoader.load(PaimonBackendFactory.class)) {
            found.add(f);
        }
        boolean present = found.stream()
                .anyMatch(f -> f instanceof AliyunDlfPaimonBackendFactory);
        Assertions.assertTrue(present,
                "ServiceLoader did not find AliyunDlfPaimonBackendFactory; got: " + found);
    }

    @Test
    void factoryNameMatchesBackendName() {
        AliyunDlfPaimonBackendFactory f = new AliyunDlfPaimonBackendFactory();
        Assertions.assertEquals("aliyun-dlf", f.name());
        Assertions.assertEquals(f.name(), f.create().name());
    }

    /**
     * DLF backend remains a stub (M3). When the orchestrator's
     * {@code paimon.catalog} cache binding loader invokes
     * {@code buildCatalog}, the resulting failure surface MUST be
     * {@link PaimonBackendException} so the orchestrator can propagate a
     * meaningful error to the user. Pinning the throw path here guards
     * against accidental silent stubs (e.g. returning {@code null}) when
     * the backend gets reshaped by the M1-11 binding migration.
     */
    @Test
    void buildCatalogThrowsPaimonBackendException() {
        PaimonBackend backend = new AliyunDlfPaimonBackendFactory().create();
        PaimonBackendContext ctx = new PaimonBackendContext("c", new HashMap<>());
        PaimonBackendException e = Assertions.assertThrows(
                PaimonBackendException.class, () -> backend.buildCatalog(ctx));
        Assertions.assertTrue(e.getMessage().contains("aliyun-dlf"),
                "stub message should mention the backend name; got: " + e.getMessage());
    }
}
