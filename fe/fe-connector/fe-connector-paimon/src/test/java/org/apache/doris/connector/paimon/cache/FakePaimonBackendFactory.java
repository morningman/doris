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

package org.apache.doris.connector.paimon.cache;

import org.apache.doris.connector.paimon.api.PaimonBackend;
import org.apache.doris.connector.paimon.api.PaimonBackendContext;
import org.apache.doris.connector.paimon.api.PaimonBackendFactory;

import org.apache.paimon.catalog.Catalog;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test-scope paimon backend that returns a {@link Catalog} produced by a
 * static {@link java.util.function.Supplier} so each test can plug in an
 * in-memory or mocked SDK catalog. Discovered via the test-resources
 * {@code META-INF/services} entry; the paimon backend type id is
 * {@code "fake"} to avoid colliding with the production filesystem / hms /
 * rest / aliyun-dlf registrations.
 */
public final class FakePaimonBackendFactory implements PaimonBackendFactory {

    public static final String TYPE = "fake";
    private static final AtomicInteger BUILD_INVOCATIONS = new AtomicInteger();
    private static volatile java.util.function.Supplier<Catalog> CATALOG_SUPPLIER = () -> {
        throw new IllegalStateException("FakePaimonBackendFactory not configured");
    };

    public static void configureCatalogSupplier(java.util.function.Supplier<Catalog> supplier) {
        CATALOG_SUPPLIER = supplier;
        BUILD_INVOCATIONS.set(0);
    }

    public static int buildInvocationCount() {
        return BUILD_INVOCATIONS.get();
    }

    @Override
    public String name() {
        return TYPE;
    }

    @Override
    public PaimonBackend create() {
        return new FakeBackend();
    }

    private static final class FakeBackend implements PaimonBackend {
        @Override
        public String name() {
            return TYPE;
        }

        @Override
        public Catalog buildCatalog(PaimonBackendContext context) {
            BUILD_INVOCATIONS.incrementAndGet();
            return CATALOG_SUPPLIER.get();
        }
    }
}
