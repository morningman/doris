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

package org.apache.doris.connector.es;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

/**
 * Confirms that {@link EsConnectorProvider} keeps the SPI-default
 * {@link Optional#empty()} for {@code defaultAccessControllerFactoryName}, so
 * es catalogs continue to use the engine's built-in access controller unless
 * the user explicitly sets {@code access_controller.class} (D8 §11.1 / M2-10).
 */
public class EsConnectorProviderAccessControllerDefaultTest {

    @Test
    public void defaultsToEmpty() {
        Assertions.assertEquals(Optional.empty(),
                new EsConnectorProvider().defaultAccessControllerFactoryName());
    }
}
