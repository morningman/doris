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

package org.apache.doris.connector.api.scan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

/**
 * G-XCUT-2 / M3-XCUT-1: locks the SPI to a single abstract entry point
 * — {@link ConnectorScanPlanProvider#planScan(ConnectorScanRequest)}.
 * Adding any new abstract {@code planScan} overload (or any new abstract
 * method on the SPI altogether) is a breaking change for every plugin
 * and must be a deliberate refactor; this test fails that change loud.
 */
class ConnectorScanRequestSweepTest {

    @Test
    void planScanIsTheOnlyAbstractMethodOnTheScanPlanProviderSpi() {
        List<Method> abstractMethods = new ArrayList<>();
        for (Method m : ConnectorScanPlanProvider.class.getDeclaredMethods()) {
            if (Modifier.isAbstract(m.getModifiers())) {
                abstractMethods.add(m);
            }
        }
        Assertions.assertEquals(1, abstractMethods.size(),
                "ConnectorScanPlanProvider must expose exactly one abstract method, got: "
                        + abstractMethods);
        Method only = abstractMethods.get(0);
        Assertions.assertEquals("planScan", only.getName(),
                "the sole abstract method must be planScan, got: " + only);
        Assertions.assertEquals(1, only.getParameterCount(),
                "planScan must take a single parameter (ConnectorScanRequest), got: " + only);
        Assertions.assertEquals(ConnectorScanRequest.class, only.getParameterTypes()[0],
                "planScan parameter type must be ConnectorScanRequest, got: "
                        + only.getParameterTypes()[0].getName());
    }
}
