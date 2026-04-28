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

package org.apache.doris.connector.api;

import org.apache.doris.connector.api.mtmv.MtmvOps;
import org.apache.doris.connector.api.policy.PolicyOps;
import org.apache.doris.connector.api.systable.SystemTableOps;
import org.apache.doris.connector.api.timetravel.RefOps;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Regression test guarding G-XCUT-1: no public method on the migrated SPIs
 * may carry both a {@code String database} parameter and a {@code String table}
 * parameter — they must be folded into a typed {@link ConnectorTableId}.
 */
public class ConnectorTableIdAdoptionTest {

    @Test
    public void noStringPairOnMigratedSpis() {
        List<Class<?>> migrated = Arrays.asList(
                RefOps.class, MtmvOps.class, PolicyOps.class, SystemTableOps.class);
        List<String> offenders = new ArrayList<>();
        for (Class<?> spi : migrated) {
            for (Method m : spi.getMethods()) {
                Class<?>[] params = m.getParameterTypes();
                String[] names = new String[params.length];
                for (int i = 0; i < params.length; i++) {
                    names[i] = params[i].getName();
                }
                boolean hasDb = false;
                boolean hasTbl = false;
                for (java.lang.reflect.Parameter p : m.getParameters()) {
                    if (!p.getType().equals(String.class)) {
                        continue;
                    }
                    String n = p.getName();
                    if ("database".equals(n) || "db".equals(n)) {
                        hasDb = true;
                    }
                    if ("table".equals(n) || "tbl".equals(n)) {
                        hasTbl = true;
                    }
                }
                int strCount = 0;
                for (Class<?> p : params) {
                    if (p.equals(String.class)) {
                        strCount++;
                    }
                }
                if ((hasDb && hasTbl) || (strCount >= 2 && !hasName(m, "ConnectorTableId"))) {
                    // Heuristic: any SPI method with at least two String params and no
                    // ConnectorTableId param is suspicious and likely the legacy pair.
                    if (strCount >= 2 && !hasIdParam(m)) {
                        offenders.add(spi.getSimpleName() + "#" + m.getName() + " " + Arrays.toString(names));
                    }
                }
            }
        }
        if (!offenders.isEmpty()) {
            throw new AssertionError("Migrated SPI still uses (String, String) pair: " + offenders);
        }
    }

    private static boolean hasName(Method m, String simple) {
        for (Class<?> p : m.getParameterTypes()) {
            if (p.getSimpleName().equals(simple)) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasIdParam(Method m) {
        for (Class<?> p : m.getParameterTypes()) {
            if (ConnectorTableId.class.equals(p)) {
                return true;
            }
        }
        return false;
    }
}
