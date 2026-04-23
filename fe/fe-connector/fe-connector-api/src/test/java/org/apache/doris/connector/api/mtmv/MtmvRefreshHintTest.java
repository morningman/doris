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

package org.apache.doris.connector.api.mtmv;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class MtmvRefreshHintTest {

    @Test
    public void factoryEmptyScope() {
        MtmvRefreshHint h = MtmvRefreshHint.of(MtmvRefreshHint.RefreshMode.FORCE_FULL);
        Assertions.assertEquals(MtmvRefreshHint.RefreshMode.FORCE_FULL, h.mode());
        Assertions.assertTrue(h.partitionScope().isEmpty());
    }

    @Test
    public void rejectsNullMode() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new MtmvRefreshHint(null, Optional.empty()));
    }

    @Test
    public void rejectsNullPartitionScope() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new MtmvRefreshHint(MtmvRefreshHint.RefreshMode.ON_DEMAND, null));
    }

    @Test
    public void carriesPartitionScope() {
        MtmvRefreshHint h = new MtmvRefreshHint(
                MtmvRefreshHint.RefreshMode.INCREMENTAL_AUTO, Optional.of("p20240101"));
        Assertions.assertEquals("p20240101", h.partitionScope().orElse(""));
    }
}
