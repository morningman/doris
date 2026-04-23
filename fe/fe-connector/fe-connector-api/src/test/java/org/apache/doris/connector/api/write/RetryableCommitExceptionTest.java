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

package org.apache.doris.connector.api.write;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;

public class RetryableCommitExceptionTest {

    @Test
    public void messageCtorDefaultsBackoffZero() {
        RetryableCommitException ex = new RetryableCommitException("boom");
        Assertions.assertEquals("boom", ex.getMessage());
        Assertions.assertNull(ex.getCause());
        Assertions.assertEquals(Duration.ZERO, ex.suggestedBackoff());
    }

    @Test
    public void causeCtorPropagates() {
        Throwable cause = new RuntimeException("inner");
        RetryableCommitException ex = new RetryableCommitException("boom", cause);
        Assertions.assertEquals("boom", ex.getMessage());
        Assertions.assertSame(cause, ex.getCause());
        Assertions.assertEquals(Duration.ZERO, ex.suggestedBackoff());
    }

    @Test
    public void explicitBackoffRoundTrip() {
        Duration d = Duration.ofMillis(500);
        RetryableCommitException ex = new RetryableCommitException("retry", null, d);
        Assertions.assertEquals(d, ex.suggestedBackoff());
    }

    @Test
    public void nullBackoffRejected() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new RetryableCommitException("x", null, null));
    }
}
