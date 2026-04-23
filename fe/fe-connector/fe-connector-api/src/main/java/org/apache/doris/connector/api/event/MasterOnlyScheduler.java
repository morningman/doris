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

package org.apache.doris.connector.api.event;

import java.time.Duration;
import java.util.concurrent.ScheduledFuture;

/**
 * Abstract scheduler exposed to self-managed event sources. The engine
 * binds this to its master-only scheduled executor (so tasks only run on
 * the elected leader FE); the SPI deliberately does not depend on the
 * fe-core internal type.
 */
public interface MasterOnlyScheduler {

    /**
     * Schedule a fixed-rate task. Equivalent to
     * {@link java.util.concurrent.ScheduledExecutorService}{@code #scheduleAtFixedRate}
     * but constrained to run only on the master FE.
     */
    ScheduledFuture<?> scheduleAtFixedRate(Runnable task, Duration initialDelay, Duration period, String taskName);

    /** Shut down the scheduler, allowing in-flight tasks up to {@code grace} to complete. */
    void shutdown(Duration grace);
}
