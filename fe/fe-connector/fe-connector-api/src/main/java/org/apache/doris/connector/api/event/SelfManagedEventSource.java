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

/**
 * D7 mixin marking an {@link EventSourceOps} implementation that owns its
 * own event-loop logic but does <b>not</b> own a thread / executor.
 *
 * <p>Implementations declare {@link EventSourceOps#isSelfManaged()} = {@code true}
 * (so the engine never polls them through {@link EventSourceOps#poll}) and
 * expose the periodic work as a plain {@link Runnable}. The fe-core engine
 * is responsible for scheduling that Runnable on its
 * {@code MasterOnlyScheduledExecutor} so HA gating, shutdown ordering and
 * metrics are uniform across plugins.</p>
 *
 * <p>Probing is done via {@code instanceof}: only sources whose
 * {@code isSelfManaged()} returns {@code true} <i>and</i> implement this
 * mixin will be wired by the engine.</p>
 */
public interface SelfManagedEventSource {

    /**
     * Returns the periodic {@link Runnable} the engine will invoke on its
     * master-only scheduler. Must never return {@code null}; the engine
     * treats every invocation as best-effort and isolates exceptions, so
     * implementations should not propagate fatal errors out of the run
     * method.
     */
    Runnable getSelfManagedTask();
}
