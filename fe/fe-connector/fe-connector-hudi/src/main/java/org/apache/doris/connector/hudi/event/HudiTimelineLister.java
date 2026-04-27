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

package org.apache.doris.connector.hudi.event;

import java.util.List;

/**
 * Lists timeline {@link HudiInstant}s for one Hudi table by scanning
 * its {@code .hoodie/} directory. Production wires this to a Hadoop
 * {@code FileSystem}-backed implementation; tests inject canned data.
 *
 * <p>An implementation must:
 * <ul>
 *   <li>return {@link java.util.Collections#emptyList()} when the
 *       {@code .hoodie/} directory does not exist (table not yet
 *       initialised);</li>
 *   <li>throw {@link RuntimeException} for catastrophic IO failures
 *       (the watcher catches and skips the table for the current tick).
 *   </li>
 * </ul>
 */
@FunctionalInterface
public interface HudiTimelineLister {
    List<HudiInstant> listInstants(HudiTableRef ref);
}
