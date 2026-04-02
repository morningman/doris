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

package org.apache.doris.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * Factory that discovers a {@link ClusterGuard} implementation via
 * {@link ServiceLoader}. If no provider is found on the classpath,
 * a {@link NoOpClusterGuard} is returned so the open-source edition
 * runs without restrictions.
 */
public class ClusterGuardFactory {
    private static final Logger LOG = LogManager.getLogger(ClusterGuardFactory.class);

    private static volatile ClusterGuard instance;

    /**
     * Get the singleton ClusterGuard instance.
     * On first call, discovers the implementation via ServiceLoader.
     */
    public static ClusterGuard getGuard() {
        if (instance == null) {
            synchronized (ClusterGuardFactory.class) {
                if (instance == null) {
                    instance = loadGuard();
                }
            }
        }
        return instance;
    }

    private static ClusterGuard loadGuard() {
        ServiceLoader<ClusterGuard> loader = ServiceLoader.load(ClusterGuard.class);
        Iterator<ClusterGuard> it = loader.iterator();
        if (it.hasNext()) {
            ClusterGuard guard = it.next();
            LOG.info("Loaded ClusterGuard implementation: {}", guard.getClass().getName());
            return guard;
        }
        LOG.info("No ClusterGuard implementation found, using NoOpClusterGuard");
        return NoOpClusterGuard.INSTANCE;
    }
}
