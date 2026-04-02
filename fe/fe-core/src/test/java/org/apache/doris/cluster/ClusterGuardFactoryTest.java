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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

/**
 * Tests for {@link ClusterGuardFactory}.
 * <p>
 * Because ClusterGuardFactory uses a static singleton, the {@code instance} field
 * is reset via reflection before and after each test to ensure isolation.
 * </p>
 */
public class ClusterGuardFactoryTest {

    private Field instanceField;

    @Before
    public void setUp() throws Exception {
        instanceField = ClusterGuardFactory.class.getDeclaredField("instance");
        instanceField.setAccessible(true);
        instanceField.set(null, null); // reset singleton
    }

    @After
    public void tearDown() throws Exception {
        instanceField.set(null, null); // clean up after each test
    }

    @Test
    public void testGetGuardReturnsNonNull() {
        ClusterGuard guard = ClusterGuardFactory.getGuard();
        Assert.assertNotNull(guard);
    }

    @Test
    public void testGetGuardReturnsSingletonInstance() {
        ClusterGuard first = ClusterGuardFactory.getGuard();
        ClusterGuard second = ClusterGuardFactory.getGuard();
        Assert.assertSame(first, second);
    }

    @Test
    public void testGetGuardReturnsNoOpWhenNoSpiProviderFound() {
        // In the test classpath there is no META-INF/services/org.apache.doris.cluster.ClusterGuard,
        // so the factory must fall back to NoOpClusterGuard.
        ClusterGuard guard = ClusterGuardFactory.getGuard();
        Assert.assertSame(NoOpClusterGuard.INSTANCE, guard);
    }

    @Test
    public void testNoOpGuardAllowsUnlimitedNodes() throws ClusterGuardException {
        ClusterGuard guard = ClusterGuardFactory.getGuard();
        // Must not throw for any node count
        guard.checkNodeLimit(0);
        guard.checkNodeLimit(100);
        guard.checkNodeLimit(Integer.MAX_VALUE);
    }

    @Test
    public void testNoOpGuardTimeValidityAlwaysPasses() throws ClusterGuardException {
        ClusterGuard guard = ClusterGuardFactory.getGuard();
        guard.checkTimeValidity();
    }

    @Test
    public void testNoOpGuardInfoIsEmptyJson() {
        ClusterGuard guard = ClusterGuardFactory.getGuard();
        Assert.assertEquals("{}", guard.getGuardInfo());
    }

    /**
     * Verifies that a custom ClusterGuard injected directly into the factory is
     * returned by subsequent {@code getGuard()} calls. This simulates what an SPI
     * provider would do at startup.
     */
    @Test
    public void testCustomGuardIsReturnedWhenInjected() throws Exception {
        ClusterGuard custom = new StubClusterGuard("custom-info");
        instanceField.set(null, custom);

        ClusterGuard guard = ClusterGuardFactory.getGuard();
        Assert.assertSame(custom, guard);
        Assert.assertEquals("custom-info", guard.getGuardInfo());
    }

    @Test
    public void testCustomGuardCheckNodeLimitEnforcesLimit() throws Exception {
        int limit = 3;
        ClusterGuard limitedGuard = new StubClusterGuard("{}") {
            @Override
            public void checkNodeLimit(int currentNodeCount) throws ClusterGuardException {
                if (currentNodeCount > limit) {
                    throw new ClusterGuardException(
                            "Node limit exceeded: max=" + limit + ", current=" + currentNodeCount);
                }
            }
        };
        instanceField.set(null, limitedGuard);

        ClusterGuard guard = ClusterGuardFactory.getGuard();
        // Within limit — must not throw
        guard.checkNodeLimit(3);

        // Exceeds limit — must throw ClusterGuardException
        try {
            guard.checkNodeLimit(4);
            Assert.fail("Expected ClusterGuardException");
        } catch (ClusterGuardException e) {
            Assert.assertTrue(e.getMessage().contains("Node limit exceeded"));
        }
    }

    @Test
    public void testCustomGuardOnStartupPropagatesException() throws Exception {
        ClusterGuard failingGuard = new StubClusterGuard("{}") {
            @Override
            public void onStartup(String dorisHomeDir) throws ClusterGuardException {
                throw new ClusterGuardException("startup failed: bad license file");
            }
        };
        instanceField.set(null, failingGuard);

        try {
            ClusterGuardFactory.getGuard().onStartup("/doris/home");
            Assert.fail("Expected ClusterGuardException");
        } catch (ClusterGuardException e) {
            Assert.assertEquals("startup failed: bad license file", e.getMessage());
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Minimal stub implementation used for injection-based tests. */
    private static class StubClusterGuard implements ClusterGuard {

        private final String guardInfo;

        StubClusterGuard(String guardInfo) {
            this.guardInfo = guardInfo;
        }

        @Override
        public void onStartup(String dorisHomeDir) throws ClusterGuardException {
            // no-op by default
        }

        @Override
        public void checkTimeValidity() throws ClusterGuardException {
            // no-op by default
        }

        @Override
        public void checkNodeLimit(int currentNodeCount) throws ClusterGuardException {
            // unlimited by default
        }

        @Override
        public String getGuardInfo() {
            return guardInfo;
        }
    }
}
