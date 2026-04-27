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

package org.apache.doris.connector.event;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MasterOnlyScheduledExecutorTest {

    private MasterOnlyScheduledExecutor exec;

    @AfterEach
    public void tearDown() {
        if (exec != null && !exec.isShutdown()) {
            exec.shutdown();
        }
    }

    @Test
    public void skipsTickWhenNonMaster() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        exec = new MasterOnlyScheduledExecutor(1, "test-skip", () -> false);
        exec.scheduleAtFixedRate("t", runs::incrementAndGet, 5L, 20L);
        Thread.sleep(150L);
        Assertions.assertEquals(0, runs.get());
        Assertions.assertTrue(exec.getSkippedTicks("t") >= 3);
        Assertions.assertEquals(0, exec.getExecutedTicks("t"));
    }

    @Test
    public void runsWhenMaster() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        exec = new MasterOnlyScheduledExecutor(1, "test-run", () -> true);
        exec.scheduleAtFixedRate("t", runs::incrementAndGet, 5L, 20L);
        Thread.sleep(150L);
        Assertions.assertTrue(runs.get() >= 3, "expected several executions, got " + runs.get());
        Assertions.assertEquals(0, exec.getSkippedTicks("t"));
        Assertions.assertTrue(exec.getExecutedTicks("t") >= 3);
    }

    @Test
    public void switchingMasterFlagMidRunIsRespected() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        final boolean[] master = {false};
        exec = new MasterOnlyScheduledExecutor(1, "test-flip", () -> master[0]);
        exec.scheduleAtFixedRate("t", runs::incrementAndGet, 5L, 20L);
        Thread.sleep(80L);
        int beforeFlip = runs.get();
        master[0] = true;
        Thread.sleep(120L);
        master[0] = false;
        int afterFlipUp = runs.get();
        Thread.sleep(80L);
        int afterFlipDown = runs.get();
        Assertions.assertEquals(0, beforeFlip);
        Assertions.assertTrue(afterFlipUp > beforeFlip, "should run while master=true");
        Assertions.assertEquals(afterFlipDown, runs.get(),
                "should not run more after master flips back to false");
    }

    @Test
    public void shutdownStopsFurtherExecution() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        exec = new MasterOnlyScheduledExecutor(1, "test-shutdown", () -> true);
        exec.scheduleAtFixedRate("t", runs::incrementAndGet, 5L, 15L);
        Thread.sleep(80L);
        exec.shutdown();
        int after = runs.get();
        Thread.sleep(80L);
        Assertions.assertEquals(after, runs.get());
        Assertions.assertTrue(exec.isShutdown());
    }

    @Test
    public void taskExceptionDoesNotKillSchedule() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        exec = new MasterOnlyScheduledExecutor(1, "test-throw", () -> true);
        exec.scheduleAtFixedRate("t", () -> {
            int i = runs.incrementAndGet();
            if (i == 2) {
                throw new RuntimeException("boom");
            }
        }, 5L, 15L);
        Thread.sleep(150L);
        Assertions.assertTrue(runs.get() >= 4, "schedule must keep ticking past a thrown exception");
    }

    @Test
    public void rejectsNonPositivePeriod() {
        exec = new MasterOnlyScheduledExecutor(1, "test-reject", () -> true);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> exec.scheduleAtFixedRate("t", () -> {}, 1L, 0L));
    }

    @Test
    public void executeOnceRespectsMasterFlag() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        exec = new MasterOnlyScheduledExecutor(1, "test-once", () -> false);
        exec.executeOnce("once", runs::incrementAndGet);
        Thread.sleep(50L);
        Assertions.assertEquals(0, runs.get());
        Assertions.assertEquals(1L, exec.getSkippedTicks("once"));
    }

    @Test
    public void rejectsScheduleAfterShutdown() {
        exec = new MasterOnlyScheduledExecutor(1, "test-after", () -> true);
        exec.shutdown();
        Assertions.assertThrows(IllegalStateException.class,
                () -> exec.scheduleAtFixedRate("t", () -> {}, 1L, 1L));
        Assertions.assertThrows(IllegalStateException.class,
                () -> exec.executeOnce("t", () -> {}));
    }

    @Test
    public void predicateExceptionTreatedAsNonMaster() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        exec = new MasterOnlyScheduledExecutor(1, "test-throw-pred", () -> {
            throw new RuntimeException("predicate failure");
        });
        exec.scheduleAtFixedRate("t", runs::incrementAndGet, 5L, 20L);
        Thread.sleep(80L);
        Assertions.assertEquals(0, runs.get());
        Assertions.assertTrue(exec.getSkippedTicks("t") >= 1);
    }

    @Test
    public void shutdownIsIdempotent() {
        exec = new MasterOnlyScheduledExecutor(1, "test-idem", () -> true);
        exec.shutdown();
        exec.shutdown();
        Assertions.assertTrue(exec.isShutdown());
    }

    @Test
    public void unknownTaskNameReturnsZeroCounters() {
        exec = new MasterOnlyScheduledExecutor(1, "test-unk", () -> true);
        Assertions.assertEquals(0L, exec.getSkippedTicks("nope"));
        Assertions.assertEquals(0L, exec.getExecutedTicks("nope"));
    }

    @Test
    public void rejectsBadConstructorArgs() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new MasterOnlyScheduledExecutor(0, "x", () -> true));
        Assertions.assertThrows(NullPointerException.class,
                () -> new MasterOnlyScheduledExecutor(1, null, () -> true));
        Assertions.assertThrows(NullPointerException.class,
                () -> new MasterOnlyScheduledExecutor(1, "x", null));
    }

    @Test
    public void awaitsBoundedShutdown() throws Exception {
        // Ensure shutdown completes quickly even when a task is currently running
        exec = new MasterOnlyScheduledExecutor(1, "test-bounded", () -> true);
        exec.scheduleAtFixedRate("t", () -> {
            try {
                Thread.sleep(50L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, 1L, 20L);
        long start = System.nanoTime();
        Thread.sleep(20L);
        exec.shutdown();
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        Assertions.assertTrue(elapsedMs < 6000L, "shutdown should complete within 6s, got " + elapsedMs);
    }
}
