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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * Scheduled executor that gates each task tick on a master-only predicate.
 *
 * <p>Each scheduled task is wrapped so that, before invocation, the supplied
 * {@link BooleanSupplier} is consulted; if it returns {@code false}, the tick
 * is skipped (the task itself stays scheduled). The skip path increments a
 * counter visible via {@link #getSkippedTicks(String)} for observability.</p>
 *
 * <p>Tests inject the predicate directly (e.g. a {@code volatile boolean
 * masterFlag::get}); production code injects
 * {@code () -> Env.getCurrentEnv().isMaster()}.</p>
 */
public final class MasterOnlyScheduledExecutor {

    private static final Logger LOG = LogManager.getLogger(MasterOnlyScheduledExecutor.class);

    private final ScheduledExecutorService delegate;
    private final BooleanSupplier isMaster;
    private final ConcurrentHashMap<String, ScheduledFuture<?>> tasks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> skipCounters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> runCounters = new ConcurrentHashMap<>();
    private volatile boolean shutdown;

    public MasterOnlyScheduledExecutor(int poolSize, String threadNamePrefix, BooleanSupplier isMaster) {
        Objects.requireNonNull(threadNamePrefix, "threadNamePrefix");
        Objects.requireNonNull(isMaster, "isMaster");
        if (poolSize <= 0) {
            throw new IllegalArgumentException("poolSize must be positive");
        }
        this.delegate = Executors.newScheduledThreadPool(poolSize, namedThreadFactory(threadNamePrefix));
        this.isMaster = isMaster;
    }

    private static ThreadFactory namedThreadFactory(String prefix) {
        AtomicInteger seq = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + "-" + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    /**
     * Schedule {@code task} at a fixed rate; each scheduled invocation is
     * gated on the master predicate. Names must be unique within a single
     * executor instance.
     */
    public ScheduledFuture<?> scheduleAtFixedRate(String taskName,
                                                  Runnable task,
                                                  long initialDelayMs,
                                                  long periodMs) {
        Objects.requireNonNull(taskName, "taskName");
        Objects.requireNonNull(task, "task");
        if (periodMs <= 0) {
            throw new IllegalArgumentException("periodMs must be positive");
        }
        if (shutdown) {
            throw new IllegalStateException("executor is shutdown");
        }
        skipCounters.computeIfAbsent(taskName, k -> new AtomicLong());
        runCounters.computeIfAbsent(taskName, k -> new AtomicLong());
        Runnable gated = () -> {
            if (shutdown) {
                return;
            }
            if (!safeIsMaster()) {
                skipCounters.get(taskName).incrementAndGet();
                return;
            }
            try {
                task.run();
                runCounters.get(taskName).incrementAndGet();
            } catch (Throwable t) {
                LOG.warn("master-only task {} threw, will retry next tick", taskName, t);
            }
        };
        ScheduledFuture<?> f = delegate.scheduleAtFixedRate(gated, initialDelayMs, periodMs, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> prev = tasks.put(taskName, f);
        if (prev != null) {
            prev.cancel(false);
        }
        return f;
    }

    private boolean safeIsMaster() {
        try {
            return isMaster.getAsBoolean();
        } catch (Throwable t) {
            // Treat predicate failure as non-master to be safe.
            LOG.warn("master predicate threw, treating as non-master", t);
            return false;
        }
    }

    /** Run a one-shot task on the executor, also gated by the master predicate. */
    public void executeOnce(String taskName, Runnable task) {
        Objects.requireNonNull(taskName, "taskName");
        Objects.requireNonNull(task, "task");
        if (shutdown) {
            throw new IllegalStateException("executor is shutdown");
        }
        skipCounters.computeIfAbsent(taskName, k -> new AtomicLong());
        runCounters.computeIfAbsent(taskName, k -> new AtomicLong());
        delegate.submit(() -> {
            if (!safeIsMaster()) {
                skipCounters.get(taskName).incrementAndGet();
                return;
            }
            try {
                task.run();
                runCounters.get(taskName).incrementAndGet();
            } catch (Throwable t) {
                LOG.warn("master-only one-shot {} threw", taskName, t);
            }
        });
    }

    public long getSkippedTicks(String taskName) {
        AtomicLong c = skipCounters.get(taskName);
        return c == null ? 0L : c.get();
    }

    public long getExecutedTicks(String taskName) {
        AtomicLong c = runCounters.get(taskName);
        return c == null ? 0L : c.get();
    }

    public boolean isShutdown() {
        return shutdown;
    }

    /**
     * Cancel a previously-scheduled task by name. Returns {@code true} if a
     * task with that name was found and cancellation was attempted (the
     * underlying {@link ScheduledFuture#cancel(boolean)} is called with
     * {@code mayInterruptIfRunning=false}). The task name's run / skip
     * counters remain in place so post-cancellation observability still
     * works.
     */
    public boolean cancel(String taskName) {
        Objects.requireNonNull(taskName, "taskName");
        ScheduledFuture<?> f = tasks.remove(taskName);
        if (f == null) {
            return false;
        }
        f.cancel(false);
        return true;
    }

    /** Whether a task with the given name is currently scheduled. */
    public boolean isScheduled(String taskName) {
        ScheduledFuture<?> f = tasks.get(taskName);
        return f != null && !f.isCancelled() && !f.isDone();
    }

    public void shutdown() {
        shutdown = true;
        for (ScheduledFuture<?> f : tasks.values()) {
            f.cancel(false);
        }
        delegate.shutdown();
        try {
            if (!delegate.awaitTermination(5, TimeUnit.SECONDS)) {
                delegate.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            delegate.shutdownNow();
        }
    }
}
