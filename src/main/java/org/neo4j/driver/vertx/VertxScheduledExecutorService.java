/*
 * Copyright (c) 2026 Roman Shterenzon
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.vertx;

import io.vertx.core.Vertx;
import org.jetbrains.annotations.NotNull;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

final class VertxScheduledExecutorService extends AbstractExecutorService implements ScheduledExecutorService {
    private final Vertx vertx;
    private final Object monitor = new Object();
    private final Set<VertxScheduledTask<?>> tasks = new HashSet<>();
    private final AtomicBoolean shutdown = new AtomicBoolean();
    private final AtomicInteger inflightCount = new AtomicInteger();

    VertxScheduledExecutorService(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            signalTermination();
        }
    }

    @Override
    public @NotNull List<Runnable> shutdownNow() {
        shutdown.set(true);
        List<VertxScheduledTask<?>> currentTasks;
        synchronized (monitor) {
            currentTasks = List.copyOf(tasks);
        }
        currentTasks.forEach(VertxScheduledTask::cancel);
        signalTermination();
        return List.copyOf(currentTasks);
    }

    @Override
    public boolean isShutdown() {
        return shutdown.get();
    }

    @Override
    public boolean isTerminated() {
        synchronized (monitor) {
            return isTerminatedLocked();
        }
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        var remainingNanos = unit.toNanos(timeout);
        var deadlineNanos = System.nanoTime() + remainingNanos;
        synchronized (monitor) {
            while (!isTerminatedLocked()) {
                if (remainingNanos <= 0) {
                    return false;
                }
                TimeUnit.NANOSECONDS.timedWait(monitor, remainingNanos);
                remainingNanos = deadlineNanos - System.nanoTime();
            }
            return true;
        }
    }

    @Override
    public void execute(@NotNull Runnable command) {
        rejectIfShutdown();
        inflightCount.incrementAndGet();
        vertx.runOnContext(ignored -> {
            try {
                command.run();
            } finally {
                if (inflightCount.decrementAndGet() == 0) {
                    synchronized (monitor) {
                        monitor.notifyAll();
                    }
                }
            }
        });
    }

    @Override
    public @NotNull ScheduledFuture<?> schedule(@NotNull Runnable command, long delay, TimeUnit unit) {
        rejectIfShutdown();
        var task = add(new VertxScheduledTask<>(unit.toNanos(delay), command, false));
        var timerId = vertx.setTimer(Math.max(0, unit.toMillis(delay)), id -> task.runOnce());
        task.timerId(timerId);
        return task;
    }

    @Override
    public <V> @NotNull ScheduledFuture<V> schedule(@NotNull Callable<V> callable, long delay, TimeUnit unit) {
        rejectIfShutdown();
        var task = add(new VertxScheduledTask<>(unit.toNanos(delay), callable, false));
        var timerId = vertx.setTimer(Math.max(0, unit.toMillis(delay)), id -> task.runOnce());
        task.timerId(timerId);
        return task;
    }

    @Override
    public @NotNull ScheduledFuture<?> scheduleAtFixedRate(@NotNull Runnable command, long initialDelay, long period, TimeUnit unit) {
        rejectIfShutdown();
        var task = add(new VertxScheduledTask<>(unit.toNanos(initialDelay), command, true));
        var timerId = vertx.setPeriodic(Math.max(0, unit.toMillis(initialDelay)), Math.max(1, unit.toMillis(period)), id -> {
            task.runPeriodic();
            if (task.isDone()) {
                vertx.cancelTimer(id);
            }
        });
        task.timerId(timerId);
        return task;
    }

    @Override
    public @NotNull ScheduledFuture<?> scheduleWithFixedDelay(@NotNull Runnable command, long initialDelay, long delay, TimeUnit unit) {
        rejectIfShutdown();
        var delayMillis = Math.max(1, unit.toMillis(delay));
        var task = add(new VertxScheduledTask<>(unit.toNanos(initialDelay), command, true));
        scheduleFixedDelay(task, Math.max(0, unit.toMillis(initialDelay)), delayMillis);
        return task;
    }

    private void scheduleFixedDelay(VertxScheduledTask<?> task, long currentDelayMillis, long nextDelayMillis) {
        var timerId = vertx.setTimer(currentDelayMillis, id -> task.runFixedDelay(() -> {
            if (shutdown.get() || task.isCancelled()) {
                task.complete();
            } else {
                scheduleFixedDelay(task, nextDelayMillis, nextDelayMillis);
            }
        }));
        task.timerId(timerId);
    }

    private <V> VertxScheduledTask<V> add(VertxScheduledTask<V> task) {
        synchronized (monitor) {
            tasks.add(task);
        }
        return task;
    }

    private void remove(VertxScheduledTask<?> task) {
        synchronized (monitor) {
            tasks.remove(task);
            monitor.notifyAll();
        }
    }

    private void signalTermination() {
        synchronized (monitor) {
            monitor.notifyAll();
        }
    }

    private boolean isTerminatedLocked() {
        return shutdown.get() && tasks.isEmpty() && inflightCount.get() == 0;
    }

    private void rejectIfShutdown() {
        if (shutdown.get()) {
            throw new RejectedExecutionException("executor has been shut down");
        }
    }

    private final class VertxScheduledTask<V> implements RunnableScheduledFuture<V> {
        private final long createdNanos = System.nanoTime();
        private final long delayNanos;
        private final Callable<V> callable;
        private final AtomicLong timerId = new AtomicLong(-1);
        private final AtomicBoolean running = new AtomicBoolean();
        private final AtomicBoolean done = new AtomicBoolean();
        private final CountDownLatch completion = new CountDownLatch(1);
        private final boolean periodic;
        private volatile V value;
        private volatile Throwable failure;
        private volatile boolean cancelled;

        VertxScheduledTask(long delayNanos, Runnable runnable, boolean periodic) {
            this(delayNanos, () -> {
                runnable.run();
                return null;
            }, periodic);
        }

        VertxScheduledTask(long delayNanos, Callable<V> callable, boolean periodic) {
            this.delayNanos = delayNanos;
            this.callable = callable;
            this.periodic = periodic;
        }

        void timerId(long timerId) {
            this.timerId.set(timerId);
            if (cancelled) {
                vertx.cancelTimer(timerId);
            }
        }

        void runOnce() {
            if (cancelled || done.get()) {
                complete();
                return;
            }
            if (!running.compareAndSet(false, true)) {
                return;
            }
            try {
                value = callable.call();
            } catch (Throwable throwable) {
                failure = throwable;
            } finally {
                synchronized (this) {
                    running.set(false);
                    complete();
                }
            }
        }

        void runPeriodic() {
            if (cancelled || shutdown.get() || done.get()) {
                complete();
                return;
            }
            if (!running.compareAndSet(false, true)) {
                return;
            }
            try {
                callable.call();
            } catch (Throwable throwable) {
                failure = throwable;
                complete();
            } finally {
                synchronized (this) {
                    running.set(false);
                    if (cancelled || shutdown.get()) {
                        complete();
                    }
                }
            }
        }

        void runFixedDelay(Runnable next) {
            if (cancelled || done.get()) {
                complete();
                return;
            }
            if (!running.compareAndSet(false, true)) {
                return;
            }
            try {
                callable.call();
            } catch (Throwable throwable) {
                failure = throwable;
            } finally {
                boolean shouldContinue;
                synchronized (this) {
                    running.set(false);
                    shouldContinue = failure == null && !cancelled && !shutdown.get();
                    if (!shouldContinue) {
                        complete();
                    }
                }
                if (shouldContinue) {
                    next.run();
                }
            }
        }

        @Override
        public boolean isPeriodic() {
            return periodic;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            var deadline = createdNanos + delayNanos;
            return unit.convert(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            return Long.compare(getDelay(TimeUnit.NANOSECONDS), other.getDelay(TimeUnit.NANOSECONDS));
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return cancel();
        }

        synchronized void complete() {
            if (done.compareAndSet(false, true)) {
                remove(this);
                completion.countDown();
            }
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public boolean isDone() {
            return done.get();
        }

        @Override
        public V get() throws InterruptedException, ExecutionException {
            completion.await();
            return resolve();
        }

        @Override
        public V get(long timeout, @NotNull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            if (!completion.await(timeout, unit)) {
                throw new TimeoutException();
            }
            return resolve();
        }

        @Override
        public void run() {
            runOnce();
        }

        private synchronized boolean cancel() {
            if (done.get()) {
                return false;
            }
            cancelled = true;
            var id = timerId.get();
            if (id >= 0) {
                vertx.cancelTimer(id);
            }
            if (!running.get()) {
                complete();
            }
            return true;
        }

        private V resolve() throws ExecutionException {
            if (cancelled) {
                throw new CancellationException();
            }
            if (failure != null) {
                throw new ExecutionException(failure);
            }
            return value;
        }
    }
}
