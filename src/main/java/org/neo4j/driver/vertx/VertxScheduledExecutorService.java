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
        vertx.executeBlocking(() -> {
            command.run();
            return null;
        }, false).onComplete(ignored -> {
            if (ignored.failed()) {
                reportFailure(ignored.cause());
            }
            if (inflightCount.decrementAndGet() == 0) {
                signalTermination();
            }
        });
    }

    private <V> void executeBlocking(VertxScheduledTask<V> task, Callable<V> callable, BlockingResultHandler<V> handler) {
        vertx.executeBlocking(callable, false).onComplete(result -> {
            try {
                if (result.succeeded()) {
                    handler.handleResult(result.result());
                } else {
                    handler.handleFailure(result.cause());
                }
            } finally {
                if (task.isDone()) {
                    signalTermination();
                }
            }
        });
    }

    @Override
    public @NotNull ScheduledFuture<?> schedule(@NotNull Runnable command, long delay, TimeUnit unit) {
        rejectIfShutdown();
        var task = add(new VertxScheduledTask<>(unit.toNanos(delay), command, false));
        scheduleTimer(task, unit.toNanos(delay), task::runOnce);
        return task;
    }

    @Override
    public <V> @NotNull ScheduledFuture<V> schedule(@NotNull Callable<V> callable, long delay, TimeUnit unit) {
        rejectIfShutdown();
        var task = add(new VertxScheduledTask<>(unit.toNanos(delay), callable, false));
        scheduleTimer(task, unit.toNanos(delay), task::runOnce);
        return task;
    }

    @Override
    public @NotNull ScheduledFuture<?> scheduleAtFixedRate(@NotNull Runnable command, long initialDelay, long period, @NotNull TimeUnit unit) {
        if (period <= 0) {
            throw new IllegalArgumentException("period must be greater than zero");
        }
        rejectIfShutdown();
        var task = add(new VertxScheduledTask<>(unit.toNanos(initialDelay), command, true));
        scheduleFixedRate(task, System.nanoTime() + Math.max(0, unit.toNanos(initialDelay)), unit.toNanos(period));
        return task;
    }

    @Override
    public @NotNull ScheduledFuture<?> scheduleWithFixedDelay(@NotNull Runnable command, long initialDelay, long delay, @NotNull TimeUnit unit) {
        if (delay <= 0) {
            throw new IllegalArgumentException("delay must be greater than zero");
        }
        rejectIfShutdown();
        var task = add(new VertxScheduledTask<>(unit.toNanos(initialDelay), command, true));
        scheduleFixedDelay(task, Math.max(0, unit.toNanos(initialDelay)), unit.toNanos(delay));
        return task;
    }

    private void scheduleFixedRate(VertxScheduledTask<?> task, long scheduledNanos, long periodNanos) {
        scheduleTimer(task, scheduledNanos - System.nanoTime(), () -> task.runPeriodic(() -> {
            if (shutdown.get() || task.isCancelled()) {
                task.complete();
            } else {
                scheduleFixedRate(task, scheduledNanos + periodNanos, periodNanos);
            }
        }));
    }

    private void scheduleFixedDelay(VertxScheduledTask<?> task, long currentDelayNanos, long nextDelayNanos) {
        scheduleTimer(task, currentDelayNanos, () -> task.runFixedDelay(() -> {
            if (shutdown.get() || task.isCancelled()) {
                task.complete();
            } else {
                scheduleFixedDelay(task, nextDelayNanos, nextDelayNanos);
            }
        }));
    }

    private static long toTimerDelayMillis(long delayNanos) {
        if (delayNanos <= 0) {
            return 0;
        }
        return Math.max(1, TimeUnit.NANOSECONDS.toMillis(delayNanos));
    }

    private void scheduleTimer(VertxScheduledTask<?> task, long delayNanos, Runnable action) {
        var delayMillis = toTimerDelayMillis(delayNanos);
        if (delayMillis == 0) {
            vertx.runOnContext(ignored -> action.run());
        } else {
            var timerId = vertx.setTimer(delayMillis, ignored -> action.run());
            task.timerId(timerId);
        }
    }

    private void reportFailure(Throwable throwable) {
        vertx.runOnContext(ignored -> {
            if (throwable instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (throwable instanceof Error error) {
                throw error;
            }
            throw new RuntimeException(throwable);
        });
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
        private final AtomicBoolean done = new AtomicBoolean();
        private final CountDownLatch completion = new CountDownLatch(1);
        private final boolean periodic;
        private volatile V value;
        private volatile Throwable failure;
        private volatile boolean cancelled;
        private boolean running;
        private long timerId = -1;

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

        synchronized void timerId(long timerId) {
            this.timerId = timerId;
            if (cancelled || done.get()) {
                vertx.cancelTimer(timerId);
            }
        }

        void runOnce() {
            if (!startRun()) {
                return;
            }
            executeBlocking(this, callable, new BlockingResultHandler<>() {
                @Override
                public void handleResult(V result) {
                    synchronized (VertxScheduledTask.this) {
                        value = result;
                        running = false;
                        complete();
                    }
                }

                @Override
                public void handleFailure(Throwable throwable) {
                    synchronized (VertxScheduledTask.this) {
                        failure = throwable;
                        running = false;
                        complete();
                    }
                }
            });
        }

        void runPeriodic(Runnable next) {
            if (cancelled || shutdown.get() || done.get()) {
                complete();
                return;
            }
            if (!startRun()) {
                return;
            }
            executeBlocking(this, callable, nextHandler(next));
        }

        void runFixedDelay(Runnable next) {
            if (cancelled || done.get()) {
                complete();
                return;
            }
            if (!startRun()) {
                return;
            }
            executeBlocking(this, callable, nextHandler(next));
        }

        private synchronized boolean startRun() {
            if (cancelled || done.get()) {
                complete();
                return false;
            }
            if (running) {
                return false;
            }
            running = true;
            return true;
        }

        private BlockingResultHandler<V> nextHandler(Runnable next) {
            return new BlockingResultHandler<>() {
                @Override
                public void handleResult(V ignored) {
                    finishPeriodic(null, next);
                }

                @Override
                public void handleFailure(Throwable throwable) {
                    finishPeriodic(throwable, next);
                }

                private void finishPeriodic(Throwable throwable, Runnable next) {
                    boolean shouldContinue;
                    synchronized (VertxScheduledTask.this) {
                        if (throwable != null) {
                            failure = throwable;
                        }
                        running = false;
                        shouldContinue = failure == null && !cancelled && !shutdown.get();
                        if (!shouldContinue) {
                            complete();
                        }
                    }
                    if (shouldContinue) {
                        next.run();
                    }
                }
            };
        }

        synchronized void complete() {
            if (done.compareAndSet(false, true)) {
                if (timerId >= 0) {
                    vertx.cancelTimer(timerId);
                }
                remove(this);
                completion.countDown();
            }
        }

        private synchronized boolean cancel() {
            if (done.get()) {
                return false;
            }
            cancelled = true;
            if (timerId >= 0) {
                vertx.cancelTimer(timerId);
            }
            if (!running) {
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
    }

    private interface BlockingResultHandler<V> {
        void handleResult(V result);

        void handleFailure(Throwable throwable);
    }
}
