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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.vertx.core.Vertx;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class VertxScheduledExecutorServiceTest {
    private Vertx vertx;
    private VertxScheduledExecutorService executor;

    @BeforeEach
    void setUp() {
        vertx = Vertx.vertx();
        executor = new VertxScheduledExecutorService(vertx);
    }

    @AfterEach
    void tearDown() throws Exception {
        executor.shutdownNow();
        vertx.close().toCompletionStage().toCompletableFuture().get(10, SECONDS);
    }

    @Test
    void scheduledFutureGetWaitsForTaskCompletion() throws Exception {
        var started = System.nanoTime();
        var future = executor.schedule(() -> "done", 80, MILLISECONDS);

        assertThat(future.get(1, SECONDS), is("done"));
        assertThat(MILLISECONDS.convert(System.nanoTime() - started, NANOSECONDS), greaterThanOrEqualTo(60L));
    }

    @Test
    void awaitTerminationWaitsForRunningTask() throws Exception {
        var started = new CountDownLatch(1);
        var finished = new CountDownLatch(1);
        executor.execute(() -> {
            started.countDown();
            sleep(100);
            finished.countDown();
        });
        assertThat(started.await(1, SECONDS), is(true));

        var shutdownStarted = System.nanoTime();
        executor.shutdown();

        assertThat(executor.awaitTermination(1, SECONDS), is(true));
        assertThat(finished.getCount(), is(0L));
        assertThat(MILLISECONDS.convert(System.nanoTime() - shutdownStarted, NANOSECONDS), greaterThanOrEqualTo(60L));
    }

    @Test
    void scheduleWithFixedDelayMeasuresDelayAfterPreviousRunCompletes() throws Exception {
        var runCount = new AtomicInteger();
        var firstEndNanos = new AtomicLong();
        var secondStartNanos = new AtomicLong();
        var secondRun = new CountDownLatch(1);

        var future = executor.scheduleWithFixedDelay(
                () -> {
                    if (runCount.incrementAndGet() == 1) {
                        sleep(80);
                        firstEndNanos.set(System.nanoTime());
                    } else {
                        secondStartNanos.compareAndSet(0, System.nanoTime());
                        secondRun.countDown();
                    }
                },
                1,
                70,
                MILLISECONDS);

        assertThat(secondRun.await(1, SECONDS), is(true));
        assertThat(
                MILLISECONDS.convert(secondStartNanos.get() - firstEndNanos.get(), NANOSECONDS),
                greaterThanOrEqualTo(50L));
        future.cancel(false);
    }

    @Test
    void periodicTasksReportThatTheyArePeriodic() {
        var future = executor.scheduleAtFixedRate(() -> {}, 1, 10, MILLISECONDS);

        assertThat(((RunnableScheduledFuture<?>) future).isPeriodic(), is(true));
        future.cancel(false);
    }

    @Test
    void cancellingPeriodicTaskWhileItIsRunningCompletesFuture() throws Exception {
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var future = executor.scheduleAtFixedRate(
                () -> {
                    started.countDown();
                    await(release);
                },
                1,
                10,
                MILLISECONDS);
        assertThat(started.await(1, SECONDS), is(true));

        future.cancel(false);
        release.countDown();
        executor.shutdown();

        assertThrows(CancellationException.class, () -> future.get(1, SECONDS));
        assertThat(future.isCancelled(), is(true));
        assertThat(future.isDone(), is(true));
        assertThat(executor.awaitTermination(1, SECONDS), is(true));
    }

    @Test
    void shutdownAllowsAlreadyScheduledDelayedTasksToRun() throws Exception {
        var future = executor.schedule(() -> "done", 50, MILLISECONDS);

        executor.shutdown();

        assertThat(future.get(1, SECONDS), is("done"));
        assertThat(executor.awaitTermination(1, SECONDS), is(true));
    }

    @Test
    void shutdownNowCancelsScheduledTasksAfterGracefulShutdownStarted() throws Exception {
        var future = executor.schedule(() -> "done", 1, SECONDS);

        executor.shutdown();
        executor.shutdownNow();

        assertThrows(CancellationException.class, () -> future.get(1, SECONDS));
        assertThat(executor.awaitTermination(1, SECONDS), is(true));
    }

    @Test
    void shutdownNowReturnsScheduledTasksThatNeverStarted() {
        var future = executor.schedule(() -> "done", 1, SECONDS);

        var queuedTasks = executor.shutdownNow();

        assertThat(queuedTasks, hasSize(1));
        assertThat(queuedTasks.get(0), sameInstance(future));
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            //noinspection ResultOfMethodCallIgnored
            latch.await(1, SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
