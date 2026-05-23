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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.ResultCursor;

class InternalVertxDriverTest {
    private Vertx vertx;

    @BeforeEach
    void setUp() {
        vertx = Vertx.vertx();
    }

    @AfterEach
    void tearDown() throws Exception {
        vertx.close().toCompletionStage().toCompletableFuture().get(10, SECONDS);
    }

    @Test
    void shouldDelegateDriverOperationsToAsyncDriverMethods() throws Exception {
        var delegate = mock(Driver.class);
        when(delegate.verifyConnectivityAsync()).thenReturn(completedFuture(null));
        when(delegate.supportsMultiDbAsync()).thenReturn(completedFuture(true));
        when(delegate.closeAsync()).thenReturn(completedFuture(null));

        var driver = VertxGraphDatabase.driver(vertx, delegate);

        driver.verifyConnectivity().toCompletionStage().toCompletableFuture().get(10, SECONDS);
        assertThat(driver.supportsMultiDb().toCompletionStage().toCompletableFuture().get(10, SECONDS), is(true));
        driver.closeFuture().toCompletionStage().toCompletableFuture().get(10, SECONDS);

        verify(delegate).verifyConnectivityAsync();
        verify(delegate).supportsMultiDbAsync();
        verify(delegate).closeAsync();
        verify(delegate, never()).verifyConnectivity();
        verify(delegate, never()).supportsMultiDb();
        verify(delegate, never()).close();
    }

    @Test
    void shouldCreateAsyncSessionAndWrapCursorResults() throws Exception {
        var delegate = mock(Driver.class);
        var asyncSession = mock(AsyncSession.class);
        var cursor = mock(ResultCursor.class);
        var config = SessionConfig.defaultConfig();
        when(delegate.session(eq(AsyncSession.class), eq(config), isNull())).thenReturn(asyncSession);
        when(asyncSession.runAsync("RETURN 1")).thenReturn(completedFuture(cursor));
        when(cursor.isOpenAsync()).thenReturn(completedFuture(true));

        var driver = VertxGraphDatabase.driver(vertx, delegate);
        var resultCursor = driver.session(config).run("RETURN 1").toCompletionStage().toCompletableFuture().get(10, SECONDS);

        assertThat(resultCursor, instanceOf(VertxResultCursor.class));
        assertThat(resultCursor.isOpen().toCompletionStage().toCompletableFuture().get(10, SECONDS), is(true));
        verify(delegate).session(AsyncSession.class, config, null);
        verify(delegate, never()).session(Session.class, config, null);
        verify(asyncSession).runAsync("RETURN 1");
        verify(cursor).isOpenAsync();
    }

    @Test
    void shouldCompleteReturnedFutureOnVertxContext() throws Exception {
        var delegate = mock(Driver.class);
        var connectivity = new CompletableFuture<Void>();
        when(delegate.verifyConnectivityAsync()).thenReturn(connectivity);
        var driver = VertxGraphDatabase.driver(vertx, delegate);
        var completedOnVertxContext = new AtomicBoolean();
        var latch = new CountDownLatch(1);

        driver.verifyConnectivity().onComplete(result -> {
            completedOnVertxContext.set(Vertx.currentContext() != null);
            latch.countDown();
        });
        connectivity.complete(null);

        assertThat(latch.await(10, SECONDS), is(true));
        assertThat(completedOnVertxContext.get(), is(true));
    }

    @Test
    void shouldReuseOneVertxContextForDriverOperationsCreatedOutsideVertxContext() throws Exception {
        var delegate = mock(Driver.class);
        var connectivity = new CompletableFuture<Void>();
        var multiDb = new CompletableFuture<Boolean>();
        when(delegate.verifyConnectivityAsync()).thenReturn(connectivity);
        when(delegate.supportsMultiDbAsync()).thenReturn(multiDb);
        var driver = VertxGraphDatabase.driver(vertx, delegate);
        var firstContext = new AtomicReference<Context>();
        var secondContext = new AtomicReference<Context>();
        var latch = new CountDownLatch(2);

        driver.verifyConnectivity().onComplete(result -> {
            firstContext.set(Vertx.currentContext());
            latch.countDown();
        });
        driver.supportsMultiDb().onComplete(result -> {
            secondContext.set(Vertx.currentContext());
            latch.countDown();
        });
        connectivity.complete(null);
        multiDb.complete(true);

        assertThat(latch.await(10, SECONDS), is(true));
        assertThat(firstContext.get(), sameInstance(secondContext.get()));
    }

    @Test
    void shouldCompleteDriverOperationOnCallerContextWhenDriverWasCreatedOutsideContext() throws Exception {
        var delegate = mock(Driver.class);
        var connectivity = new CompletableFuture<Void>();
        when(delegate.verifyConnectivityAsync()).thenReturn(connectivity);
        var driver = VertxGraphDatabase.driver(vertx, delegate);
        var callerContext = new AtomicReference<Context>();
        var completionContext = new AtomicReference<Context>();
        var latch = new CountDownLatch(1);

        vertx.runOnContext(ignored -> {
            callerContext.set(Vertx.currentContext());
            driver.verifyConnectivity().onComplete(result -> {
                completionContext.set(Vertx.currentContext());
                latch.countDown();
            });
            connectivity.complete(null);
        });

        assertThat(latch.await(10, SECONDS), is(true));
        assertThat(completionContext.get(), sameInstance(callerContext.get()));
    }

    @Test
    void shouldCompleteAlreadyCompletedStageImmediatelyWhenAdaptingOnSameContext() throws Exception {
        var completedImmediately = new AtomicBoolean();
        var latch = new CountDownLatch(1);

        vertx.runOnContext(ignored -> {
            var future = FutureAdapter.fromStage(Vertx.currentContext(), completedFuture("done"));

            completedImmediately.set(future.succeeded() && "done".equals(future.result()));
            latch.countDown();
        });

        assertThat(latch.await(10, SECONDS), is(true));
        assertThat(completedImmediately.get(), is(true));
    }
}
