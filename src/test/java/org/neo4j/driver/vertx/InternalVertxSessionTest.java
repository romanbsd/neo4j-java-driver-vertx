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
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransactionCallback;
import org.neo4j.driver.async.AsyncTransactionContext;

class InternalVertxSessionTest {
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
    @SuppressWarnings("unchecked")
    void shouldAdaptManagedTransactionCallbackToCompletionStage() throws Exception {
        var delegate = mock(AsyncSession.class);
        var transactionContext = mock(AsyncTransactionContext.class);
        var config = TransactionConfig.empty();
        var callbackCaptor = ArgumentCaptor.forClass(AsyncTransactionCallback.class);
        when(delegate.executeReadAsync(callbackCaptor.capture(), eq(config))).thenAnswer(invocation -> {
            var callback = (AsyncTransactionCallback<CompletionStage<String>>) invocation.getArgument(0);
            return callback.execute(transactionContext);
        });
        var session = new InternalVertxSession(vertx.getOrCreateContext(), delegate);

        var result = session.executeRead(context -> {
                    assertThat(context, is(org.hamcrest.Matchers.instanceOf(VertxTransactionContext.class)));
                    return Future.succeededFuture("value");
                }, config)
                .toCompletionStage()
                .toCompletableFuture()
                .get(10, SECONDS);

        assertThat(result, is("value"));
        verify(delegate).executeReadAsync(any(), eq(config));
    }

    @Test
    void shouldDelegateCloseToAsyncSessionClose() throws Exception {
        var delegate = mock(AsyncSession.class);
        when(delegate.closeAsync()).thenReturn(completedFuture(null));
        var session = new InternalVertxSession(vertx.getOrCreateContext(), delegate);

        session.closeFuture().toCompletionStage().toCompletableFuture().get(10, SECONDS);

        verify(delegate).closeAsync();
    }
}
