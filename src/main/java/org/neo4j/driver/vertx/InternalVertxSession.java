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

import static org.neo4j.driver.vertx.FutureAdapter.fromStage;

import io.vertx.core.Context;
import io.vertx.core.Future;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.async.AsyncSession;

final class InternalVertxSession implements VertxSession {
    private final Context context;
    private final AsyncSession delegate;

    InternalVertxSession(Context context, AsyncSession delegate) {
        this.context = context;
        this.delegate = delegate;
    }

    @Override
    public Future<VertxTransaction> beginTransaction() {
        return fromStage(context, delegate.beginTransactionAsync(), tx -> new InternalVertxTransaction(context, tx));
    }

    @Override
    public Future<VertxTransaction> beginTransaction(TransactionConfig config) {
        return fromStage(
                context, delegate.beginTransactionAsync(config), tx -> new InternalVertxTransaction(context, tx));
    }

    @Override
    public <T> Future<T> executeRead(Function<VertxTransactionContext, Future<T>> callback) {
        return executeRead(callback, TransactionConfig.empty());
    }

    @Override
    public <T> Future<T> executeRead(Function<VertxTransactionContext, Future<T>> callback, TransactionConfig config) {
        return fromStage(
                context,
                delegate.executeReadAsync(
                        tx -> callback.apply(new InternalVertxTransactionContext(context, tx)).toCompletionStage(),
                        config));
    }

    @Override
    public <T> Future<T> executeWrite(Function<VertxTransactionContext, Future<T>> callback) {
        return executeWrite(callback, TransactionConfig.empty());
    }

    @Override
    public <T> Future<T> executeWrite(Function<VertxTransactionContext, Future<T>> callback, TransactionConfig config) {
        return fromStage(
                context,
                delegate.executeWriteAsync(
                        tx -> callback.apply(new InternalVertxTransactionContext(context, tx)).toCompletionStage(),
                        config));
    }

    @Override
    public Future<VertxResultCursor> run(String query) {
        return fromStage(context, delegate.runAsync(query), cursor -> new InternalVertxResultCursor(context, cursor));
    }

    @Override
    public Future<VertxResultCursor> run(String query, Value parameters) {
        return fromStage(
                context, delegate.runAsync(query, parameters), cursor -> new InternalVertxResultCursor(context, cursor));
    }

    @Override
    public Future<VertxResultCursor> run(String query, Map<String, Object> parameters) {
        return fromStage(
                context, delegate.runAsync(query, parameters), cursor -> new InternalVertxResultCursor(context, cursor));
    }

    @Override
    public Future<VertxResultCursor> run(String query, Record parameters) {
        return fromStage(
                context, delegate.runAsync(query, parameters), cursor -> new InternalVertxResultCursor(context, cursor));
    }

    @Override
    public Future<VertxResultCursor> run(Query query) {
        return fromStage(context, delegate.runAsync(query), cursor -> new InternalVertxResultCursor(context, cursor));
    }

    @Override
    public Future<VertxResultCursor> run(String query, TransactionConfig config) {
        return fromStage(
                context, delegate.runAsync(query, config), cursor -> new InternalVertxResultCursor(context, cursor));
    }

    @Override
    public Future<VertxResultCursor> run(String query, Map<String, Object> parameters, TransactionConfig config) {
        return fromStage(
                context,
                delegate.runAsync(query, parameters, config),
                cursor -> new InternalVertxResultCursor(context, cursor));
    }

    @Override
    public Future<VertxResultCursor> run(Query query, TransactionConfig config) {
        return fromStage(
                context, delegate.runAsync(query, config), cursor -> new InternalVertxResultCursor(context, cursor));
    }

    @Override
    public Set<Bookmark> lastBookmarks() {
        return delegate.lastBookmarks();
    }

    @Override
    public Future<Void> closeFuture() {
        return fromStage(context, delegate.closeAsync());
    }
}
