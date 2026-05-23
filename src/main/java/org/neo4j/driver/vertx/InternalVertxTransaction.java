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
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.async.AsyncTransaction;

final class InternalVertxTransaction implements VertxTransaction {
    private final Context context;
    private final AsyncTransaction delegate;

    InternalVertxTransaction(Context context, AsyncTransaction delegate) {
        this.context = context;
        this.delegate = delegate;
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
    public Future<Void> commit() {
        return fromStage(context, delegate.commitAsync());
    }

    @Override
    public Future<Void> rollback() {
        return fromStage(context, delegate.rollbackAsync());
    }

    @Override
    public Future<Void> closeFuture() {
        return fromStage(context, delegate.closeAsync());
    }

    @Override
    public Future<Boolean> isOpen() {
        return fromStage(context, delegate.isOpenAsync());
    }
}
