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
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import org.neo4j.driver.Record;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.summary.ResultSummary;

final class InternalVertxResultCursor implements VertxResultCursor {
    private final Context context;
    private final ResultCursor delegate;

    InternalVertxResultCursor(Context context, ResultCursor delegate) {
        this.context = context;
        this.delegate = delegate;
    }

    @Override
    public Future<ResultSummary> consume() {
        return fromStage(context, delegate.consumeAsync());
    }

    @Override
    public Future<Record> next() {
        return fromStage(context, delegate.nextAsync());
    }

    @Override
    public Future<Record> peek() {
        return fromStage(context, delegate.peekAsync());
    }

    @Override
    public Future<Record> single() {
        return fromStage(context, delegate.singleAsync());
    }

    @Override
    public Future<ResultSummary> forEach(Consumer<Record> action) {
        return fromStage(context, delegate.forEachAsync(action));
    }

    @Override
    public Future<List<Record>> list() {
        return fromStage(context, delegate.listAsync());
    }

    @Override
    public <T> Future<List<T>> list(Function<Record, T> mapFunction) {
        return fromStage(context, delegate.listAsync(mapFunction));
    }

    @Override
    public Future<Boolean> isOpen() {
        return fromStage(context, delegate.isOpenAsync());
    }
}
