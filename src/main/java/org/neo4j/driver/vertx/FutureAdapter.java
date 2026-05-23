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

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

final class FutureAdapter {
    private FutureAdapter() {}

    static <T> Future<T> fromStage(Context context, CompletionStage<T> stage) {
        var promise = Promise.<T>promise();
        stage.whenComplete((value, throwable) -> {
            if (Vertx.currentContext() == context) {
                complete(promise, value, throwable);
            } else {
                context.runOnContext(ignored -> complete(promise, value, throwable));
            }
        });
        return promise.future();
    }

    static <T, R> Future<R> fromStage(Context context, CompletionStage<T> stage, Function<T, R> mapper) {
        var promise = Promise.<R>promise();
        stage.whenComplete((value, throwable) -> {
            if (Vertx.currentContext() == context) {
                completeWith(promise, value, throwable, mapper);
            } else {
                context.runOnContext(ignored -> completeWith(promise, value, throwable, mapper));
            }
        });
        return promise.future();
    }

    static Throwable unwrap(Throwable throwable) {
        return throwable instanceof CompletionException && throwable.getCause() != null
                ? throwable.getCause()
                : throwable;
    }

    private static <T> void complete(Promise<T> promise, T value, Throwable throwable) {
        if (throwable != null) {
            promise.fail(unwrap(throwable));
        } else {
            promise.complete(value);
        }
    }

    private static <T, R> void completeWith(Promise<R> promise, T value, Throwable throwable, Function<T, R> mapper) {
        if (throwable != null) {
            promise.fail(unwrap(throwable));
        } else {
            promise.complete(mapper.apply(value));
        }
    }
}
