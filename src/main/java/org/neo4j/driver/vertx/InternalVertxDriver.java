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
import io.vertx.core.Vertx;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Driver;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.async.AsyncSession;

final class InternalVertxDriver implements VertxDriver {
    private final Context defaultContext;
    private final Driver delegate;

    InternalVertxDriver(Vertx vertx, Driver delegate) {
        this.defaultContext = vertx.getOrCreateContext();
        this.delegate = delegate;
    }

    @Override
    public VertxSession session() {
        return session(SessionConfig.defaultConfig());
    }

    @Override
    public VertxSession session(SessionConfig sessionConfig) {
        return session(sessionConfig, null);
    }

    @Override
    public VertxSession session(SessionConfig sessionConfig, AuthToken sessionAuthToken) {
        return new InternalVertxSession(
                context(), delegate.session(AsyncSession.class, sessionConfig, sessionAuthToken));
    }

    @Override
    public boolean isEncrypted() {
        return delegate.isEncrypted();
    }

    @Override
    public Future<Void> verifyConnectivity() {
        return fromStage(context(), delegate.verifyConnectivityAsync());
    }

    @Override
    public Future<Boolean> supportsMultiDb() {
        return fromStage(context(), delegate.supportsMultiDbAsync());
    }

    @Override
    public Future<Void> closeFuture() {
        return fromStage(context(), delegate.closeAsync());
    }

    private Context context() {
        var currentContext = Vertx.currentContext();
        return currentContext != null ? currentContext : defaultContext;
    }
}
