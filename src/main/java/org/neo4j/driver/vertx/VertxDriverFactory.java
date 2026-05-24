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

import java.net.URI;
import java.time.Clock;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import org.neo4j.bolt.connection.BoltAgent;
import org.neo4j.bolt.connection.BoltConnectionProviderFactory;
import org.neo4j.bolt.connection.BoltConnectionSource;
import org.neo4j.bolt.connection.NotificationConfig;
import org.neo4j.bolt.connection.RoutedBoltConnectionParameters;
import org.neo4j.bolt.connection.pooled.SecurityPlanSupplier;
import org.neo4j.bolt.connection.routed.Rediscovery;
import org.neo4j.driver.Config;
import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.adaptedbolt.BoltObservationProvider;
import org.neo4j.driver.internal.boltlistener.BoltConnectionListener;

final class VertxDriverFactory extends DriverFactory {
    @Override
    protected BoltConnectionSource<RoutedBoltConnectionParameters> createBoltConnectionSource(
            URI uri,
            Config config,
            ScheduledExecutorService eventLoopGroup,
            Supplier<Rediscovery> rediscoverySupplier,
            BoltConnectionListener boltConnectionListener,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            BoltObservationProvider observationProvider,
            Clock clock,
            org.neo4j.bolt.connection.pooled.AuthTokenManager authTokenManager,
            SecurityPlanSupplier securityPlanSupplier,
            NotificationConfig notificationConfig,
            BoltConnectionProviderFactory boltConnectionProviderFactory) {
        return super.createBoltConnectionSource(
                uri,
                config,
                null,
                rediscoverySupplier,
                boltConnectionListener,
                boltAgent,
                userAgent,
                connectTimeoutMillis,
                observationProvider,
                clock,
                authTokenManager,
                securityPlanSupplier,
                notificationConfig,
                boltConnectionProviderFactory);
    }
}
