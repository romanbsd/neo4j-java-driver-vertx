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

import static java.util.Objects.requireNonNull;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.net.URI;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.ClientCertificateManager;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.internal.ValidatingClientCertificateManager;
import org.neo4j.driver.internal.security.StaticAuthTokenManager;
import org.neo4j.driver.internal.security.ValidatingAuthTokenManager;

/**
 * Factory for Vert.x {@link Future}-based Neo4j drivers.
 */
public final class VertxGraphDatabase {
    private VertxGraphDatabase() {}

    /**
     * Creates an unauthenticated Vert.x driver.
     *
     * @param vertx the Vert.x instance
     * @param uri the database URI
     * @return a Vert.x driver
     */
    public static VertxDriver driver(Vertx vertx, String uri) {
        return driver(vertx, URI.create(uri));
    }

    /**
     * Creates an unauthenticated Vert.x driver.
     *
     * @param vertx the Vert.x instance
     * @param uri the database URI
     * @return a Vert.x driver
     */
    public static VertxDriver driver(Vertx vertx, URI uri) {
        return driver(vertx, uri, Config.defaultConfig());
    }

    /**
     * Creates a Vert.x driver with a static authentication token.
     *
     * @param vertx the Vert.x instance
     * @param uri the database URI
     * @param authToken authentication token
     * @return a Vert.x driver
     */
    public static VertxDriver driver(Vertx vertx, String uri, AuthToken authToken) {
        return driver(vertx, URI.create(uri), authToken);
    }

    /**
     * Creates a Vert.x driver with a static authentication token.
     *
     * @param vertx the Vert.x instance
     * @param uri the database URI
     * @param authToken authentication token
     * @return a Vert.x driver
     */
    public static VertxDriver driver(Vertx vertx, URI uri, AuthToken authToken) {
        return driver(vertx, uri, authToken, Config.defaultConfig());
    }

    /**
     * Creates a Vert.x driver with a static authentication token and configuration.
     *
     * @param vertx the Vert.x instance
     * @param uri the database URI
     * @param authToken authentication token
     * @param config driver configuration
     * @return a Vert.x driver
     */
    public static VertxDriver driver(Vertx vertx, String uri, AuthToken authToken, Config config) {
        return driver(vertx, URI.create(uri), authToken, config);
    }

    /**
     * Creates a Vert.x driver with a static authentication token and configuration.
     *
     * @param vertx the Vert.x instance
     * @param uri the database URI
     * @param authToken authentication token
     * @param config driver configuration
     * @return a Vert.x driver
     */
    public static VertxDriver driver(Vertx vertx, URI uri, AuthToken authToken, Config config) {
        if (authToken == null) {
            authToken = AuthTokens.none();
        }
        return driver(vertx, uri, new StaticAuthTokenManager(authToken), null, config);
    }

    /**
     * Creates an unauthenticated Vert.x driver with configuration.
     *
     * @param vertx the Vert.x instance
     * @param uri the database URI
     * @param config driver configuration
     * @return a Vert.x driver
     */
    public static VertxDriver driver(Vertx vertx, String uri, Config config) {
        return driver(vertx, URI.create(uri), config);
    }

    /**
     * Creates an unauthenticated Vert.x driver with configuration.
     *
     * @param vertx the Vert.x instance
     * @param uri the database URI
     * @param config driver configuration
     * @return a Vert.x driver
     */
    public static VertxDriver driver(Vertx vertx, URI uri, Config config) {
        return driver(vertx, uri, AuthTokens.none(), config);
    }

    /**
     * Creates a Vert.x driver with an authentication token manager.
     *
     * @param vertx the Vert.x instance
     * @param uri the database URI
     * @param authTokenManager authentication token manager
     * @return a Vert.x driver
     */
    public static VertxDriver driver(Vertx vertx, String uri, AuthTokenManager authTokenManager) {
        return driver(vertx, URI.create(uri), authTokenManager);
    }

    /**
     * Creates a Vert.x driver with an authentication token manager.
     *
     * @param vertx the Vert.x instance
     * @param uri the database URI
     * @param authTokenManager authentication token manager
     * @return a Vert.x driver
     */
    public static VertxDriver driver(Vertx vertx, URI uri, AuthTokenManager authTokenManager) {
        return driver(vertx, uri, authTokenManager, Config.defaultConfig());
    }

    /**
     * Creates a Vert.x driver with an authentication token manager and configuration.
     *
     * @param vertx the Vert.x instance
     * @param uri the database URI
     * @param authTokenManager authentication token manager
     * @param config driver configuration
     * @return a Vert.x driver
     */
    public static VertxDriver driver(Vertx vertx, String uri, AuthTokenManager authTokenManager, Config config) {
        return driver(vertx, URI.create(uri), authTokenManager, config);
    }

    /**
     * Creates a Vert.x driver with an authentication token manager and configuration.
     *
     * @param vertx the Vert.x instance
     * @param uri the database URI
     * @param authTokenManager authentication token manager
     * @param config driver configuration
     * @return a Vert.x driver
     */
    public static VertxDriver driver(Vertx vertx, URI uri, AuthTokenManager authTokenManager, Config config) {
        requireNonNull(authTokenManager, "authTokenManager must not be null");
        config = getOrDefault(config);
        return driver(vertx, uri, validatingAuthTokenManager(authTokenManager, config), null, config);
    }

    /**
     * Creates a Vert.x driver with a static authentication token, client certificate manager, and configuration.
     *
     * @param vertx the Vert.x instance
     * @param uri the database URI
     * @param authToken authentication token
     * @param clientCertificateManager client certificate manager
     * @param config driver configuration
     * @return a Vert.x driver
     */
    public static VertxDriver driver(
            Vertx vertx,
            String uri,
            AuthToken authToken,
            ClientCertificateManager clientCertificateManager,
            Config config) {
        return driver(vertx, URI.create(uri), authToken, clientCertificateManager, config);
    }

    /**
     * Creates a Vert.x driver with a static authentication token, client certificate manager, and configuration.
     *
     * @param vertx the Vert.x instance
     * @param uri the database URI
     * @param authToken authentication token
     * @param clientCertificateManager client certificate manager
     * @param config driver configuration
     * @return a Vert.x driver
     */
    public static VertxDriver driver(
            Vertx vertx,
            URI uri,
            AuthToken authToken,
            ClientCertificateManager clientCertificateManager,
            Config config) {
        if (authToken == null) {
            authToken = AuthTokens.none();
        }
        return driver(
                vertx, uri, new StaticAuthTokenManager(authToken), clientCertificateManager, getOrDefault(config));
    }

    static VertxDriver driver(Vertx vertx, Driver driver) {
        return new InternalVertxDriver(vertx, driver);
    }

    private static VertxDriver driver(
            Vertx vertx,
            URI uri,
            AuthTokenManager authTokenManager,
            ClientCertificateManager clientCertificateManager,
            Config config) {
        requireNonNull(vertx, "vertx must not be null");
        requireNonNull(uri, "uri must not be null");
        requireNonNull(authTokenManager, "authTokenManager must not be null");
        if (clientCertificateManager != null) {
            clientCertificateManager = new ValidatingClientCertificateManager(clientCertificateManager);
        }
        var driver = new VertxDriverFactory()
                .newInstance(
                        uri,
                        authTokenManager,
                        clientCertificateManager,
                        getOrDefault(config),
                        null,
                        new VertxScheduledExecutorService(vertx),
                        null);
        return new InternalVertxDriver(vertx, driver);
    }

    private static Config getOrDefault(Config config) {
        return config != null ? config : Config.defaultConfig();
    }

    @SuppressWarnings("deprecation")
    private static ValidatingAuthTokenManager validatingAuthTokenManager(AuthTokenManager authTokenManager, Config config) {
        return new ValidatingAuthTokenManager(authTokenManager, config.logging());
    }
}
