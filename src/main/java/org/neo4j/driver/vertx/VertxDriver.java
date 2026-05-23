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

import io.vertx.core.Future;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.SessionConfig;

/**
 * Vert.x {@link Future}-based Neo4j driver.
 *
 * <p>Returned futures complete on the Vert.x context that is current when an operation is initiated. If an operation is
 * initiated outside a Vert.x context, they complete on the driver context captured when the driver was created.
 */
public interface VertxDriver {
    /**
     * Creates a session with default session configuration.
     *
     * @return a Vert.x session
     */
    VertxSession session();

    /**
     * Creates a session with the supplied session configuration.
     *
     * @param sessionConfig the session configuration
     * @return a Vert.x session
     */
    VertxSession session(SessionConfig sessionConfig);

    /**
     * Creates a session with the supplied session configuration and session authentication token.
     *
     * @param sessionConfig the session configuration
     * @param sessionAuthToken the session authentication token
     * @return a Vert.x session
     */
    VertxSession session(SessionConfig sessionConfig, AuthToken sessionAuthToken);

    /**
     * Returns whether this driver was configured to use encrypted connections.
     *
     * @return {@code true} when encrypted
     */
    boolean isEncrypted();

    /**
     * Verifies connectivity to the remote database.
     *
     * @return a future completed when connectivity has been verified
     */
    Future<Void> verifyConnectivity();

    /**
     * Checks whether the remote database supports multiple databases.
     *
     * @return a future completed with the support flag
     */
    Future<Boolean> supportsMultiDb();

    /**
     * Closes this driver asynchronously.
     *
     * @return a future completed when the driver is closed
     */
    Future<Void> closeFuture();

}
