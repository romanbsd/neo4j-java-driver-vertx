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

/**
 * Vert.x {@link Future}-based unmanaged transaction.
 *
 * <p>Returned futures complete on the Vert.x context captured when the transaction was created.
 */
public interface VertxTransaction extends VertxQueryRunner {
    /**
     * Commits this transaction.
     *
     * @return a future completed when the transaction is committed
     */
    Future<Void> commit();

    /**
     * Rolls back this transaction.
     *
     * @return a future completed when the transaction is rolled back
     */
    Future<Void> rollback();

    /**
     * Closes this transaction asynchronously.
     *
     * @return a future completed when the transaction is closed
     */
    Future<Void> closeFuture();

    /**
     * Checks whether this transaction is open.
     *
     * @return a future completed with the open flag
     */
    Future<Boolean> isOpen();

}
