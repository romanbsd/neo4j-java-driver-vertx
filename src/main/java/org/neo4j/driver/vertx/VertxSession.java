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
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;

/**
 * Vert.x {@link Future}-based Neo4j session.
 *
 * <p>Returned futures complete on the Vert.x context that is current when an operation is initiated. If an operation is
 * initiated outside a Vert.x context, they complete on the session context captured when the session was created.
 */
public interface VertxSession extends VertxQueryRunner {
    /**
     * Begins an unmanaged transaction.
     *
     * @return a future completed with the transaction
     */
    Future<VertxTransaction> beginTransaction();

    /**
     * Begins an unmanaged transaction with transaction configuration.
     *
     * @param config transaction configuration
     * @return a future completed with the transaction
     */
    Future<VertxTransaction> beginTransaction(TransactionConfig config);

    /**
     * Executes a managed read transaction.
     *
     * @param callback transaction callback
     * @param <T> callback result type
     * @return a future completed with the callback result
     */
    <T> Future<T> executeRead(Function<VertxTransactionContext, Future<T>> callback);

    /**
     * Executes a managed read transaction with transaction configuration.
     *
     * @param callback transaction callback
     * @param config transaction configuration
     * @param <T> callback result type
     * @return a future completed with the callback result
     */
    <T> Future<T> executeRead(Function<VertxTransactionContext, Future<T>> callback, TransactionConfig config);

    /**
     * Executes a managed write transaction.
     *
     * @param callback transaction callback
     * @param <T> callback result type
     * @return a future completed with the callback result
     */
    <T> Future<T> executeWrite(Function<VertxTransactionContext, Future<T>> callback);

    /**
     * Executes a managed write transaction with transaction configuration.
     *
     * @param callback transaction callback
     * @param config transaction configuration
     * @param <T> callback result type
     * @return a future completed with the callback result
     */
    <T> Future<T> executeWrite(Function<VertxTransactionContext, Future<T>> callback, TransactionConfig config);

    /**
     * Runs an auto-commit query with transaction configuration.
     *
     * @param query the query text
     * @param config transaction configuration
     * @return a future completed with a result cursor
     */
    Future<VertxResultCursor> run(String query, TransactionConfig config);

    /**
     * Runs an auto-commit query with parameters and transaction configuration.
     *
     * @param query the query text
     * @param parameters query parameters
     * @param config transaction configuration
     * @return a future completed with a result cursor
     */
    Future<VertxResultCursor> run(String query, Map<String, Object> parameters, TransactionConfig config);

    /**
     * Runs an auto-commit query with transaction configuration.
     *
     * @param query the query
     * @param config transaction configuration
     * @return a future completed with a result cursor
     */
    Future<VertxResultCursor> run(Query query, TransactionConfig config);

    /**
     * Returns the latest bookmarks received by this session.
     *
     * @return latest bookmarks
     */
    Set<Bookmark> lastBookmarks();

    /**
     * Closes this session asynchronously.
     *
     * @return a future completed when the session is closed
     */
    Future<Void> closeFuture();

}
