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
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.Value;

/**
 * Common Vert.x query execution contract.
 *
 * <p>Returned futures complete on the Vert.x context selected by the owning driver, session, transaction, or managed
 * transaction context.
 */
public interface VertxQueryRunner {
    /**
     * Runs a query without parameters.
     *
     * @param query the query text
     * @return a future completed with a result cursor
     */
    Future<VertxResultCursor> run(String query);

    /**
     * Runs a query with value parameters.
     *
     * @param query the query text
     * @param parameters query parameters
     * @return a future completed with a result cursor
     */
    Future<VertxResultCursor> run(String query, Value parameters);

    /**
     * Runs a query with map parameters.
     *
     * @param query the query text
     * @param parameters query parameters
     * @return a future completed with a result cursor
     */
    Future<VertxResultCursor> run(String query, Map<String, Object> parameters);

    /**
     * Runs a query with record parameters.
     *
     * @param query the query text
     * @param parameters query parameters
     * @return a future completed with a result cursor
     */
    Future<VertxResultCursor> run(String query, Record parameters);

    /**
     * Runs a query object.
     *
     * @param query the query
     * @return a future completed with a result cursor
     */
    Future<VertxResultCursor> run(Query query);

    /**
     * Runs a query and consumes the full result stream.
     *
     * <p>Useful for write queries (schema changes, MERGE/SET batches, etc.) where the caller only needs a summary and
     * wants to avoid boilerplate {@code run(...).compose(VertxResultCursor::consume)}.
     *
     * @param query the query text
     * @return a future completed with the Neo4j result summary
     */
    default Future<ResultSummary> runAndConsume(String query) {
        return run(query).compose(VertxResultCursor::consume);
    }

    /**
     * Runs a query with map parameters and consumes the full result stream.
     *
     * @param query the query text
     * @param parameters query parameters
     * @return a future completed with the Neo4j result summary
     */
    default Future<ResultSummary> runAndConsume(String query, Map<String, Object> parameters) {
        return run(query, parameters).compose(VertxResultCursor::consume);
    }

    /**
     * Runs a query with value parameters and consumes the full result stream.
     *
     * @param query the query text
     * @param parameters query parameters
     * @return a future completed with the Neo4j result summary
     */
    default Future<ResultSummary> runAndConsume(String query, Value parameters) {
        return run(query, parameters).compose(VertxResultCursor::consume);
    }

    /**
     * Runs a query with record parameters and consumes the full result stream.
     *
     * @param query the query text
     * @param parameters query parameters
     * @return a future completed with the Neo4j result summary
     */
    default Future<ResultSummary> runAndConsume(String query, Record parameters) {
        return run(query, parameters).compose(VertxResultCursor::consume);
    }

    /**
     * Runs a query object and consumes the full result stream.
     *
     * @param query the query
     * @return a future completed with the Neo4j result summary
     */
    default Future<ResultSummary> runAndConsume(Query query) {
        return run(query).compose(VertxResultCursor::consume);
    }

    /**
     * Runs a query without parameters and returns the mapped record list.
     *
     * @param query the query text
     * @param mapper record mapper
     * @param <T> mapped row type
     * @return a future completed with the mapped list
     */
    default <T> Future<List<T>> runAndList(String query, Function<Record, T> mapper) {
        return run(query).compose(cursor -> cursor.list(mapper));
    }

    /**
     * Runs a query with value parameters and returns the mapped record list.
     *
     * @param query the query text
     * @param parameters query parameters
     * @param mapper record mapper
     * @param <T> mapped row type
     * @return a future completed with the mapped list
     */
    default <T> Future<List<T>> runAndList(String query, Value parameters, Function<Record, T> mapper) {
        return run(query, parameters).compose(cursor -> cursor.list(mapper));
    }

    /**
     * Runs a query with map parameters and returns the mapped record list.
     *
     * @param query the query text
     * @param parameters query parameters
     * @param mapper record mapper
     * @param <T> mapped row type
     * @return a future completed with the mapped list
     */
    default <T> Future<List<T>> runAndList(String query, Map<String, Object> parameters, Function<Record, T> mapper) {
        return run(query, parameters).compose(cursor -> cursor.list(mapper));
    }

    /**
     * Runs a query with record parameters and returns the mapped record list.
     *
     * @param query the query text
     * @param parameters query parameters
     * @param mapper record mapper
     * @param <T> mapped row type
     * @return a future completed with the mapped list
     */
    default <T> Future<List<T>> runAndList(String query, Record parameters, Function<Record, T> mapper) {
        return run(query, parameters).compose(cursor -> cursor.list(mapper));
    }

    /**
     * Runs a query object and returns the mapped record list.
     *
     * @param query the query
     * @param mapper record mapper
     * @param <T> mapped row type
     * @return a future completed with the mapped list
     */
    default <T> Future<List<T>> runAndList(Query query, Function<Record, T> mapper) {
        return run(query).compose(cursor -> cursor.list(mapper));
    }
}
