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
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
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
}
