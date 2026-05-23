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
import java.util.function.Consumer;
import java.util.function.Function;
import org.neo4j.driver.Record;
import org.neo4j.driver.summary.ResultSummary;

/**
 * Vert.x {@link Future}-based result cursor.
 *
 * <p>Returned futures complete on the Vert.x context captured when the cursor was created.
 */
public interface VertxResultCursor {
    /**
     * Consumes all remaining records and returns the result summary.
     *
     * @return a future completed with the result summary
     */
    Future<ResultSummary> consume();

    /**
     * Fetches the next record.
     *
     * @return a future completed with the next record or {@code null} when exhausted
     */
    Future<Record> next();

    /**
     * Peeks at the next record without consuming it.
     *
     * @return a future completed with the next record or {@code null} when exhausted
     */
    Future<Record> peek();

    /**
     * Returns the single record in the result.
     *
     * @return a future completed with the single record
     */
    Future<Record> single();

    /**
     * Applies an action to every remaining record.
     *
     * @param action record consumer
     * @return a future completed with the result summary
     */
    Future<ResultSummary> forEach(Consumer<Record> action);

    /**
     * Returns all remaining records.
     *
     * @return a future completed with all remaining records
     */
    Future<List<Record>> list();

    /**
     * Maps all remaining records.
     *
     * @param mapFunction record mapping function
     * @param <T> mapped record type
     * @return a future completed with all mapped records
     */
    <T> Future<List<T>> list(Function<Record, T> mapFunction);

    /**
     * Checks whether the cursor is open.
     *
     * @return a future completed with the open flag
     */
    Future<Boolean> isOpen();
}
