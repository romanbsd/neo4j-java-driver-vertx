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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Record;
import org.neo4j.driver.summary.ResultSummary;

class VertxQueryRunnerDefaultsTest {
    @Test
    void runAndConsume_runsAndConsumesCursor() {
        VertxQueryRunner runner = mock(VertxQueryRunner.class, CALLS_REAL_METHODS);
        VertxResultCursor cursor = mock(VertxResultCursor.class);
        ResultSummary summary = mock(ResultSummary.class);

        when(runner.run("RETURN 1")).thenReturn(Future.succeededFuture(cursor));
        when(cursor.consume()).thenReturn(Future.succeededFuture(summary));

        ResultSummary result = runner.runAndConsume("RETURN 1").result();
        assertSame(summary, result);
        verify(runner).run("RETURN 1");
        verify(cursor).consume();
    }

    @Test
    void runAndList_runsAndMapsRecords() {
        VertxQueryRunner runner = mock(VertxQueryRunner.class, CALLS_REAL_METHODS);
        VertxResultCursor cursor = mock(VertxResultCursor.class);

        Function<Record, String> mapper = r -> "x";

        when(runner.run("MATCH (n) RETURN n", Map.of("k", "v"))).thenReturn(Future.succeededFuture(cursor));
        when(cursor.list(same(mapper))).thenReturn(Future.succeededFuture(List.of("x")));

        List<String> out = runner.runAndList("MATCH (n) RETURN n", Map.of("k", "v"), mapper).result();
        assertEquals(1, out.size());
        verify(runner).run("MATCH (n) RETURN n", Map.of("k", "v"));
        verify(cursor).list(same(mapper));
    }
}

