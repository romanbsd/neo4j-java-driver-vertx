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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Test;

class VertxCloseTest {
    @Test
    void asynchronousApisDoNotImplementAutoCloseable() {
        assertThat(AutoCloseable.class.isAssignableFrom(VertxDriver.class), is(false));
        assertThat(AutoCloseable.class.isAssignableFrom(VertxSession.class), is(false));
        assertThat(AutoCloseable.class.isAssignableFrom(VertxTransaction.class), is(false));
    }
}
