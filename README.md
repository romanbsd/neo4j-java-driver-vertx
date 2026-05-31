# Neo4j Java Driver Vert.x

[![Maven Central](https://img.shields.io/maven-central/v/io.github.romanbsd/neo4j-java-driver-vertx.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.romanbsd/neo4j-java-driver-vertx)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Vert.x `Future` API wrapper for the Neo4j Java Driver.

This package reuses the official Neo4j Java Driver for protocol, routing, authentication, value mapping, and retry behavior, while adapting the public async surface to Vert.x `Future` and using a Vert.x-backed scheduler for driver timers.

## Requirements

- Java 17+
- Vert.x 5.x
- Neo4j Java Driver 6.1.x

## Installation

### Maven

```xml
<dependency>
  <groupId>io.github.romanbsd</groupId>
  <artifactId>neo4j-java-driver-vertx</artifactId>
  <version>0.3.0</version>
</dependency>
```

### Gradle

```gradle
implementation 'io.github.romanbsd:neo4j-java-driver-vertx:0.3.0'
```

The package declares its Neo4j Java Driver and Vert.x dependencies directly. If you need to align versions with your application, override the dependency versions in your own dependency management.

## Basic Usage

Create the driver from a Vert.x context, such as a verticle `start()` method, so callbacks complete on the caller context.

```java
package example;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Record;
import org.neo4j.driver.Values;
import org.neo4j.driver.vertx.VertxDriver;
import org.neo4j.driver.vertx.VertxGraphDatabase;

public final class MoviesVerticle extends AbstractVerticle {
    private VertxDriver driver;

    @Override
    public void start() {
        driver = VertxGraphDatabase.driver(
                vertx,
                "neo4j://localhost:7687",
                AuthTokens.basic("neo4j", "password"));

        driver.verifyConnectivity()
                .compose(ignored -> driver.session()
                        .run("MATCH (m:Movie {title: $title}) RETURN m.title AS title",
                                Values.parameters("title", "The Matrix")))
                .compose(cursor -> cursor.single())
                .map(Record::asMap)
                .onSuccess(row -> System.out.println("Movie: " + row.get("title")))
                .onFailure(Throwable::printStackTrace);
    }

    @Override
    public Future<Void> stop() {
        return driver != null ? driver.closeFuture() : Future.succeededFuture();
    }
}
```

## Managed Transactions

```java
var session = driver.session();

session.executeRead(tx -> tx.run("RETURN 1 AS n")
        .compose(cursor -> cursor.single())
        .map(record -> record.get("n").asInt()))
    .onSuccess(n -> System.out.println("n = " + n))
    .eventually(session::closeFuture);
```

## Closing

The API is intentionally asynchronous. `VertxDriver`, `VertxSession`, and `VertxTransaction` do not implement `AutoCloseable`; use `closeFuture()` instead.

```java
session.closeFuture()
        .compose(ignored -> driver.closeFuture())
        .onComplete(ar -> vertx.close());
```

## Notes

- The official Neo4j Java Driver remains the source of truth for Bolt protocol behavior.
- This package focuses on Vert.x `Future` adaptation, Vert.x context dispatch, and Vert.x-backed scheduling.
- Constructing the driver inside a Vert.x context is recommended. Operations invoked inside a Vert.x context complete on that caller context; operations invoked outside a context use the driver's fallback context.

## License

This project is licensed under the [Apache License, Version 2.0](LICENSE).
