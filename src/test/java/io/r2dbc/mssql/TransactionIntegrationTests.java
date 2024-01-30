/*
 * Copyright 2019-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.mssql;

import io.r2dbc.mssql.api.MssqlTransactionDefinition;
import io.r2dbc.mssql.util.IntegrationTestSupport;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Result;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for transactional behavior.
 *
 * @author Mark Paluch
 */
class TransactionIntegrationTests extends IntegrationTestSupport {

    @Test
    void savepointsSynchronized() {

        createTable(connection);

        connection.beginTransaction()
            .as(StepVerifier::create)
            .verifyComplete();

        Flux.from(connection.createStatement("INSERT INTO r2dbc_example VALUES(@P1, @P2, @P3)")
            .bind(0, 0).bind(1, "Walter").bind(2, "White").add()
            .bind(0, 1).bind(1, "Jesse").bind(2, "Pinkman").execute())
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNextCount(2)
            .verifyComplete();

        connection.createSavepoint("savepoint")
            .as(StepVerifier::create)
            .verifyComplete();

        Flux.from(connection.createStatement("INSERT INTO r2dbc_example VALUES(@P1, @P2, @P3)")
            .bind(0, 2).bind(1, "Hank").bind(2, "Schrader").execute())
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();

        Flux.from(connection.createStatement("SELECT COUNT(*) FROM r2dbc_example")
            .execute())
            .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, Integer.class)))
            .as(StepVerifier::create)
            .expectNext(3)
            .verifyComplete();

        connection.rollbackTransactionToSavepoint("savepoint")
            .as(StepVerifier::create)
            .verifyComplete();

        Flux.from(connection.createStatement("SELECT COUNT(*) FROM r2dbc_example /* in-tx */")
            .execute())
            .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, Integer.class)))
            .as(StepVerifier::create)
            .expectNext(2)
            .verifyComplete();

        connection.commitTransaction()
            .as(StepVerifier::create)
            .verifyComplete();

        Flux.from(connection.createStatement("SELECT COUNT(*) FROM r2dbc_example /* after-tx */")
            .execute())
            .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, Integer.class)))
            .as(StepVerifier::create)
            .expectNext(2)
            .verifyComplete();
    }

    @Test
    void savepointsConcatWith() {

        createTable(connection);

        connection.beginTransaction()
            .cast(Object.class)
            .concatWith(Flux.from(connection.createStatement("INSERT INTO r2dbc_example VALUES(@P1, @P2, @P3)")
                .bind(0, 0).bind(1, "Walter").bind(2, "White").execute())
                .flatMap(Result::getRowsUpdated))
            .concatWith(connection.createSavepoint("savepoint.1"))
            .concatWith(Flux.from(connection.createStatement("INSERT INTO r2dbc_example VALUES(@P1, @P2, @P3)")
                .bind(0, 2).bind(1, "Hank").bind(2, "Schrader").execute())
                .flatMap(Result::getRowsUpdated))
            .concatWith(connection.rollbackTransactionToSavepoint("savepoint.1"))
            .concatWith(Flux.from(connection.createStatement("SELECT COUNT(*) FROM r2dbc_example /* in-tx */")
                    .execute())
                .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, Integer.class))))
            .concatWith(connection.commitTransaction())
            .concatWith(Flux.from(connection.createStatement("SELECT COUNT(*) FROM r2dbc_example /* after-tx */")
                    .execute())
                .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, Integer.class))))
            .as(StepVerifier::create)
            .expectNext(1L).as("Affected Rows Count from first INSERT")
            .expectNext(1L).as("Affected Rows Count from second INSERT")
            .expectNext(1).as("SELECT COUNT(*) after ROLLBACK TO SAVEPOINT")
            .expectNext(1).as("SELECT COUNT(*) after COMMIT")
            .verifyComplete();
    }

    @Test
    void autoCommitDisabled() {

        createTable(connection);

        assertThat(connection.isAutoCommit()).isTrue();

        connection.setAutoCommit(false)
            .as(StepVerifier::create)
            .verifyComplete();

        assertThat(connection.isAutoCommit()).isFalse();

        connection.createStatement("INSERT INTO r2dbc_example VALUES(0, 'Walter', 'White')").execute()
            .<Object>flatMap(Result::getRowsUpdated)
            .concatWith(connection.rollbackTransaction())
            .as(StepVerifier::create)
            .expectNext(1L).as("Affected Rows Count from first INSERT")
            .verifyComplete();

        connectionFactory.create().flatMapMany(c -> c.createStatement("SELECT * FROM r2dbc_example")
            .execute().flatMap(it -> it.map((row, metadata) -> row.get("first_name"))))
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void savepointStartsTransaction() {

        createTable(connection);

        connection.createStatement("INSERT INTO r2dbc_example VALUES(1, 'Jesse', 'Pinkman')")
            .execute().flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1L)
            .verifyComplete();

        connection.createSavepoint("s1")
            .thenMany(connection.createStatement("INSERT INTO r2dbc_example VALUES(0, 'Walter', 'White')").execute()
                .<Object>flatMap(Result::getRowsUpdated))
            .concatWith(Mono.fromSupplier(() -> connection.isAutoCommit()))
            .concatWith(connection.rollbackTransaction())
            .as(StepVerifier::create)
            .expectNext(1L).as("Affected Rows Count from first INSERT")
            .expectNext(false).as("Auto-commit disabled by createSavepoint")
            .verifyComplete();

        connection.createStatement("INSERT INTO r2dbc_example VALUES(0, 'Walter', 'White')").execute()
            .<Object>flatMap(Result::getRowsUpdated)
            .concatWith(connection.rollbackTransaction())
            .as(StepVerifier::create)
            .expectNext(1L).as("Affected Rows Count from second INSERT")
            .verifyComplete();

        connectionFactory.create().flatMapMany(c -> c.createStatement("SELECT * FROM r2dbc_example")
            .execute().flatMap(it -> it.map((row, metadata) -> row.get("first_name"))))
            .as(StepVerifier::create)
            .expectNext("Jesse")
            .verifyComplete();
    }

    @Test
    void commitTransaction() {

        createTable(connection);

        connection.beginTransaction()
            .as(StepVerifier::create)
            .verifyComplete();

        Flux.from(connection.createStatement("INSERT INTO r2dbc_example VALUES(@P1, @P2, @P3)")
            .bind(0, 0).bind(1, "Walter").bind(2, "White").add()
            .bind(0, 1).bind(1, "Jesse").bind(2, "Pinkman").execute())
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNextCount(2)
            .verifyComplete();

        connection.commitTransaction()
            .as(StepVerifier::create)
            .verifyComplete();

        Flux.from(connection.createStatement("SELECT COUNT(*) FROM r2dbc_example")
            .execute())
            .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, Integer.class)))
            .as(StepVerifier::create)
            .expectNext(2)
            .verifyComplete();
    }

    @Test
    void rollbackTransaction() {

        createTable(connection);

        connection.beginTransaction()
            .as(StepVerifier::create)
            .verifyComplete();

        Flux.from(connection.createStatement("INSERT INTO r2dbc_example VALUES(@P1, @P2, @P3)")
            .bind(0, 0).bind(1, "Walter").bind(2, "White").add()
            .bind(0, 1).bind(1, "Jesse").bind(2, "Pinkman").execute())
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNextCount(2)
            .verifyComplete();

        connection.rollbackTransaction()
            .as(StepVerifier::create)
            .verifyComplete();

        Flux.from(connection.createStatement("SELECT COUNT(*) FROM r2dbc_example")
            .execute())
            .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, Integer.class)))
            .as(StepVerifier::create)
            .expectNext(0)
            .verifyComplete();
    }

    @Test
    void shouldBeginExtendedTransaction() {

        getTransactionCount()
            .as(StepVerifier::create)
            .expectNext(0)
            .verifyComplete();

        connection.beginTransaction(MssqlTransactionDefinition.from(IsolationLevel.READ_UNCOMMITTED)
            .name("foo-1").mark("bar")
            .lockTimeout(Duration.ofMinutes(1))).as(StepVerifier::create).verifyComplete();

        getTransactionCount()
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();

        connection.createStatement("SELECT @@LOCK_TIMEOUT").execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get(0, Long.class)))
            .as(StepVerifier::create)
            .expectNext(TimeUnit.MINUTES.toMillis(1))
            .verifyComplete();

        getIsolationLevel()
            .as(StepVerifier::create)
            .expectNext(MssqlIsolationLevel.READ_UNCOMMITTED)
            .verifyComplete();

        connection.rollbackTransaction().as(StepVerifier::create).verifyComplete();

        getTransactionCount()
            .as(StepVerifier::create)
            .expectNext(0)
            .verifyComplete();
    }

    private Flux<Integer> getTransactionCount() {
        return connection.createStatement("SELECT @@TRANCOUNT").execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get(0, Integer.class)));
    }

    @Test
    void shouldRestoreAutoCommitAfterExtendedTx() {

        getImplicitTransactions().as(StepVerifier::create).expectNext(false).verifyComplete();

        connection.beginTransaction(IsolationLevel.READ_UNCOMMITTED).as(StepVerifier::create).verifyComplete();
        assertThat(connection.isAutoCommit()).isFalse();
        getImplicitTransactions().as(StepVerifier::create).expectNext(false).verifyComplete();

        connection.rollbackTransaction().as(StepVerifier::create).verifyComplete();
        assertThat(connection.isAutoCommit()).isTrue();

        // huh?
        getImplicitTransactions().as(StepVerifier::create).expectNext(false).verifyComplete();

        connection.setAutoCommit(true).as(StepVerifier::create).verifyComplete();
        assertThat(connection.isAutoCommit()).isTrue();
        getImplicitTransactions().as(StepVerifier::create).expectNext(false).verifyComplete();

        assertAutoCommit();
    }

    private void assertAutoCommit() {
        createTable(connection);

        connection.createStatement("INSERT INTO r2dbc_example VALUES(0, 'Walter', 'White')")
            .execute()
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();

        getTransactionCount()
            .as(StepVerifier::create)
            .expectNext(0)
            .verifyComplete();

        MssqlConnection connection = connectionFactory.create().block();

        connection.createStatement("SELECT COUNT(*) FROM r2dbc_example")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get(0, Integer.class)))
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();

        connection.close().as(StepVerifier::create).verifyComplete();
    }

    @Test
    void shouldResetIsolationLevelAfterTransaction() {

        getIsolationLevel()
            .as(StepVerifier::create)
            .expectNext(MssqlIsolationLevel.READ_COMMITTED)
            .verifyComplete();

        connection.beginTransaction(IsolationLevel.READ_UNCOMMITTED).as(StepVerifier::create).verifyComplete();

        connection.rollbackTransaction().as(StepVerifier::create).verifyComplete();

        getIsolationLevel()
            .as(StepVerifier::create)
            .expectNext(MssqlIsolationLevel.READ_COMMITTED)
            .verifyComplete();

        assertThat(connection.getTransactionIsolationLevel()).isEqualTo(MssqlIsolationLevel.READ_COMMITTED);
    }

    @Test
    void shouldResetLockTimeoutAfterTransaction() {

        connection.createStatement("SELECT @@LOCK_TIMEOUT").execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get(0, Integer.class)))
            .as(StepVerifier::create)
            .expectNext(-1)
            .verifyComplete();

        connection.beginTransaction(MssqlTransactionDefinition.named("foo").lockTimeout(Duration.ofMinutes(60))).as(StepVerifier::create).verifyComplete();
        connection.rollbackTransaction().as(StepVerifier::create).verifyComplete();

        connection.createStatement("SELECT @@LOCK_TIMEOUT").execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get(0, Integer.class)))
            .as(StepVerifier::create)
            .expectNext(-1)
            .verifyComplete();
    }

    @Test
    void shouldGetRowsUpdatedInUpsert() throws Exception {
        createTable(connection);
        String insertSql = "INSERT INTO r2dbc_example (id, first_name, last_name) VALUES(1, 'Jessi', 'Pinkmann')";

        
        reactor.core.publisher.Flux<Long> flux = connection
                .createStatement(insertSql)
                .execute()
                .flatMap(MssqlResult::getRowsUpdated);

        java.util.concurrent.CompletableFuture<Long> future = reactor.core.publisher.Mono.from(flux).toFuture();
        Long result = future.get(2, java.util.concurrent.TimeUnit.SECONDS);
        org.junit.jupiter.api.Assertions.assertEquals(1L, result);

        String updateSql = "UPDATE r2dbc_example SET id = 1, first_name = 'Jesse', last_name = 'Pinkman' WHERE id = 1";
        reactor.core.publisher.Flux<Long> fluxUpdate = connection
                .createStatement(updateSql)
                .execute()
                .flatMap(MssqlResult::getRowsUpdated);
        java.util.concurrent.CompletableFuture<Long> futureUpdate = reactor.core.publisher.Mono.from(fluxUpdate).toFuture();
        Long resultUpdate = futureUpdate.get(2, java.util.concurrent.TimeUnit.SECONDS);
        org.junit.jupiter.api.Assertions.assertEquals(1L, resultUpdate);

        //clean the state
        createTable(connection);

        String upsertSQl = updateSql + " if @@ROWCOUNT = 0 " + insertSql;
        reactor.core.publisher.Flux<Long> fluxUpsert = connection
                .createStatement(upsertSQl)
                .execute()
                .flatMap(MssqlResult::getRowsUpdated);

        java.util.concurrent.CompletableFuture<Long> futureUpsert = reactor.core.publisher.Mono.from(fluxUpsert).toFuture();
        Long resultUpsert = futureUpsert.get(2, java.util.concurrent.TimeUnit.SECONDS);

        // should also have 1 updated row (from the insert part of the upsert)
        org.junit.jupiter.api.Assertions.assertEquals(1L, resultUpsert);

        reactor.core.publisher.Flux<Long> secondFluxUpsert = connection
                .createStatement(upsertSQl)
                .execute()
                .flatMap(MssqlResult::getRowsUpdated);

        java.util.concurrent.CompletableFuture<Long> secondFutureUpsert = reactor.core.publisher.Mono.from(secondFluxUpsert).toFuture();
        Long secondResultUpsert = secondFutureUpsert.get(2, java.util.concurrent.TimeUnit.SECONDS);

        // should also have 1 updated row (from the update part of the upsert)
        org.junit.jupiter.api.Assertions.assertEquals(1L, secondResultUpsert);
    }


    Mono<IsolationLevel> getIsolationLevel() {

        return connection.createStatement("SELECT CASE transaction_isolation_level \n" +
            "WHEN 0 THEN 'UNSPECIFIED' \n" +
            "WHEN 1 THEN 'READ_UNCOMMITTED' \n" +
            "WHEN 2 THEN 'READ_COMMITTED' \n" +
            "WHEN 3 THEN 'REPEATABLE_READ' \n" +
            "WHEN 4 THEN 'SERIALIZABLE' \n" +
            "WHEN 5 THEN 'SNAPSHOT' END AS TRANSACTION_ISOLATION_LEVEL \n" +
            "FROM sys.dm_exec_sessions \n" +
            "where session_id = @@SPID").execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get(0, String.class)))
            .map(it -> {

                switch (it) {
                    case "READ_UNCOMMITTED":
                        return MssqlIsolationLevel.READ_UNCOMMITTED;
                    case "READ_COMMITTED":
                        return MssqlIsolationLevel.READ_COMMITTED;
                    case "SERIALIZABLE":
                        return MssqlIsolationLevel.SERIALIZABLE;
                    case "REPEATABLE_READ":
                        return MssqlIsolationLevel.REPEATABLE_READ;
                    case "SNAPSHOT":
                        return MssqlIsolationLevel.SNAPSHOT;
                }

                return MssqlIsolationLevel.UNSPECIFIED;
            }).single();
    }

    Mono<Boolean> getImplicitTransactions() {

        return connection.createStatement("SELECT @@OPTIONS AS IMPLICIT_TRANSACTIONS;").execute()
            .flatMap(it -> it.map((row, rowMetadata) -> (row.get(0, Integer.class) & 2) == 2))
            .single();
    }

    private void createTable(MssqlConnection connection) {

        connection.createStatement("DROP TABLE r2dbc_example").execute()
            .flatMap(MssqlResult::getRowsUpdated)
            .onErrorResume(e -> Mono.empty())
            .thenMany(connection.createStatement("CREATE TABLE r2dbc_example (" +
                "id int PRIMARY KEY, " +
                "first_name varchar(255), " +
                "last_name varchar(255))")
                .execute().flatMap(MssqlResult::getRowsUpdated).then())
            .as(StepVerifier::create)
            .verifyComplete();
    }
}
