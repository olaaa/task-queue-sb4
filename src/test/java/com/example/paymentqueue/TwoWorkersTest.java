package com.example.paymentqueue;

import com.example.paymentqueue.dao.PaymentTaskDao;
import com.example.paymentqueue.scheduler.PaymentScheduler;
import com.example.paymentqueue.service.PaymentQueueService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;


import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Интеграционные тесты паттерна FOR UPDATE SKIP LOCKED.
 * Testcontainers поднимает PostgreSQL в Docker автоматически.
 */
@Slf4j
@SpringBootTest
@Testcontainers
class TwoWorkersTest {

    @MockitoBean
    PaymentScheduler paymentScheduler;

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:15")
            .withDatabaseName("paymentqueue")
            .withUsername("postgres")
            .withPassword("postgres");

    @Autowired
    PaymentQueueService service;

    @Autowired
    PaymentTaskDao dao;

    @Autowired
    DatabaseClient client;

    @BeforeEach
    void cleanUp() {
        client.sql("DELETE FROM payment_task")
                .fetch().rowsUpdated()
                .block();
    }

    /**
     * Два воркера стартуют одновременно — каждое поручение захватывается ровно раз.
     */
    @Test
    void twoWorkersShouldNotStepOnEachOther() {
        insertPayments(4).block();

        AtomicInteger totalLocked = new AtomicInteger(0);

        // Flux.merge подписывается на оба потока одновременно
        Mono<Void> worker1 = drainQueue(1, totalLocked);
        Mono<Void> worker2 = drainQueue(2, totalLocked);

        StepVerifier.create(Flux.merge(worker1, worker2))
                .verifyComplete();

        assertThat(totalLocked.get()).isEqualTo(4);
        assertThat(countByState("WAIT").block()).isEqualTo(0);
        assertThat(countByState("PROCESSING").block()).isEqualTo(4);
    }

    /**
     * Зависшая задача сбрасывается обратно в WAIT, retry_count увеличивается.
     */
    @Test
    void stalePaymentShouldBeReset() {
        insertPayments(1).block();

        service.lockNext().block();

        // Имитируем зависший воркер
        client.sql("UPDATE payment_task SET modified_at = :t WHERE state = 'PROCESSING'")
                .bind("t", OffsetDateTime.now().minusMinutes(10))
                .fetch()
                .rowsUpdated()
                .block();

        Long reset = dao.resetStale(OffsetDateTime.now().minusMinutes(5)).block();
        assertThat(reset).isEqualTo(1);
        assertThat(countByState("WAIT").block()).isEqualTo(1);

        Short retryCount = client
                .sql("SELECT retry_count FROM payment_task WHERE state = 'WAIT'")
                .map(row -> Objects.requireNonNull(row.get("retry_count", Short.class)))
                .one()
                .block();
        assertThat(retryCount).isEqualTo((short) 1);
    }

    // ──────────────────────────────────────────────────────────────────────────

    @SuppressWarnings("Convert2MethodRef")
    private Mono<Void> drainQueue(int workerId, AtomicInteger counter) {
        return Flux.defer(() -> service.lockNext()
                        .map(paymentTaskId -> Optional.of(paymentTaskId))
                        .defaultIfEmpty(Optional.empty()))
//        После завершения текущего прохода подписывается снова.
                .repeat()
//        Продолжаем цикл, пока приходят Optional с задачей.
                .takeWhile(Optional::isPresent)
// После takeWhile здесь гарантированно только непустые Optional, поэтому безопасно извлекается taskId.
                .map(Optional::get)
                .doOnNext(taskId -> {
                    counter.incrementAndGet();
                    log.info("Воркер {} захватил платёж id={}", workerId, taskId.id());
                })
                .then();
    }

    private reactor.core.publisher.Mono<Void> insertPayments(int count) {
        return Flux.range(1, count)
                .flatMap(i -> client.sql("""
                                INSERT INTO payment_task (amount, from_account, to_account, description)
                                VALUES (:amount, :from, :to, :desc)
                                """)
                        .bind("amount", BigDecimal.valueOf(i * 1000L))
                        .bind("from", "40817810000000000001")
                        .bind("to", "40817810000000000002")
                        .bind("desc", "Тестовый платёж " + i)
                        .fetch().rowsUpdated()
                )
                .then();
    }

    private reactor.core.publisher.Mono<Integer> countByState(String state) {
        return client.sql("SELECT COUNT(*) FROM payment_task WHERE state = :state::payment_state")
                .bind("state", state)
                .map(row -> Objects.requireNonNull(row.get(0, Integer.class)))
                .one();
    }
}
