package com.example.paymentqueue.dao;

import com.example.paymentqueue.model.PaymentState;
import com.example.paymentqueue.model.PaymentTask;
import com.example.paymentqueue.model.PaymentTaskId;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;

@Slf4j
@Repository
@RequiredArgsConstructor
public class PaymentTaskDao {

    private final DatabaseClient client;

    // ──────────────────────────────────────────────────────────────────────────
    // Захват задачи
    // ──────────────────────────────────────────────────────────────────────────

    /**
     * Находит следующее платёжное поручение в WAIT и блокирует его строку.
     *
     * FOR UPDATE      — блокирует строку до COMMIT/ROLLBACK текущей транзакции.
     * SKIP LOCKED     — если строка занята другим воркером — пропустить, взять следующую.
     *                   Без этого все воркеры выстраивались бы в очередь за одной строкой.
     * ORDER BY modified_at — честная FIFO очередь: старейшее поручение первым.
     * LIMIT 1         — один воркер берёт одно поручение за раз.
     *
     * ВАЖНО: выполняется внутри @Transactional — блокировка живёт до COMMIT.
     */
    private static final String FIND_NEXT = """
            SELECT id, retry_count
            FROM payment_task
            WHERE state = 'WAIT'
            ORDER BY modified_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            """;

    public Mono<PaymentTaskId> findNextWithSkipLock() {
        return client.sql(FIND_NEXT)
                .map(row -> new PaymentTaskId(
                        row.get("id", Long.class),
                        row.get("retry_count", Short.class)
                ))
                .one();
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Смена состояния
    // ──────────────────────────────────────────────────────────────────────────

    /**
     * Переводит задачу из одного состояния в другое.
     *
     * AND retry_count = :retryCount — проверка "я всё ещё тот воркер?".
     * Если планировщик вызвал resetStale — retry_count вырос, UPDATE вернёт 0.
     * Воркер получит Mono.empty() и остановится.
     */
    private static final String STATE_TRANSITION = """
            UPDATE payment_task
            SET state = :to::payment_state,
                modified_at = CURRENT_TIMESTAMP
            WHERE id = :id
              AND state = :from::payment_state
              AND retry_count = :retryCount
            """;

    public Mono<Long> stateTransition(long id, short retryCount,
                                      PaymentState from, PaymentState to) {
        return client.sql(STATE_TRANSITION)
                .bind("id", id)
                .bind("retryCount", retryCount)
                .bind("from", from.name())
                .bind("to", to.name())
                .fetch()
                .rowsUpdated();
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Получение данных платежа
    // ──────────────────────────────────────────────────────────────────────────

    private static final String FIND_BY_ID = """
            SELECT id, retry_count, amount, from_account, to_account, description
            FROM payment_task
            WHERE id = :id
            """;

    public Mono<PaymentTask> findById(long id) {
        return client.sql(FIND_BY_ID)
                .bind("id", id)
                .map(row -> new PaymentTask(
                        row.get("id", Long.class),
                        row.get("retry_count", Short.class),
                        row.get("amount", java.math.BigDecimal.class),
                        row.get("from_account", String.class),
                        row.get("to_account", String.class),
                        row.get("description", String.class)
                ))
                .one();
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Heartbeat — "я живой"
    // ──────────────────────────────────────────────────────────────────────────

    /**
     * Обновляет modified_at чтобы планировщик не считал задачу зависшей.
     * Вызывается воркером во время долгой обработки.
     *
     * Если вернул 0 — retry_count изменился, воркер должен остановиться.
     */
    private static final String HEARTBEAT = """
            UPDATE payment_task
            SET modified_at = CURRENT_TIMESTAMP
            WHERE id = :id
              AND retry_count = :retryCount
            """;

    public Mono<Long> heartbeat(long id, short retryCount) {
        return client.sql(HEARTBEAT)
                .bind("id", id)
                .bind("retryCount", retryCount)
                .fetch()
                .rowsUpdated();
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Сброс зависших задач
    // ──────────────────────────────────────────────────────────────────────────

    /**
     * Сбрасывает поручения, застрявшие в PROCESSING.
     *
     * После сброса: state = WAIT, retry_count + 1.
     * Старый воркер при следующей записи получит 0 rows updated и остановится.
     */
    private static final String RESET_STALE = """
            UPDATE payment_task
            SET state = 'WAIT',
                retry_count = retry_count + 1,
                modified_at = CURRENT_TIMESTAMP
            WHERE state = 'PROCESSING'
              AND modified_at < :pointcut
            """;

    // на пять минут в прошлое
    public Mono<Long> resetStale(OffsetDateTime pointcut) {
        return client.sql(RESET_STALE)
                .bind("pointcut", pointcut)
                .fetch()
                .rowsUpdated()
                .doOnNext(count -> {
                    if (count > 0) {
                        log.warn("resetStale: сброшено {} зависших платёжных поручений", count);
                    }
                });
    }
}
