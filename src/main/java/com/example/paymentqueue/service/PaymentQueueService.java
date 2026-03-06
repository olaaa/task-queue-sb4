package com.example.paymentqueue.service;

import com.example.paymentqueue.dao.PaymentTaskDao;
import com.example.paymentqueue.model.PaymentState;
import com.example.paymentqueue.model.PaymentTask;
import com.example.paymentqueue.model.PaymentTaskId;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentQueueService {

    private final PaymentTaskDao dao;

    // ──────────────────────────────────────────────────────────────────────────
    // Захват задачи
    // ──────────────────────────────────────────────────────────────────────────

    /**
     * Трёхшаговый паттерн захвата платёжного поручения:
     *
     *  1. SELECT FOR UPDATE SKIP LOCKED — находим свободное поручение, блокируем строку.
     *  2. UPDATE state = PROCESSING    — помечаем что поручение наше.
     *     Если UPDATE вернул 0 — кто-то успел раньше, возвращаем Mono.empty().
     *  3. COMMIT (@Transactional)      — блокировка снята, поручение защищено через state.
     *
     * Транзакция в R2DBC передаётся через реактивный контекст (не ThreadLocal).
     * Нельзя разрывать цепочку через block() или subscribeOn(newThread).
     */
    @Transactional
    public Mono<PaymentTaskId> lockNext() {
        return dao.findNextWithSkipLock()
                .flatMap(taskId ->
                        dao.stateTransition(
                                taskId.id(),
                                taskId.retryCount(),
                                PaymentState.WAIT,
                                PaymentState.PROCESSING
                        )
                        .flatMap(updated -> {
                            if (updated > 0) {
                                log.info("Платёж id={} захвачен (retryCount={})",
                                        taskId.id(), taskId.retryCount());
                                return Mono.just(taskId);
                            } else {
                                log.debug("Платёж id={} уже захвачен другим воркером", taskId.id());
                                return Mono.<PaymentTaskId>empty();
                            }
                        })
                );
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Обработка платежа
    // ──────────────────────────────────────────────────────────────────────────

    /**
     * Обрабатывает захваченное платёжное поручение.
     *
     * НЕ аннотирован @Transactional — обработка долгая.
     * Каждый переход состояния — отдельная короткая транзакция.
     */
    public Mono<Void> process(PaymentTaskId taskId) {
        log.info("Начало обработки платежа id={}", taskId.id());

        return dao.findById(taskId.id())
                .flatMap(task ->
                        executePayment(task)
                                .then(completePayment(taskId, PaymentState.DONE))
                                .doOnSuccess(v -> log.info(
                                        "Платёж id={} на сумму {} успешно исполнен",
                                        task.id(), task.amount()))
                                .onErrorResume(ex -> {
                                    log.error("Ошибка исполнения платежа id={}: {}",
                                            taskId.id(), ex.getMessage());
                                    return completePayment(taskId, PaymentState.ERROR);
                                })
                );
    }

    /**
     * Имитация исполнения платежа с heartbeat между шагами.
     *
     * В реальном приложении здесь: вызов платёжного шлюза, обновление баланса,
     * запись в аудит-лог, отправка уведомления клиенту и т.п.
     */
    private Mono<Void> executePayment(PaymentTask task) {
        log.info("Исполняем платёж: {} -> {}, сумма: {}",
                task.fromAccount(), task.toAccount(), task.amount());

        // Шаг 1: проверка баланса
        return Mono.delay(Duration.ofMillis(300))
                .flatMap(t -> sendHeartbeat(task))
                // Шаг 2: резервирование средств
                .then(Mono.delay(Duration.ofMillis(300)))
                .flatMap(t -> sendHeartbeat(task))
                // Шаг 3: финальное проведение
                .then(Mono.delay(Duration.ofMillis(300)))
                .then();
    }

    /**
     * Heartbeat — сообщаем БД что воркер жив.
     * Если вернул 0 — retry_count изменился, прерываем обработку.
     */
    private Mono<Void> sendHeartbeat(PaymentTask task) {
        return dao.heartbeat(task.id(), task.retryCount())
                .flatMap(updated -> {
                    if (updated == 0) {
                        return Mono.error(new IllegalStateException(
                                "Heartbeat вернул 0 — платёж id=" + task.id() +
                                " перехвачен другим воркером"));
                    }
                    log.debug("Heartbeat отправлен для платежа id={}", task.id());
                    return Mono.<Void>empty();
                });
    }

    @Transactional
    public Mono<Void> completePayment(PaymentTaskId taskId, PaymentState finalState) {
        return dao.stateTransition(
                        taskId.id(),
                        taskId.retryCount(),
                        PaymentState.PROCESSING,
                        finalState
                )
                .doOnNext(updated -> {
                    if (updated == 0) {
                        log.warn("Платёж id={} был сброшен планировщиком — " +
                                "результат не сохранён", taskId.id());
                    }
                })
                .then();
    }
}
