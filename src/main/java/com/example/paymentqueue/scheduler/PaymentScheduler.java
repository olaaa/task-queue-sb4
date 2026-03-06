package com.example.paymentqueue.scheduler;

import com.example.paymentqueue.dao.PaymentTaskDao;
import com.example.paymentqueue.service.PaymentQueueService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;

/**
 * Два scheduled job-а:
 *  1. processNextPayment  — берёт и исполняет следующее поручение из очереди.
 *  2. resetStalePayments  — сбрасывает зависшие поручения обратно в WAIT.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentScheduler {

    private final PaymentQueueService service;
    private final PaymentTaskDao dao;

    @Value("${payment-queue.stale-timeout-minutes:5}")
    private int staleTimeoutMinutes;

    /**
     * Основной воркер.
     *
     * fixedDelay — следующий запуск начинается через N мс ПОСЛЕ завершения предыдущего.
     * subscribe() — запускает реактивную цепочку. Без него Mono — просто описание,
     * выполнение не начнётся. В @Scheduled нет внешнего subscriber-а.
     */
    @Scheduled(fixedDelayString = "${payment-queue.scheduler-interval-ms:2000}") // 2 сек
    public void processNextPayment() {
        service.lockNext()
                .flatMap(service::process)
                .subscribe(
                        null,
                        ex -> log.error("Ошибка обработки платежа: {}", ex.getMessage(), ex)
                );
    }

    /**
     * Сторож зависших поручений. Запускается каждую минуту.
     */
    @Scheduled(fixedDelay = 60_000)
    public void resetStalePayments() {
        OffsetDateTime pointcut = OffsetDateTime.now().minusMinutes(staleTimeoutMinutes);
        dao.resetStale(pointcut)
                .subscribe(
                        null,
                        ex -> log.error("Ошибка в resetStale: {}", ex.getMessage(), ex)
                );
    }
}
