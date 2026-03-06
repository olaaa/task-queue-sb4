package com.example.paymentqueue.model;

import java.math.BigDecimal;

/**
 * Полная модель платёжного поручения — используется для обработки.
 */
public record PaymentTask(
        long id,
        short retryCount,
        BigDecimal amount,
        String fromAccount,
        String toAccount,
        String description
) {}
