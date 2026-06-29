package com.swiftpay.ledger.event;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

public record PaymentFailedEvent(
        UUID transactionId,
        UUID senderId,
        UUID receiverId,
        BigDecimal amount,
        String currency,
        String failureReason,
        OffsetDateTime failedAt) {
}
