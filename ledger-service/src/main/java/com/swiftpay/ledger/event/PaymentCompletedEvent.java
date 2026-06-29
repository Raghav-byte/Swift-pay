package com.swiftpay.ledger.event;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

public record PaymentCompletedEvent(
        UUID transactionId,
        UUID senderId,
        UUID receiverId,
        BigDecimal amount,
        String currency,
        OffsetDateTime completedAt) {
}
