package com.swiftpay.gateway.event;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

public record PaymentInitiatedEvent(
        UUID transactionId,
        UUID senderId,
        UUID receiverId,
        BigDecimal amount,
        String currency,
        OffsetDateTime timestamp) {
}
