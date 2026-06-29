package com.swiftpay.ledger.dto;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

public record TransactionHistoryResponseDTO(
        UUID id,
        UUID senderId,
        UUID receiverId,
        BigDecimal amount,
        String currency,
        String status,
        OffsetDateTime createdAt) {
}
