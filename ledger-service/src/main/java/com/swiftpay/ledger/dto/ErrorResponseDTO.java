package com.swiftpay.ledger.dto;

import java.time.OffsetDateTime;

public record ErrorResponseDTO(
        String code,
        String message,
        OffsetDateTime timestamp,
        String path) {
}
