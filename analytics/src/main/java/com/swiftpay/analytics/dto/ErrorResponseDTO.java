package com.swiftpay.analytics.dto;

import java.time.OffsetDateTime;

public record ErrorResponseDTO(
        String code,
        String message,
        OffsetDateTime timestamp,
        String path) {
}
