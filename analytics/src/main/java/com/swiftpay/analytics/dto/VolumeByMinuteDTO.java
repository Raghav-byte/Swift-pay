package com.swiftpay.analytics.dto;

import java.math.BigDecimal;

public record VolumeByMinuteDTO(
        String minute,
        long count,
        BigDecimal totalAmount) {
}
