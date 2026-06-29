package com.swiftpay.analytics.dto;

import java.math.BigDecimal;

public record AnalyticsSummaryDTO(
        long totalTransactions,
        BigDecimal totalVolume,
        String currency) {
}
