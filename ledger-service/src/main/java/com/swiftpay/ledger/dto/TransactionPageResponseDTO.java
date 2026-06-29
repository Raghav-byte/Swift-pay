package com.swiftpay.ledger.dto;

import java.util.List;

public record TransactionPageResponseDTO(
        List<TransactionHistoryResponseDTO> content,
        int page,
        int size,
        long totalElements) {
}
