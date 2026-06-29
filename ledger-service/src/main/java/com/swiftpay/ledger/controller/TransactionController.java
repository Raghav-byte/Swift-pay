package com.swiftpay.ledger.controller;

import com.swiftpay.ledger.dto.TransactionPageResponseDTO;
import com.swiftpay.ledger.exception.SwiftPayException;
import com.swiftpay.ledger.service.TransactionHistoryService;
import io.swagger.v3.oas.annotations.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/v1/transactions")
public class TransactionController {

    private static final Logger logger = LoggerFactory.getLogger(TransactionController.class);

    private final TransactionHistoryService transactionHistoryService;

    public TransactionController(TransactionHistoryService transactionHistoryService) {
        this.transactionHistoryService = transactionHistoryService;
    }

    @Operation(summary = "Fetch paginated transaction history for a user")
    @GetMapping("/{userId}")
    public TransactionPageResponseDTO getHistory(
            @PathVariable UUID userId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) throws SwiftPayException {
        int effectiveSize = Math.min(size, 100);
        logger.info("GET transaction history: userId={}, page={}, size={}", userId, page, effectiveSize);
        return transactionHistoryService.getHistory(userId, page, effectiveSize);
    }
}
