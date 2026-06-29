package com.swiftpay.ledger.service;

import com.swiftpay.ledger.dto.TransactionHistoryResponseDTO;
import com.swiftpay.ledger.dto.TransactionPageResponseDTO;
import com.swiftpay.ledger.entity.TransactionEntity;
import com.swiftpay.ledger.exception.SwiftPayException;
import com.swiftpay.ledger.repository.AccountRepository;
import com.swiftpay.ledger.repository.TransactionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

@Service
public class TransactionHistoryService {

    private static final Logger logger = LoggerFactory.getLogger(TransactionHistoryService.class);

    private final TransactionRepository transactionRepository;
    private final AccountRepository accountRepository;

    public TransactionHistoryService(
            TransactionRepository transactionRepository, AccountRepository accountRepository) {
        this.transactionRepository = transactionRepository;
        this.accountRepository = accountRepository;
    }

    public TransactionPageResponseDTO getHistory(UUID userId, int page, int size) throws SwiftPayException {
        logger.info("Fetching transaction history for userId={}, page={}, size={}", userId, page, size);
        try {
            if (accountRepository.findByOwnerId(userId).isEmpty()) {
                throw new SwiftPayException(404, "ACCOUNT_NOT_FOUND", "No account found for user: " + userId);
            }

            int offset = page * size;
            List<TransactionEntity> results = transactionRepository.findByUserId(userId, offset, size);
            long total = transactionRepository.countByUserId(userId);

            List<TransactionHistoryResponseDTO> content = results.stream()
                    .map(this::toDTO)
                    .toList();

            logger.info("Returned {} transactions for userId={} (total={})", content.size(), userId, total);
            return new TransactionPageResponseDTO(content, page, size, total);
        } catch (SwiftPayException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Failed to retrieve transaction history for userId={}", userId, e);
            throw new SwiftPayException(500, "QUERY_FAILED", "Failed to retrieve transaction history", e);
        }
    }

    private TransactionHistoryResponseDTO toDTO(TransactionEntity entity) {
        return new TransactionHistoryResponseDTO(
                entity.getId(),
                entity.getSenderId(),
                entity.getReceiverId(),
                entity.getAmount(),
                entity.getCurrency(),
                entity.getStatus().name(),
                entity.getDateCreated());
    }
}
