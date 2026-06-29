package com.swiftpay.gateway.service;

import com.swiftpay.gateway.dto.PaymentRequestDTO;
import com.swiftpay.gateway.dto.PaymentResponseDTO;
import com.swiftpay.gateway.entity.TransactionEntity;
import com.swiftpay.gateway.enums.TransactionStatusEnum;
import com.swiftpay.gateway.exception.SwiftPayException;
import com.swiftpay.gateway.exception.UnsupportedCurrencyException;
import com.swiftpay.gateway.repository.TransactionRepository;
import com.swiftpay.gateway.utils.ResponseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Service
public class PaymentService {

    static final String PAYMENT_CURRENCY = "INR";

    private static final Logger logger = LoggerFactory.getLogger(PaymentService.class);

    private final IdempotencyService idempotencyService;
    private final BalanceService balanceService;
    private final TransactionRepository transactionRepository;
    private final KafkaProducerService kafkaProducerService;

    public PaymentService(
            IdempotencyService idempotencyService,
            BalanceService balanceService,
            TransactionRepository transactionRepository,
            KafkaProducerService kafkaProducerService) {
        this.idempotencyService = idempotencyService;
        this.balanceService = balanceService;
        this.transactionRepository = transactionRepository;
        this.kafkaProducerService = kafkaProducerService;
    }

    @Transactional(rollbackFor = Exception.class)
    public PaymentResponseDTO initiatePayment(PaymentRequestDTO dto) throws SwiftPayException {
        logger.info("Initiating payment for transactionId: {}", dto.getTransactionId());

        validateCurrency(dto.getCurrency());

        idempotencyService.checkDuplicate(dto.getTransactionId());

        balanceService.validate(dto.getSenderId(), dto.getAmount());

        TransactionEntity tx = new TransactionEntity();
        tx.setSenderId(dto.getSenderId());
        tx.setReceiverId(dto.getReceiverId());
        tx.setAmount(dto.getAmount());
        tx.setCurrency(dto.getCurrency());
        tx.setStatus(TransactionStatusEnum.PENDING);
        tx.setIdempotencyKey(dto.getTransactionId().toString());
        TransactionEntity saved = transactionRepository.save(tx);

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                kafkaProducerService.emitPaymentInitiated(saved);
                idempotencyService.store(dto.getTransactionId());
            }
        });

        return ResponseUtil.paymentAccepted(saved);
    }

    private void validateCurrency(String currency) throws UnsupportedCurrencyException {
        if (currency == null || !PAYMENT_CURRENCY.equals(currency.trim())) {
            throw new UnsupportedCurrencyException(currency);
        }
    }
}
