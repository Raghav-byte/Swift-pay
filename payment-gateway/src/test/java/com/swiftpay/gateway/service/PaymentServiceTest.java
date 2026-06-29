package com.swiftpay.gateway.service;

import com.swiftpay.gateway.dto.PaymentRequestDTO;
import com.swiftpay.gateway.dto.PaymentResponseDTO;
import com.swiftpay.gateway.entity.TransactionEntity;
import com.swiftpay.gateway.enums.TransactionStatusEnum;
import com.swiftpay.gateway.exception.DuplicateTransactionException;
import com.swiftpay.gateway.exception.InsufficientFundsException;
import com.swiftpay.gateway.exception.UnsupportedCurrencyException;
import com.swiftpay.gateway.repository.TransactionRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {

    @Mock
    private IdempotencyService idempotencyService;

    @Mock
    private BalanceService balanceService;

    @Mock
    private TransactionRepository transactionRepository;

    @Mock
    private KafkaProducerService kafkaProducerService;

    @InjectMocks
    private PaymentService paymentService;

    @BeforeEach
    void activateTransactionSynchronization() {
        TransactionSynchronizationManager.initSynchronization();
        TransactionSynchronizationManager.setActualTransactionActive(true);
    }

    @AfterEach
    void clearTransactionSynchronization() {
        TransactionSynchronizationManager.clear();
    }

    @Test
    void initiatePayment_happyPath() throws Exception {
        UUID transactionId = UUID.randomUUID();
        UUID senderId = UUID.randomUUID();
        UUID receiverId = UUID.randomUUID();
        BigDecimal amount = new BigDecimal("100.00");
        PaymentRequestDTO request = new PaymentRequestDTO(
                transactionId, senderId, receiverId, amount, "INR");

        UUID savedId = UUID.randomUUID();
        when(transactionRepository.save(any(TransactionEntity.class))).thenAnswer(invocation -> {
            TransactionEntity tx = invocation.getArgument(0);
            tx.setId(savedId);
            return tx;
        });

        PaymentResponseDTO response = paymentService.initiatePayment(request);
        triggerAfterCommitCallbacks();

        verify(idempotencyService).checkDuplicate(transactionId);
        verify(balanceService).validate(senderId, amount);

        ArgumentCaptor<TransactionEntity> txCaptor = ArgumentCaptor.forClass(TransactionEntity.class);
        verify(transactionRepository).save(txCaptor.capture());
        TransactionEntity captured = txCaptor.getValue();
        assertEquals(senderId, captured.getSenderId());
        assertEquals(receiverId, captured.getReceiverId());
        assertEquals(amount, captured.getAmount());
        assertEquals(PaymentService.PAYMENT_CURRENCY, captured.getCurrency());
        assertEquals(TransactionStatusEnum.PENDING, captured.getStatus());
        assertEquals(transactionId.toString(), captured.getIdempotencyKey());

        ArgumentCaptor<TransactionEntity> kafkaCaptor = ArgumentCaptor.forClass(TransactionEntity.class);
        verify(kafkaProducerService).emitPaymentInitiated(kafkaCaptor.capture());
        assertEquals(savedId, kafkaCaptor.getValue().getId());
        assertEquals(TransactionStatusEnum.PENDING, kafkaCaptor.getValue().getStatus());

        verify(idempotencyService).store(transactionId);

        assertEquals(savedId, response.getTransactionId());
        assertEquals(TransactionStatusEnum.PENDING.name(), response.getStatus());
        assertEquals("Payment accepted", response.getMessage());
        assertNotNull(response.getTimestamp());
    }

    @Test
    void initiatePayment_duplicateTransactionId_doesNotSaveOrEmitOrStore() throws Exception {
        UUID transactionId = UUID.randomUUID();
        PaymentRequestDTO request = new PaymentRequestDTO(
                transactionId,
                UUID.randomUUID(),
                UUID.randomUUID(),
                new BigDecimal("50.00"),
                "INR");

        doThrow(new DuplicateTransactionException(transactionId.toString()))
                .when(idempotencyService).checkDuplicate(transactionId);

        assertThrows(DuplicateTransactionException.class, () -> paymentService.initiatePayment(request));

        verify(balanceService, never()).validate(any(), any());
        verify(transactionRepository, never()).save(any());
        verify(kafkaProducerService, never()).emitPaymentInitiated(any());
        verify(idempotencyService, never()).store(any());
    }

    @Test
    void initiatePayment_insufficientFunds_doesNotSaveOrEmitOrStore() throws Exception {
        UUID transactionId = UUID.randomUUID();
        UUID senderId = UUID.randomUUID();
        BigDecimal amount = new BigDecimal("999.99");
        PaymentRequestDTO request = new PaymentRequestDTO(
                transactionId,
                senderId,
                UUID.randomUUID(),
                amount,
                "INR");

        doThrow(new InsufficientFundsException()).when(balanceService).validate(senderId, amount);

        assertThrows(InsufficientFundsException.class, () -> paymentService.initiatePayment(request));

        verify(idempotencyService).checkDuplicate(transactionId);
        verify(transactionRepository, never()).save(any());
        verify(kafkaProducerService, never()).emitPaymentInitiated(any());
        verify(idempotencyService, never()).store(any());
    }

    @Test
    void initiatePayment_unsupportedCurrency_beforeIdempotency() throws Exception {
        PaymentRequestDTO request = new PaymentRequestDTO(
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                new BigDecimal("10.00"),
                "USD");

        assertThrows(UnsupportedCurrencyException.class, () -> paymentService.initiatePayment(request));

        verify(idempotencyService, never()).checkDuplicate(any());
        verify(balanceService, never()).validate(any(), any());
        verify(transactionRepository, never()).save(any());
    }

    private void triggerAfterCommitCallbacks() {
        for (TransactionSynchronization sync : TransactionSynchronizationManager.getSynchronizations()) {
            sync.afterCommit();
        }
    }
}
