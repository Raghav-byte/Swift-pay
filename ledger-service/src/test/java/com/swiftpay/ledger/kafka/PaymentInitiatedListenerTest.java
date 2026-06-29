package com.swiftpay.ledger.kafka;

import com.swiftpay.ledger.event.PaymentInitiatedEvent;
import com.swiftpay.ledger.exception.InsufficientFundsException;
import com.swiftpay.ledger.exception.RetryableLedgerException;
import com.swiftpay.ledger.service.KafkaProducerService;
import com.swiftpay.ledger.service.LedgerService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PaymentInitiatedListenerTest {

    private static final PaymentInitiatedEvent EVENT = new PaymentInitiatedEvent(
            UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
            UUID.fromString("11111111-1111-1111-1111-111111111111"),
            UUID.fromString("22222222-2222-2222-2222-222222222222"),
            new BigDecimal("25.0000"),
            "USD",
            OffsetDateTime.now(ZoneOffset.UTC));

    @Mock
    private LedgerService ledgerService;

    @Mock
    private KafkaProducerService kafkaProducerService;

    @Mock
    private Acknowledgment ack;

    @InjectMocks
    private PaymentInitiatedListener listener;

    @Test
    void onPaymentInitiated_insufficientFunds_emitsFailedAcksAndSkipsCompleted() throws Exception {
        doThrow(new InsufficientFundsException()).when(ledgerService).transfer(EVENT);

        listener.onPaymentInitiated(EVENT, ack);

        verify(kafkaProducerService).emitFailed(EVENT, "INSUFFICIENT_FUNDS");
        verify(kafkaProducerService, never()).emitCompleted(EVENT);
        verify(ack).acknowledge();
    }

    @Test
    void onPaymentInitiated_retryableFailure_doesNotAckAndRethrows() throws Exception {
        RuntimeException cause = new RuntimeException("database unavailable");
        doThrow(cause).when(ledgerService).transfer(EVENT);

        RetryableLedgerException thrown = assertThrows(
                RetryableLedgerException.class, () -> listener.onPaymentInitiated(EVENT, ack));

        assertTrue(thrown.getMessage().contains("Retryable failure"));
        assertEquals(cause, thrown.getCause());
        verify(ack, never()).acknowledge();
        verifyNoInteractions(kafkaProducerService);
    }
}
