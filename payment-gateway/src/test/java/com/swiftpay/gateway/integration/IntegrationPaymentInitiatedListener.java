package com.swiftpay.gateway.integration;

import com.swiftpay.gateway.event.PaymentInitiatedEvent;
import com.swiftpay.gateway.exception.InsufficientFundsException;
import com.swiftpay.gateway.exception.SwiftPayException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class IntegrationPaymentInitiatedListener {

    private static final Logger logger = LoggerFactory.getLogger(IntegrationPaymentInitiatedListener.class);

    private final IntegrationLedgerService ledgerService;

    public IntegrationPaymentInitiatedListener(IntegrationLedgerService ledgerService) {
        this.ledgerService = ledgerService;
    }

    @KafkaListener(
            topics = "payment-initiated",
            groupId = "integration-test-ledger-group",
            containerFactory = "integrationKafkaListenerContainerFactory")
    public void onPaymentInitiated(PaymentInitiatedEvent event, Acknowledgment ack) {
        logger.info("Received PaymentInitiatedEvent for transactionId={}", event.transactionId());
        try {
            ledgerService.transfer(event);
            ack.acknowledge();
            logger.info("Transfer completed for transactionId={}", event.transactionId());
        } catch (InsufficientFundsException e) {
            logger.warn("Insufficient funds for transactionId={}", event.transactionId());
            ack.acknowledge();
        } catch (SwiftPayException e) {
            logger.warn("Ledger failure for transactionId={}: {}", event.transactionId(), e.getMessage());
            ack.acknowledge();
        } catch (Exception e) {
            logger.error("Unexpected error for transactionId={}", event.transactionId(), e);
            throw new RuntimeException("Retryable failure for transactionId=" + event.transactionId(), e);
        }
    }
}
