package com.swiftpay.ledger.kafka;

import com.swiftpay.ledger.event.PaymentInitiatedEvent;
import com.swiftpay.ledger.exception.AccountNotFoundException;
import com.swiftpay.ledger.exception.InsufficientFundsException;
import com.swiftpay.ledger.exception.RetryableLedgerException;
import com.swiftpay.ledger.service.KafkaProducerService;
import com.swiftpay.ledger.service.LedgerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class PaymentInitiatedListener {

    private static final Logger logger = LoggerFactory.getLogger(PaymentInitiatedListener.class);

    private final LedgerService ledgerService;
    private final KafkaProducerService kafkaProducerService;

    public PaymentInitiatedListener(LedgerService ledgerService, KafkaProducerService kafkaProducerService) {
        this.ledgerService = ledgerService;
        this.kafkaProducerService = kafkaProducerService;
    }

    @KafkaListener(
            topics = "payment-initiated",
            groupId = "ledger-service-group",
            containerFactory = "kafkaListenerContainerFactory")
    public void onPaymentInitiated(PaymentInitiatedEvent event, Acknowledgment ack) {
        logger.info("Received PaymentInitiatedEvent for transactionId={}", event.transactionId());
        try {
            ledgerService.transfer(event);
            kafkaProducerService.emitCompleted(event);
            ack.acknowledge();
            logger.info("Transfer completed for transactionId={}", event.transactionId());
        } catch (InsufficientFundsException e) {
            logger.warn("Insufficient funds for transactionId={}", event.transactionId());
            kafkaProducerService.emitFailed(event, "INSUFFICIENT_FUNDS");
            ack.acknowledge();
        } catch (AccountNotFoundException e) {
            logger.warn("Account not found for transactionId={}", event.transactionId());
            kafkaProducerService.emitFailed(event, "ACCOUNT_NOT_FOUND");
            ack.acknowledge();
        } catch (Exception e) {
            logger.error("Unexpected error for transactionId={}", event.transactionId(), e);
            throw new RetryableLedgerException("Retryable failure for transactionId=" + event.transactionId(), e);
        }
    }
}
