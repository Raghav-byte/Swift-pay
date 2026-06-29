package com.swiftpay.ledger.service;

import com.swiftpay.ledger.event.PaymentCompletedEvent;
import com.swiftpay.ledger.event.PaymentFailedEvent;
import com.swiftpay.ledger.event.PaymentInitiatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

@Service
public class KafkaProducerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);

    private static final String TOPIC_COMPLETED = "payment-completed";
    private static final String TOPIC_FAILED = "payment-failed";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void emitCompleted(PaymentInitiatedEvent event) {
        PaymentCompletedEvent completed = new PaymentCompletedEvent(
                event.transactionId(),
                event.senderId(),
                event.receiverId(),
                event.amount(),
                event.currency(),
                OffsetDateTime.now(ZoneOffset.UTC));
        kafkaTemplate.send(TOPIC_COMPLETED, event.transactionId().toString(), completed);
        logger.info("Emitted PaymentCompletedEvent for transactionId={}", event.transactionId());
    }

    public void emitFailed(PaymentInitiatedEvent event, String failureReason) {
        PaymentFailedEvent failed = new PaymentFailedEvent(
                event.transactionId(),
                event.senderId(),
                event.receiverId(),
                event.amount(),
                event.currency(),
                failureReason,
                OffsetDateTime.now(ZoneOffset.UTC));
        kafkaTemplate.send(TOPIC_FAILED, event.transactionId().toString(), failed);
        logger.info("Emitted PaymentFailedEvent for transactionId={}, reason={}", event.transactionId(), failureReason);
    }
}
