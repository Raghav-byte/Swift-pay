package com.swiftpay.gateway.service;

import com.swiftpay.gateway.entity.TransactionEntity;
import com.swiftpay.gateway.event.PaymentInitiatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

@Service
public class KafkaProducerService {

    private static final String TOPIC = "payment-initiated";
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);

    private final KafkaTemplate<String, PaymentInitiatedEvent> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, PaymentInitiatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void emitPaymentInitiated(TransactionEntity tx) {
        PaymentInitiatedEvent event = new PaymentInitiatedEvent(
                tx.getId(),
                tx.getSenderId(),
                tx.getReceiverId(),
                tx.getAmount(),
                tx.getCurrency(),
                OffsetDateTime.now(ZoneOffset.UTC));
        kafkaTemplate.send(TOPIC, tx.getId().toString(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        logger.error("Kafka emit failed for txId={}", tx.getId(), ex);
                    } else {
                        logger.info("Emitted to Kafka: txId={} offset={}", tx.getId(),
                                result.getRecordMetadata().offset());
                    }
                });
    }
}
