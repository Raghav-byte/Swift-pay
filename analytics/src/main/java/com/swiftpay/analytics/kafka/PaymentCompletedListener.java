package com.swiftpay.analytics.kafka;

import com.swiftpay.analytics.event.PaymentCompletedEvent;
import com.swiftpay.analytics.service.AnalyticsIngestService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class PaymentCompletedListener {

    private static final Logger logger = LoggerFactory.getLogger(PaymentCompletedListener.class);

    private final AnalyticsIngestService analyticsIngestService;

    public PaymentCompletedListener(AnalyticsIngestService analyticsIngestService) {
        this.analyticsIngestService = analyticsIngestService;
    }

    @KafkaListener(
            topics = "payment-completed",
            groupId = "analytics-worker-group",
            containerFactory = "analyticsKafkaListenerContainerFactory")
    public void onPaymentCompleted(PaymentCompletedEvent event, Acknowledgment ack) {
        logger.info("Analytics received PaymentCompleted: transactionId={} amount={} currency={}",
                event.transactionId(), event.amount(), event.currency());
        try {
            analyticsIngestService.record(event);
            ack.acknowledge();
        } catch (Exception e) {
            logger.error("Analytics ingest failed for transactionId={}", event.transactionId(), e);
            throw new RuntimeException("Analytics ingest failed", e);
        }
    }
}
