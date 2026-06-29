package com.swiftpay.analytics.service;

import com.swiftpay.analytics.entity.AnalyticsEventEntity;
import com.swiftpay.analytics.event.PaymentCompletedEvent;
import com.swiftpay.analytics.repository.AnalyticsEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class AnalyticsIngestService {

    private static final Logger logger = LoggerFactory.getLogger(AnalyticsIngestService.class);

    private final AnalyticsEventRepository analyticsEventRepository;

    public AnalyticsIngestService(AnalyticsEventRepository analyticsEventRepository) {
        this.analyticsEventRepository = analyticsEventRepository;
    }

    @Transactional(rollbackFor = Exception.class)
    public void record(PaymentCompletedEvent event) {
        logger.info("Recording analytics for transactionId={}", event.transactionId());

        AnalyticsEventEntity entity = new AnalyticsEventEntity();
        entity.setTransactionId(event.transactionId());
        entity.setSenderId(event.senderId());
        entity.setReceiverId(event.receiverId());
        entity.setAmount(event.amount());
        entity.setCurrency(event.currency());
        entity.setCompletedAt(event.completedAt());

        analyticsEventRepository.save(entity);
        logger.info("Analytics recorded for transactionId={}", event.transactionId());
    }
}
