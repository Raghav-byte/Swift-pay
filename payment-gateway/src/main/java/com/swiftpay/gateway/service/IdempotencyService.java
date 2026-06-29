package com.swiftpay.gateway.service;

import com.swiftpay.gateway.exception.DuplicateTransactionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.UUID;

@Service
public class IdempotencyService {

    private static final Logger logger = LoggerFactory.getLogger(IdempotencyService.class);

    private static final String PREFIX = "swiftpay:idempotency:";
    private static final long TTL_SECONDS = 86400L;

    private final StringRedisTemplate redisTemplate;

    public IdempotencyService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void checkDuplicate(UUID transactionId) throws DuplicateTransactionException {
        String key = PREFIX + transactionId;
        Boolean exists = redisTemplate.hasKey(key);
        if (Boolean.TRUE.equals(exists)) {
            logger.warn("Duplicate transaction detected: transactionId={}", transactionId);
            throw new DuplicateTransactionException(transactionId.toString());
        }
    }

    public void store(UUID transactionId) {
        String key = PREFIX + transactionId;
        redisTemplate.opsForValue().set(key, "processed", Duration.ofSeconds(TTL_SECONDS));
        logger.debug("Idempotency key stored for transactionId={}", transactionId);
    }
}
