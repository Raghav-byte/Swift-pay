package com.swiftpay.gateway.service;

import com.swiftpay.gateway.exception.DuplicateTransactionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IdempotencyServiceTest {

    private static final String PREFIX = "swiftpay:idempotency:";

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @InjectMocks
    private IdempotencyService idempotencyService;

    @Test
    void checkDuplicate_firstCallDoesNotThrow_storeSetsKeyWithTtl() {
        UUID transactionId = UUID.randomUUID();
        String key = PREFIX + transactionId;

        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(redisTemplate.hasKey(key)).thenReturn(false);

        assertDoesNotThrow(() -> idempotencyService.checkDuplicate(transactionId));
        verify(redisTemplate).hasKey(key);

        idempotencyService.store(transactionId);

        verify(valueOperations).set(key, "processed", Duration.ofSeconds(86400L));
    }

    @Test
    void checkDuplicate_secondCallThrowsDuplicateTransactionException() {
        UUID transactionId = UUID.randomUUID();
        String key = PREFIX + transactionId;

        when(redisTemplate.hasKey(key)).thenReturn(true);

        assertThrows(DuplicateTransactionException.class, () -> idempotencyService.checkDuplicate(transactionId));
        verify(redisTemplate).hasKey(key);
    }
}
