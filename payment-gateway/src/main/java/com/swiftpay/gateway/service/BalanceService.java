package com.swiftpay.gateway.service;

import com.swiftpay.gateway.entity.AccountEntity;
import com.swiftpay.gateway.exception.InsufficientFundsException;
import com.swiftpay.gateway.exception.SwiftPayException;
import com.swiftpay.gateway.repository.AccountRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;

@Service
public class BalanceService {

    private static final Logger logger = LoggerFactory.getLogger(BalanceService.class);

    private static final String PREFIX = "swiftpay:balance:";
    private static final long CACHE_TTL = 30L;

    private final StringRedisTemplate redisTemplate;
    private final AccountRepository accountRepository;

    public BalanceService(StringRedisTemplate redisTemplate, AccountRepository accountRepository) {
        this.redisTemplate = redisTemplate;
        this.accountRepository = accountRepository;
    }

    public void validate(UUID senderId, BigDecimal amount) throws SwiftPayException {
        BigDecimal balance = getCachedBalance(senderId);
        if (balance.compareTo(amount) < 0) {
            logger.warn("Insufficient funds for senderId={}: balance={}, required={}", senderId, balance, amount);
            throw new InsufficientFundsException();
        }
        logger.debug("Balance validation passed for senderId={}, amount={}", senderId, amount);
    }

    private BigDecimal getCachedBalance(UUID senderId) throws SwiftPayException {
        String key = PREFIX + senderId;
        String cached = redisTemplate.opsForValue().get(key);
        if (cached != null) {
            return new BigDecimal(cached);
        }

        AccountEntity account = accountRepository.findByOwnerId(senderId)
                .orElseThrow(() -> new SwiftPayException(404, "ACCOUNT_NOT_FOUND", "Account not found"));

        redisTemplate.opsForValue().set(key, account.getBalance().toPlainString(), Duration.ofSeconds(CACHE_TTL));
        return account.getBalance();
    }

    public void invalidateCache(UUID ownerId) {
        redisTemplate.delete(PREFIX + ownerId);
    }
}
