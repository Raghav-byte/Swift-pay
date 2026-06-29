package com.swiftpay.gateway.integration;

import com.swiftpay.gateway.entity.AccountEntity;
import com.swiftpay.gateway.entity.TransactionEntity;
import com.swiftpay.gateway.enums.TransactionStatusEnum;
import com.swiftpay.gateway.event.PaymentInitiatedEvent;
import com.swiftpay.gateway.exception.InsufficientFundsException;
import com.swiftpay.gateway.exception.SwiftPayException;
import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class IntegrationLedgerService {

    private static final Logger logger = LoggerFactory.getLogger(IntegrationLedgerService.class);

    private final EntityManager entityManager;

    public IntegrationLedgerService(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    @Transactional(rollbackFor = Exception.class)
    public void transfer(PaymentInitiatedEvent event) throws SwiftPayException {
        logger.info("Executing transfer for transactionId={}", event.transactionId());

        UUID first = event.senderId().compareTo(event.receiverId()) < 0 ? event.senderId() : event.receiverId();
        UUID second = event.senderId().compareTo(event.receiverId()) < 0 ? event.receiverId() : event.senderId();

        AccountEntity accountFirst = lockByOwnerId(first);
        AccountEntity accountSecond = lockByOwnerId(second);

        AccountEntity sender = event.senderId().equals(accountFirst.getOwnerId()) ? accountFirst : accountSecond;
        AccountEntity receiver = event.senderId().equals(accountFirst.getOwnerId()) ? accountSecond : accountFirst;

        if (sender.getBalance().compareTo(event.amount()) < 0) {
            updateTransactionStatus(event.transactionId(), TransactionStatusEnum.FAILED);
            throw new InsufficientFundsException();
        }

        sender.setBalance(sender.getBalance().subtract(event.amount()));
        receiver.setBalance(receiver.getBalance().add(event.amount()));
        updateTransactionStatus(event.transactionId(), TransactionStatusEnum.COMPLETED);

        logger.info("Transfer successful for transactionId={}", event.transactionId());
    }

    private AccountEntity lockByOwnerId(UUID ownerId) throws SwiftPayException {
        try {
            return entityManager.createQuery(
                            "SELECT a FROM AccountEntity a WHERE a.ownerId = :ownerId", AccountEntity.class)
                    .setParameter("ownerId", ownerId)
                    .setLockMode(LockModeType.PESSIMISTIC_WRITE)
                    .getSingleResult();
        } catch (Exception e) {
            throw new SwiftPayException(404, "ACCOUNT_NOT_FOUND", "Account not found for ownerId=" + ownerId);
        }
    }

    private void updateTransactionStatus(UUID transactionId, TransactionStatusEnum status) {
        TransactionEntity transaction = entityManager.find(TransactionEntity.class, transactionId);
        if (transaction != null) {
            transaction.setStatus(status);
        }
    }
}
