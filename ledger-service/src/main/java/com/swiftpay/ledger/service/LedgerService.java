package com.swiftpay.ledger.service;

import com.swiftpay.ledger.entity.AccountEntity;
import com.swiftpay.ledger.enums.TransactionStatusEnum;
import com.swiftpay.ledger.event.PaymentInitiatedEvent;
import com.swiftpay.ledger.exception.AccountNotFoundException;
import com.swiftpay.ledger.exception.InsufficientFundsException;
import com.swiftpay.ledger.exception.SwiftPayException;
import com.swiftpay.ledger.repository.AccountRepository;
import com.swiftpay.ledger.repository.TransactionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class LedgerService {

    private static final Logger logger = LoggerFactory.getLogger(LedgerService.class);

    private final AccountRepository accountRepository;
    private final TransactionRepository transactionRepository;

    public LedgerService(AccountRepository accountRepository, TransactionRepository transactionRepository) {
        this.accountRepository = accountRepository;
        this.transactionRepository = transactionRepository;
    }

    @Transactional(rollbackFor = Exception.class)
    public void transfer(PaymentInitiatedEvent event) throws SwiftPayException {
        logger.info("Executing transfer for transactionId={}", event.transactionId());

        UUID first = event.senderId().compareTo(event.receiverId()) < 0 ? event.senderId() : event.receiverId();
        UUID second = event.senderId().compareTo(event.receiverId()) < 0 ? event.receiverId() : event.senderId();

        AccountEntity accountFirst = accountRepository.findByOwnerIdForUpdate(first)
                .orElseThrow(() -> new AccountNotFoundException(first));
        AccountEntity accountSecond = accountRepository.findByOwnerIdForUpdate(second)
                .orElseThrow(() -> new AccountNotFoundException(second));

        AccountEntity sender = event.senderId().equals(accountFirst.getOwnerId()) ? accountFirst : accountSecond;
        AccountEntity receiver = event.senderId().equals(accountFirst.getOwnerId()) ? accountSecond : accountFirst;

        if (sender.getBalance().compareTo(event.amount()) < 0) {
            transactionRepository.updateStatus(event.transactionId(), TransactionStatusEnum.FAILED);
            throw new InsufficientFundsException();
        }

        sender.setBalance(sender.getBalance().subtract(event.amount()));
        receiver.setBalance(receiver.getBalance().add(event.amount()));

        accountRepository.save(sender);
        accountRepository.save(receiver);

        transactionRepository.updateStatus(event.transactionId(), TransactionStatusEnum.COMPLETED);

        logger.info("Transfer successful for transactionId={}", event.transactionId());
    }
}
