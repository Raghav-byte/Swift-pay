package com.swiftpay.ledger.service;

import com.swiftpay.ledger.entity.AccountEntity;
import com.swiftpay.ledger.enums.TransactionStatusEnum;
import com.swiftpay.ledger.event.PaymentInitiatedEvent;
import com.swiftpay.ledger.exception.InsufficientFundsException;
import com.swiftpay.ledger.repository.AccountRepository;
import com.swiftpay.ledger.repository.TransactionRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LedgerServiceTest {

    private static final UUID TX_ID = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static final UUID LOWER_OWNER_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID HIGHER_OWNER_ID = UUID.fromString("22222222-2222-2222-2222-222222222222");

    @Mock
    private AccountRepository accountRepository;

    @Mock
    private TransactionRepository transactionRepository;

    @InjectMocks
    private LedgerService ledgerService;

    private static PaymentInitiatedEvent event(UUID senderId, UUID receiverId, BigDecimal amount) {
        return new PaymentInitiatedEvent(
                TX_ID,
                senderId,
                receiverId,
                amount,
                "USD",
                OffsetDateTime.now(ZoneOffset.UTC));
    }

    private static AccountEntity account(UUID ownerId, BigDecimal balance) {
        AccountEntity account = new AccountEntity();
        account.setOwnerId(ownerId);
        account.setBalance(balance);
        account.setCurrency("USD");
        return account;
    }

    @Test
    void transfer_happyPath_debitsSenderCreditsReceiverAndMarksCompleted() throws Exception {
        BigDecimal amount = new BigDecimal("30.0000");
        AccountEntity sender = account(HIGHER_OWNER_ID, new BigDecimal("100.0000"));
        AccountEntity receiver = account(LOWER_OWNER_ID, new BigDecimal("50.0000"));
        PaymentInitiatedEvent event = event(HIGHER_OWNER_ID, LOWER_OWNER_ID, amount);

        when(accountRepository.findByOwnerIdForUpdate(LOWER_OWNER_ID)).thenReturn(Optional.of(receiver));
        when(accountRepository.findByOwnerIdForUpdate(HIGHER_OWNER_ID)).thenReturn(Optional.of(sender));

        ledgerService.transfer(event);

        assertEquals(new BigDecimal("70.0000"), sender.getBalance());
        assertEquals(new BigDecimal("80.0000"), receiver.getBalance());
        verify(accountRepository).save(sender);
        verify(accountRepository).save(receiver);
        verify(transactionRepository).updateStatus(TX_ID, TransactionStatusEnum.COMPLETED);
        verify(transactionRepository, never()).updateStatus(TX_ID, TransactionStatusEnum.FAILED);
    }

    @Test
    void transfer_insufficientFunds_updatesFailedThenThrows() {
        BigDecimal amount = new BigDecimal("30.0000");
        AccountEntity sender = account(HIGHER_OWNER_ID, new BigDecimal("10.0000"));
        AccountEntity receiver = account(LOWER_OWNER_ID, new BigDecimal("50.0000"));
        PaymentInitiatedEvent event = event(HIGHER_OWNER_ID, LOWER_OWNER_ID, amount);

        when(accountRepository.findByOwnerIdForUpdate(LOWER_OWNER_ID)).thenReturn(Optional.of(receiver));
        when(accountRepository.findByOwnerIdForUpdate(HIGHER_OWNER_ID)).thenReturn(Optional.of(sender));

        assertThrows(InsufficientFundsException.class, () -> ledgerService.transfer(event));

        verify(transactionRepository).updateStatus(TX_ID, TransactionStatusEnum.FAILED);
        verify(accountRepository, never()).save(sender);
        verify(accountRepository, never()).save(receiver);
        verify(transactionRepository, never()).updateStatus(TX_ID, TransactionStatusEnum.COMPLETED);
    }

    @Test
    void transfer_locksAccountsInUuidSortedOrderRegardlessOfEventOrder() throws Exception {
        BigDecimal amount = new BigDecimal("10.0000");
        AccountEntity lowerAccount = account(LOWER_OWNER_ID, new BigDecimal("100.0000"));
        AccountEntity higherAccount = account(HIGHER_OWNER_ID, new BigDecimal("100.0000"));
        PaymentInitiatedEvent event = event(HIGHER_OWNER_ID, LOWER_OWNER_ID, amount);

        when(accountRepository.findByOwnerIdForUpdate(LOWER_OWNER_ID)).thenReturn(Optional.of(lowerAccount));
        when(accountRepository.findByOwnerIdForUpdate(HIGHER_OWNER_ID)).thenReturn(Optional.of(higherAccount));

        ledgerService.transfer(event);

        InOrder lockOrder = inOrder(accountRepository);
        lockOrder.verify(accountRepository).findByOwnerIdForUpdate(LOWER_OWNER_ID);
        lockOrder.verify(accountRepository).findByOwnerIdForUpdate(HIGHER_OWNER_ID);
    }
}
