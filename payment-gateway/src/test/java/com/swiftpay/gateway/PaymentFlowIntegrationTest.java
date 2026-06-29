package com.swiftpay.gateway;

import com.swiftpay.gateway.dto.PaymentRequestDTO;
import com.swiftpay.gateway.dto.PaymentResponseDTO;
import com.swiftpay.gateway.entity.AccountEntity;
import com.swiftpay.gateway.enums.TransactionStatusEnum;
import com.swiftpay.gateway.repository.AccountRepository;
import com.swiftpay.gateway.repository.TransactionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class PaymentFlowIntegrationTest extends BaseIntegrationTest {

    private static final UUID SENDER_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID RECEIVER_ID = UUID.fromString("22222222-2222-2222-2222-222222222222");
    private static final UUID LOW_BALANCE_SENDER_ID = UUID.fromString("33333333-3333-3333-3333-333333333333");

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @BeforeEach
    void setUp() {
        transactionRepository.deleteAll();
        accountRepository.deleteAll();
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();

        saveAccount(SENDER_ID, new BigDecimal("10000.00"));
        saveAccount(RECEIVER_ID, new BigDecimal("500.00"));
        saveAccount(LOW_BALANCE_SENDER_ID, new BigDecimal("50.00"));
    }

    @Test
    void initiatePayment_completesTransferAndUpdatesBalances() {
        UUID clientTransactionId = UUID.randomUUID();
        BigDecimal amount = new BigDecimal("1500.00");

        PaymentRequestDTO request = new PaymentRequestDTO(
                clientTransactionId, SENDER_ID, RECEIVER_ID, amount, "INR");

        ResponseEntity<PaymentResponseDTO> response = restTemplate.postForEntity(
                "/v1/payments", request, PaymentResponseDTO.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getStatus()).isEqualTo(TransactionStatusEnum.PENDING.name());

        UUID transactionId = response.getBody().getTransactionId();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            TransactionStatusEnum status = transactionRepository.findById(transactionId)
                    .orElseThrow()
                    .getStatus();
            assertThat(status).isEqualTo(TransactionStatusEnum.COMPLETED);
        });

        AccountEntity sender = accountRepository.findByOwnerId(SENDER_ID).orElseThrow();
        AccountEntity receiver = accountRepository.findByOwnerId(RECEIVER_ID).orElseThrow();

        assertThat(sender.getBalance()).isEqualByComparingTo(new BigDecimal("8500.00"));
        assertThat(receiver.getBalance()).isEqualByComparingTo(new BigDecimal("2000.00"));
    }

    @Test
    void initiatePayment_rejectsInsufficientFunds() {
        UUID clientTransactionId = UUID.randomUUID();
        BigDecimal amount = new BigDecimal("100.00");

        PaymentRequestDTO request = new PaymentRequestDTO(
                clientTransactionId, LOW_BALANCE_SENDER_ID, RECEIVER_ID, amount, "INR");

        ResponseEntity<String> response = restTemplate.postForEntity(
                "/v1/payments", request, String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(response.getBody()).contains("INSUFFICIENT_FUNDS");
        assertThat(transactionRepository.count()).isZero();
    }

    private void saveAccount(UUID ownerId, BigDecimal balance) {
        AccountEntity account = new AccountEntity();
        account.setOwnerId(ownerId);
        account.setBalance(balance);
        account.setCurrency("INR");
        accountRepository.save(account);
    }
}
