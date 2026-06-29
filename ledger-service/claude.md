# SwiftPay — Ledger Service (Service B)
## Claude Code Instructions

---

## Project Overview

**Service:** ledger-service  
**Role:** Consumes `PaymentInitiated` events from Kafka, performs atomic debit/credit transfers in PostgreSQL, emits `PaymentCompleted` or `PaymentFailed` events, and exposes a transaction history read API.  
**Java:** 21 | **Framework:** Spring Boot 3.x | **Build:** Maven  
**DB:** PostgreSQL (via Spring Data JPA + Hibernate) | **Messaging:** Kafka (consumer + producer)  
**Documentation:** Swagger / OpenAPI 3 (springdoc-openapi)  
**Base package:** `com.swiftpay.ledger` | **Context path:** `/v1`  
**Port:** `8081`

---

## Package Structure

```
com.swiftpay.ledger/
├── LedgerApplication.java          ← @SpringBootApplication entry point
├── config/
│   ├── KafkaConsumerConfig         ← ConsumerFactory, ConcurrentKafkaListenerContainerFactory
│   ├── KafkaProducerConfig         ← ProducerFactory + KafkaTemplate (for result events)
│   └── OpenApiConfig               ← Swagger/OpenAPI bean
├── controller/
│   └── TransactionController       ← GET /v1/transactions/{userId}
├── dto/
│   ├── TransactionHistoryResponseDTO  ← id, senderId, receiverId, amount, currency, status, createdAt
│   ├── TransactionPageResponseDTO     ← content (List<>), page, size, totalElements
│   └── ErrorResponseDTO               ← code, message, timestamp, path
├── entity/
│   ├── AccountEntity               ← sp_accounts — id, ownerId, balance, currency, version (@Version)
│   └── TransactionEntity           ← sp_transactions — id, senderId, receiverId, amount, currency, status, idempotencyKey
├── enums/
│   └── TransactionStatusEnum       ← PENDING, COMPLETED, FAILED
├── event/
│   ├── PaymentInitiatedEvent       ← Kafka inbound payload (matches payment-gateway's emission)
│   ├── PaymentCompletedEvent       ← Kafka outbound on success
│   └── PaymentFailedEvent          ← Kafka outbound on failure
├── exception/
│   ├── SwiftPayException           ← checked base exception
│   ├── AccountNotFoundException    ← extends SwiftPayException
│   ├── InsufficientFundsException  ← extends SwiftPayException
│   └── GlobalExceptionHandler      ← @RestControllerAdvice
├── repository/
│   ├── AccountRepository           ← JpaRepository<AccountEntity, UUID>
│   └── TransactionRepository       ← JpaRepository<TransactionEntity, UUID>
├── service/
│   ├── LedgerService               ← atomic debit/credit transfer logic
│   ├── TransactionHistoryService   ← query + paginate transaction history
│   └── KafkaProducerService        ← emits PaymentCompleted / PaymentFailed events
└── kafka/
    └── PaymentInitiatedListener    ← @KafkaListener — entry point for all processing
```

---

## Architecture — What This Service Does

```
Kafka Topic: payment-initiated
  │
  ▼
PaymentInitiatedListener.onPaymentInitiated(event)
  │
  ├─► LedgerService.transfer(event)
  │     @Transactional(rollbackFor = Exception.class)
  │     ├─ accountRepository.findByOwnerIdForUpdate(senderId)   ← SELECT FOR UPDATE (pessimistic lock)
  │     ├─ accountRepository.findByOwnerIdForUpdate(receiverId) ← SELECT FOR UPDATE
  │     ├─ if sender.balance < amount → throw InsufficientFundsException
  │     ├─ sender.balance  -= amount  → accountRepository.save(sender)
  │     ├─ receiver.balance += amount → accountRepository.save(receiver)
  │     └─ transactionRepository.updateStatus(txId, COMPLETED)
  │
  ├─► (on success) KafkaProducerService.emitCompleted(event)
  │     Topic: payment-completed
  │
  └─► (on failure) KafkaProducerService.emitFailed(event, reason)
        Topic: payment-failed

REST API:
GET /v1/transactions/{userId}?page=0&size=20
  └─► TransactionHistoryService.getHistory(userId, page, size)
        → returns paginated list (sent + received)
```

---

## Database Schema

**Same `sp_accounts` and `sp_transactions` tables as payment-gateway.**  
Both services share the same PostgreSQL database. The ledger service WRITES to these tables (balance updates, status updates). The payment-gateway READS from them (balance check) and INSERTS new transactions.

```sql
-- Accounts already created by payment-gateway schema migration
-- Transactions already created by payment-gateway schema migration

-- Index for history query (both sender + receiver)
CREATE INDEX IF NOT EXISTS idx_tx_sender_id   ON sp_transactions(sender_id);
CREATE INDEX IF NOT EXISTS idx_tx_receiver_id ON sp_transactions(receiver_id);
```

**Pessimistic locking query (REQUIRED for atomic transfers):**
```sql
SELECT * FROM sp_accounts WHERE owner_id = :ownerId FOR UPDATE
```
This must be a native query in `AccountRepository` — JPA derived queries do not support `FOR UPDATE`.

---

## Kafka Configuration

**Consumed topic:** `payment-initiated`  
**Produced topics:** `payment-completed`, `payment-failed`, `payment-failed-dlq`

**Consumer config:**
```
bootstrap.servers      = localhost:9092
group.id               = ledger-service-group
key.deserializer       = StringDeserializer
value.deserializer     = JsonDeserializer
trusted.packages       = com.swiftpay.gateway.event, com.swiftpay.ledger.event
auto.offset.reset      = earliest
enable.auto.commit     = false   ← manual ack via MANUAL_IMMEDIATE
```

**Producer config (for result events):**
```
bootstrap.servers = localhost:9092
key.serializer   = StringSerializer
value.serializer = JsonSerializer
acks             = all
retries          = 3
```

**Retry policy on consumer:**
- Max 3 retry attempts with 1s exponential backoff
- On exhaustion: route to `payment-failed-dlq` topic
- Implementation: `DefaultErrorHandler` with `FixedBackOff(1000L, 3L)` + `DeadLetterPublishingRecoverer`

---

## Event Payloads

**PaymentInitiatedEvent (inbound — must match payment-gateway output exactly):**
```java
public record PaymentInitiatedEvent(
    UUID transactionId,
    UUID senderId,
    UUID receiverId,
    BigDecimal amount,
    String currency,
    OffsetDateTime timestamp
) {}
```

**PaymentCompletedEvent (outbound):**
```java
public record PaymentCompletedEvent(
    UUID transactionId,
    UUID senderId,
    UUID receiverId,
    BigDecimal amount,
    String currency,
    OffsetDateTime completedAt
) {}
```

**PaymentFailedEvent (outbound):**
```java
public record PaymentFailedEvent(
    UUID transactionId,
    UUID senderId,
    UUID receiverId,
    BigDecimal amount,
    String currency,
    String failureReason,   // "INSUFFICIENT_FUNDS" | "ACCOUNT_NOT_FOUND" | "UNEXPECTED_ERROR"
    OffsetDateTime failedAt
) {}
```

---

## Kafka Listener Implementation Guide

```java
@Component
public class PaymentInitiatedListener {

    private static final Logger logger = LoggerFactory.getLogger(PaymentInitiatedListener.class);

    private final LedgerService ledgerService;
    private final KafkaProducerService kafkaProducerService;

    // explicit constructor

    @KafkaListener(
        topics = "payment-initiated",
        groupId = "ledger-service-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void onPaymentInitiated(PaymentInitiatedEvent event, Acknowledgment ack) {
        logger.info("Received PaymentInitiatedEvent for transactionId={}", event.transactionId());
        try {
            ledgerService.transfer(event);
            kafkaProducerService.emitCompleted(event);
            ack.acknowledge();
            logger.info("Transfer completed for transactionId={}", event.transactionId());
        } catch (InsufficientFundsException e) {
            logger.warn("Insufficient funds for transactionId={}", event.transactionId());
            kafkaProducerService.emitFailed(event, "INSUFFICIENT_FUNDS");
            ack.acknowledge();  // ack — this is a business failure, not a retryable error
        } catch (AccountNotFoundException e) {
            logger.warn("Account not found for transactionId={}", event.transactionId());
            kafkaProducerService.emitFailed(event, "ACCOUNT_NOT_FOUND");
            ack.acknowledge();
        } catch (Exception e) {
            logger.error("Unexpected error for transactionId={}", event.transactionId(), e);
            // Do NOT ack — let the retry/DLQ handler take over
            throw new RuntimeException("Retryable failure for transactionId=" + event.transactionId(), e);
        }
    }
}
```

---

## LedgerService — Atomic Transfer Implementation Guide

```java
@Service
public class LedgerService {

    private static final Logger logger = LoggerFactory.getLogger(LedgerService.class);

    private final AccountRepository accountRepository;
    private final TransactionRepository transactionRepository;

    // explicit constructor

    @Transactional(rollbackFor = Exception.class)
    public void transfer(PaymentInitiatedEvent event) throws SwiftPayException {
        logger.info("Executing transfer for transactionId={}", event.transactionId());

        // Always lock in a consistent order to prevent deadlocks
        // Lock the lower UUID first
        UUID first  = event.senderId().compareTo(event.receiverId()) < 0 ? event.senderId()  : event.receiverId();
        UUID second = event.senderId().compareTo(event.receiverId()) < 0 ? event.receiverId() : event.senderId();

        AccountEntity accountFirst  = accountRepository.findByOwnerIdForUpdate(first)
            .orElseThrow(() -> new AccountNotFoundException(first));
        AccountEntity accountSecond = accountRepository.findByOwnerIdForUpdate(second)
            .orElseThrow(() -> new AccountNotFoundException(second));

        AccountEntity sender   = event.senderId().equals(accountFirst.getOwnerId()) ? accountFirst  : accountSecond;
        AccountEntity receiver = event.senderId().equals(accountFirst.getOwnerId()) ? accountSecond : accountFirst;

        // Balance check (double-check at execution time)
        if (sender.getBalance().compareTo(event.amount()) < 0) {
            transactionRepository.updateStatus(event.transactionId(), TransactionStatusEnum.FAILED);
            throw new InsufficientFundsException();
        }

        // Atomic debit/credit
        sender.setBalance(sender.getBalance().subtract(event.amount()));
        receiver.setBalance(receiver.getBalance().add(event.amount()));

        accountRepository.save(sender);
        accountRepository.save(receiver);

        // Update transaction status
        transactionRepository.updateStatus(event.transactionId(), TransactionStatusEnum.COMPLETED);

        logger.info("Transfer successful for transactionId={}", event.transactionId());
    }
}
```

**Why UUID-ordered locking?** Two concurrent transactions A→B and B→A would deadlock if each locks their sender first. By always locking the lexicographically smaller UUID first, both threads acquire locks in the same order — no deadlock possible.

---

## AccountRepository — Pessimistic Lock Query

```java
@Repository
public interface AccountRepository extends JpaRepository<AccountEntity, UUID> {

    Optional<AccountEntity> findByOwnerId(UUID ownerId);

    // CRITICAL: FOR UPDATE acquires a row-level lock for the duration of the transaction
    @Query(value = "SELECT * FROM sp_accounts WHERE owner_id = :ownerId FOR UPDATE", nativeQuery = true)
    Optional<AccountEntity> findByOwnerIdForUpdate(@Param("ownerId") UUID ownerId);
}
```

---

## TransactionRepository

```java
@Repository
public interface TransactionRepository extends JpaRepository<TransactionEntity, UUID> {

    // Paginated history — user appears as either sender or receiver
    @Query(value = """
            SELECT * FROM sp_transactions
            WHERE sender_id = :userId OR receiver_id = :userId
            ORDER BY created_at DESC
            LIMIT :size OFFSET :offset
            """, nativeQuery = true)
    List<TransactionEntity> findByUserId(
        @Param("userId") UUID userId,
        @Param("offset") int offset,
        @Param("size") int size
    );

    @Query(value = """
            SELECT COUNT(*) FROM sp_transactions
            WHERE sender_id = :userId OR receiver_id = :userId
            """, nativeQuery = true)
    long countByUserId(@Param("userId") UUID userId);

    // Status update — used after transfer
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = "UPDATE sp_transactions SET status = :#{#status.name()}, updated_at = NOW() WHERE id = :id",
           nativeQuery = true)
    int updateStatus(@Param("id") UUID id, @Param("status") TransactionStatusEnum status);
}
```

---

## TransactionHistoryService

```java
@Service
public class TransactionHistoryService {

    private final TransactionRepository transactionRepository;

    public TransactionPageResponseDTO getHistory(UUID userId, int page, int size) throws SwiftPayException {
        try {
            int offset = page * size;
            List<TransactionEntity> results = transactionRepository.findByUserId(userId, offset, size);
            long total = transactionRepository.countByUserId(userId);

            List<TransactionHistoryResponseDTO> content = results.stream()
                .map(this::toDTO)
                .toList();

            return new TransactionPageResponseDTO(content, page, size, total);
        } catch (Exception e) {
            throw new SwiftPayException(500, "QUERY_FAILED", "Failed to retrieve transaction history", e);
        }
    }

    private TransactionHistoryResponseDTO toDTO(TransactionEntity e) {
        return new TransactionHistoryResponseDTO(
            e.getId(), e.getSenderId(), e.getReceiverId(),
            e.getAmount(), e.getCurrency(), e.getStatus().name(), e.getCreatedAt()
        );
    }
}
```

---

## REST API Contract

### GET /v1/transactions/{userId}

**Query params:**
- `page` (int, default 0)
- `size` (int, default 20, max 100)

**Response 200:**
```json
{
  "content": [
    {
      "id":         "uuid",
      "senderId":   "uuid",
      "receiverId": "uuid",
      "amount":     1500.00,
      "currency":   "INR",
      "status":     "COMPLETED",
      "createdAt":  "2025-01-01T10:00:00Z"
    }
  ],
  "page": 0,
  "size": 20,
  "totalElements": 142
}
```

**Response 404:** userId has no account  
**Response 500:** query failure

---

## KafkaConsumerConfig — Full Config

```java
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ConsumerFactory<String, PaymentInitiatedEvent> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ledger-service-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.swiftpay.gateway.event,com.swiftpay.ledger.event");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, PaymentInitiatedEvent.class.getName());
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
            new JsonDeserializer<>(PaymentInitiatedEvent.class, false));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentInitiatedEvent> kafkaListenerContainerFactory(
            ConsumerFactory<String, PaymentInitiatedEvent> cf,
            KafkaTemplate<String, Object> kafkaTemplate) {

        ConcurrentKafkaListenerContainerFactory<String, PaymentInitiatedEvent> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(cf);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // Retry: 3 attempts, 1s backoff, then DLQ
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
            (record, ex) -> new TopicPartition("payment-failed-dlq", -1));
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3L));
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}
```

---

## application.yml

```yaml
server:
  port: 8081

spring:
  config:
    import:
      - optional:file:../.env[.properties]
      - optional:file:.env[.properties]
  application:
    name: ledger-service
  datasource:
    # Supabase Transaction Pooler — port 6543, NOT 5432 (direct)
    # Dashboard → Settings → Database → Connection string → Java/JDBC
    url: ${DB_URL}
    username: ${DB_USERNAME}   # Supabase format: postgres.<project-ref>
    password: ${DB_PASSWORD}
    driver-class-name: org.postgresql.Driver
    hikari:
      maximum-pool-size: 10    # Free tier allows max 15 — stay under this
      minimum-idle: 2
      keepalive-time: 30000
      max-lifetime: 1800000
  jpa:
    hibernate:
      ddl-auto: validate       # Schema applied via Supabase SQL editor — never auto-create
    show-sql: false
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP:localhost:9092}
    consumer:
      group-id: ledger-service-group
      auto-offset-reset: earliest
      enable-auto-commit: false
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "com.swiftpay.gateway.event,com.swiftpay.ledger.event"
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all
      retries: 3

springdoc:
  api-docs:
    path: /v3/api-docs
  swagger-ui:
    path: /swagger-ui.html

logging:
  level:
    com.swiftpay.ledger: INFO
    org.springframework.kafka: WARN
```

---

## Dockerfile

```dockerfile
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -q
COPY src ./src
RUN mvn package -DskipTests -q

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
```

---

## Testing Strategy

| Test | What to test | Tool |
|---|---|---|
| `LedgerServiceTest` | Happy path transfer, insufficient funds, deadlock-safe lock order | JUnit 5 + Mockito |
| `PaymentInitiatedListenerTest` | Ack on business failure, no-ack on unexpected exception | JUnit 5 + Mockito |
| `LedgerIntegrationTest` | Full Kafka → DB → status update → result event | Testcontainers (PG + Kafka) |

**Key integration test case:**
```java
@Test
void transferShouldDebitSenderAndCreditReceiver() {
    // Setup accounts with known balances
    // Publish PaymentInitiatedEvent to Kafka
    // Wait for processing (Awaitility, max 10s)
    // Assert sender.balance decreased
    // Assert receiver.balance increased
    // Assert transaction status = COMPLETED
    // Assert PaymentCompleted emitted on payment-completed topic
}

@Test
void transferShouldFailAndEmitPaymentFailedWhenInsufficientFunds() {
    // Setup sender with balance = 0
    // Publish event with amount > 0
    // Wait for processing
    // Assert transaction status = FAILED
    // Assert PaymentFailed emitted on payment-failed topic
}
```

---

## Code Generation Rules for Cursor Agent

1. **Kafka listener NEVER calls a service that is also `@Transactional` from outside** — the `@Transactional` is on `LedgerService.transfer()`, called from the listener. This is correct.
2. **`SELECT FOR UPDATE` is mandatory** for any method that reads an account to modify it. Never read then write without the lock.
3. **Always lock accounts in UUID-sorted order** to prevent deadlocks. This is non-negotiable.
4. **Business failures (InsufficientFunds, AccountNotFound) are ACKed immediately.** They are not retryable — the condition won't change on retry. Only infrastructure failures (DB down, timeout) are retried.
5. **All monetary amounts are `BigDecimal`.** Never `double` or `float`.
6. **Entity IDs are `UUID`.** Never `Long` or `String`.
7. **All timestamps are `OffsetDateTime` with `ZoneOffset.UTC`.**
8. **`@Transactional(rollbackFor = Exception.class)`** on `LedgerService.transfer()`.
9. **Enums persisted as `@Enumerated(EnumType.STRING)`.**
10. **Constructor injection only** — never `@Autowired` on fields.
11. **Table prefix:** always `sp_` (e.g. `sp_transactions`, `sp_accounts`).
12. **`@Modifying(clearAutomatically = true, flushAutomatically = true)`** on all `@Query` update statements.