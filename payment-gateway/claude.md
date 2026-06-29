# SwiftPay — Payment Gateway Service (Service A)
## Claude Code Instructions

---

## Project Overview

**Service:** payment-gateway  
**Role:** Accepts inbound P2P payment requests, enforces idempotency, validates sender balance, persists the transaction as PENDING, and emits a `PaymentInitiated` event to Kafka.  
**Java:** 21 | **Framework:** Spring Boot 3.x | **Build:** Maven  
**DB:** PostgreSQL (via Spring Data JPA + Hibernate) | **Cache:** Redis (Lettuce) | **Messaging:** Kafka  
**Documentation:** Swagger / OpenAPI 3 (springdoc-openapi)  
**Base package:** `com.swiftpay.gateway` | **Context path:** `/v1`  
**Port:** `8080`

---

## Package Structure

```
com.swiftpay.gateway/
├── PaymentGatewayApplication.java  ← @SpringBootApplication entry point
├── config/
│   ├── KafkaProducerConfig         ← Kafka producer factory + KafkaTemplate bean
│   ├── RedisConfig                 ← LettuceConnectionFactory + RedisTemplate<String,String>
│   ├── OpenApiConfig               ← Swagger/OpenAPI bean, bearer auth scheme
│   └── DataSourceConfig            ← (optional) HikariCP pool tuning
├── controller/
│   └── PaymentController           ← POST /v1/payments, GET /v1/health
├── dto/
│   ├── PaymentRequestDTO           ← transaction_id, sender_id, receiver_id, amount (INR set server-side)
│   ├── PaymentResponseDTO          ← transaction_id, status, message, timestamp
│   ├── HealthResponseDTO           ← status, db, kafka, redis
│   └── ErrorResponseDTO            ← code, message, timestamp, path
├── entity/
│   └── TransactionEntity           ← maps to sp_transactions table
├── enums/
│   └── TransactionStatusEnum       ← PENDING, COMPLETED, FAILED
├── event/
│   └── PaymentInitiatedEvent       ← Kafka message payload (record/POJO)
├── exception/
│   ├── SwiftPayException           ← checked base exception
│   ├── InsufficientFundsException  ← extends SwiftPayException
│   ├── DuplicateTransactionException ← extends SwiftPayException
│   └── GlobalExceptionHandler      ← @RestControllerAdvice
├── repository/
│   ├── TransactionRepository       ← JpaRepository<TransactionEntity, UUID>
│   └── AccountRepository           ← JpaRepository<AccountEntity, UUID> — for balance check
├── entity/
│   ├── TransactionEntity
│   └── AccountEntity               ← id, ownerId, balance, currency, version (@Version)
├── service/
│   ├── PaymentService              ← orchestrates the full payment flow
│   ├── IdempotencyService          ← Redis SET NX EX logic
│   ├── BalanceService              ← reads cached or DB balance
│   └── KafkaProducerService        ← wraps KafkaTemplate.send()
└── utils/
    └── ResponseUtil                ← static factory for standard response shapes
```

---

## Architecture — What This Service Does

```
Client
  │
  ▼
POST /v1/payments
  │
  ├─► IdempotencyService.check(transactionId)
  │     Redis: GET swiftpay:idempotency:{txId}
  │     If key exists → throw DuplicateTransactionException (409)
  │
  ├─► BalanceService.validate(senderId, amount)
  │     Redis GET swiftpay:balance:{senderId}
  │     Cache miss → AccountRepository.findById(senderId)
  │     If balance < amount → throw InsufficientFundsException (422)
  │
  ├─► TransactionRepository.save(PENDING transaction)
  │
  ├─► KafkaProducerService.emit(PaymentInitiatedEvent)
  │     Topic: payment-initiated
  │
  └─► IdempotencyService.store(transactionId, 24h TTL)
        Redis: SET swiftpay:idempotency:{txId} "processed" EX 86400 NX

Response: 202 Accepted + { transaction_id, status: PENDING }
```

---

## Database Schema

**Table: `sp_accounts`**
```sql
CREATE TABLE sp_accounts (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_id        UUID NOT NULL UNIQUE,
    balance         NUMERIC(19,4) NOT NULL DEFAULT 0,
    currency        VARCHAR(10) NOT NULL DEFAULT 'INR',
    version         INTEGER NOT NULL DEFAULT 0,
    date_created    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    date_modified   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_accounts_owner_id ON sp_accounts(owner_id);
```

**Table: `sp_transactions`**
```sql
CREATE TABLE sp_transactions (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sender_id           UUID NOT NULL,
    receiver_id         UUID NOT NULL,
    amount              NUMERIC(19,4) NOT NULL,
    currency            VARCHAR(10) NOT NULL DEFAULT 'INR',
    status              VARCHAR(30) NOT NULL DEFAULT 'PENDING',
    idempotency_key     VARCHAR(255) NOT NULL UNIQUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_tx_sender_id    ON sp_transactions(sender_id);
CREATE INDEX idx_tx_receiver_id  ON sp_transactions(receiver_id);
CREATE UNIQUE INDEX idx_tx_idempotency ON sp_transactions(idempotency_key);
```

---

## Kafka Configuration

**Topic:** `payment-initiated`  
**Producer config:**
```
bootstrap.servers = localhost:9092
key.serializer   = StringSerializer
value.serializer = JsonSerializer
acks             = all
retries          = 3
enable.idempotence = true
```

**PaymentInitiatedEvent payload (JSON on Kafka):**
```json
{
  "transactionId": "uuid",
  "senderId":      "uuid",
  "receiverId":    "uuid",
  "amount":        1500.00,
  "currency":      "INR",
  "timestamp":     "2025-01-01T10:00:00Z"
}
```

---

## Redis Key Conventions

| Key pattern | TTL | Value | Purpose |
|---|---|---|---|
| `swiftpay:idempotency:{transactionId}` | 86400s (24h) | `"processed"` | Deduplicate requests |
| `swiftpay:balance:{ownerId}` | 30s | balance as string | Cache balance reads |

**Idempotency SET command:** `SET swiftpay:idempotency:{txId} processed EX 86400 NX`  
If the return value is `null` (NX failed), the key already existed → duplicate → return 409.

---

## REST API Contract

### POST /v1/payments
**Request:**
```json
{
  "transactionId": "client-generated UUID — used as idempotency key",
  "senderId":      "uuid",
  "receiverId":    "uuid",
  "amount":        1500.00
}
```
All payments are stored and emitted as **INR** (`PaymentService.PAYMENT_CURRENCY`).

**Responses:**

| Status | Condition |
|---|---|
| 202 Accepted | Transaction accepted and queued |
| 400 Bad Request | Validation failure (null fields, amount ≤ 0) |
| 409 Conflict | Duplicate transactionId within 24h |
| 422 Unprocessable | Insufficient funds |
| 500 Internal | Unexpected failure |

**202 body:**
```json
{ "transactionId": "uuid", "status": "PENDING", "message": "Payment accepted", "timestamp": "..." }
```

### GET /v1/health
**Response 200:**
```json
{ "status": "UP", "db": "UP", "redis": "UP", "kafka": "UP" }
```
Each dependency checked live. If any is DOWN, return 503.

---

## Entity Pattern

```java
@Entity
@Table(name = "sp_transactions")
@Getter @Setter @NoArgsConstructor @AllArgsConstructor
public class TransactionEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "sender_id", nullable = false)
    private UUID senderId;

    @Column(name = "receiver_id", nullable = false)
    private UUID receiverId;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, length = 10)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 30)
    private TransactionStatusEnum status;

    @Column(name = "idempotency_key", nullable = false, unique = true)
    private String idempotencyKey;

    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at")
    private OffsetDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = updatedAt = OffsetDateTime.now(ZoneOffset.UTC);
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = OffsetDateTime.now(ZoneOffset.UTC);
    }
}
```

```java
@Entity
@Table(name = "sp_accounts")
@Getter @Setter @NoArgsConstructor @AllArgsConstructor
public class AccountEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "owner_id", nullable = false, unique = true)
    private UUID ownerId;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal balance;

    @Column(nullable = false, length = 10)
    private String currency;

    @Version
    private Integer version;   // optimistic locking

    @Column(name = "date_created", updatable = false)
    private OffsetDateTime dateCreated;

    @Column(name = "date_modified")
    private OffsetDateTime dateModified;
}
```

---

## Service Pattern

```java
@Service
public class PaymentService {

    private static final Logger logger = LoggerFactory.getLogger(PaymentService.class);

    private final IdempotencyService idempotencyService;
    private final BalanceService balanceService;
    private final TransactionRepository transactionRepository;
    private final KafkaProducerService kafkaProducerService;

    // explicit constructor — no @RequiredArgsConstructor

    @Transactional(rollbackFor = Exception.class)
    public PaymentResponseDTO initiatePayment(PaymentRequestDTO dto) throws SwiftPayException {
        logger.info("Initiating payment for transactionId: {}", dto.getTransactionId());

        // 1. Idempotency check
        idempotencyService.checkDuplicate(dto.getTransactionId());

        // 2. Balance validation
        balanceService.validate(dto.getSenderId(), dto.getAmount());

        // 3. Persist PENDING transaction
        TransactionEntity tx = new TransactionEntity();
        tx.setSenderId(dto.getSenderId());
        tx.setReceiverId(dto.getReceiverId());
        tx.setAmount(dto.getAmount());
        tx.setCurrency(PaymentService.PAYMENT_CURRENCY);
        tx.setStatus(TransactionStatusEnum.PENDING);
        tx.setIdempotencyKey(dto.getTransactionId().toString());
        TransactionEntity saved = transactionRepository.save(tx);

        // 4. Emit Kafka event
        kafkaProducerService.emitPaymentInitiated(saved);

        // 5. Store idempotency key (after successful emit)
        idempotencyService.store(dto.getTransactionId());

        return ResponseUtil.paymentAccepted(saved);
    }
}
```

---

## Exception Handling

```java
// Base
public class SwiftPayException extends Exception {
    private final int httpStatus;
    private final String code;
    public SwiftPayException(int httpStatus, String code, String message) { ... }
    public SwiftPayException(int httpStatus, String code, String message, Throwable cause) { ... }
}

// Specifics
public class DuplicateTransactionException extends SwiftPayException {
    public DuplicateTransactionException(String transactionId) {
        super(409, "DUPLICATE_TRANSACTION", "Transaction " + transactionId + " already processed");
    }
}

public class InsufficientFundsException extends SwiftPayException {
    public InsufficientFundsException() {
        super(422, "INSUFFICIENT_FUNDS", "Sender has insufficient balance");
    }
}
```

**GlobalExceptionHandler:**
- `SwiftPayException` → use `ex.getHttpStatus()` for the response code
- `MethodArgumentNotValidException` → 400, collect all field errors
- `Exception` (catch-all) → 500, log with stack trace, return generic message

**Standard error body:**
```json
{ "code": "INSUFFICIENT_FUNDS", "message": "...", "timestamp": "...", "path": "/v1/payments" }
```

---

## Validation Rules (on PaymentRequestDTO)

```java
@NotNull(message = "transactionId is required")
private UUID transactionId;

@NotNull(message = "senderId is required")
private UUID senderId;

@NotNull(message = "receiverId is required")
private UUID receiverId;

@NotNull @DecimalMin(value = "0.01", message = "amount must be greater than zero")
private BigDecimal amount;
```

---

## Idempotency Service Implementation Guide

```java
@Service
public class IdempotencyService {
    private static final String PREFIX = "swiftpay:idempotency:";
    private static final long TTL_SECONDS = 86400L;

    private final StringRedisTemplate redisTemplate;

    public void checkDuplicate(UUID transactionId) throws DuplicateTransactionException {
        String key = PREFIX + transactionId;
        // Boolean — null means key didn't exist AND was set; false means NX failed (key existed)
        // We only STORE after success — here we just check existence
        Boolean exists = redisTemplate.hasKey(key);
        if (Boolean.TRUE.equals(exists)) {
            throw new DuplicateTransactionException(transactionId.toString());
        }
    }

    public void store(UUID transactionId) {
        String key = PREFIX + transactionId;
        redisTemplate.opsForValue().set(key, "processed", Duration.ofSeconds(TTL_SECONDS));
    }
}
```

---

## Balance Service Implementation Guide

```java
@Service
public class BalanceService {
    private static final String PREFIX = "swiftpay:balance:";
    private static final long CACHE_TTL = 30L;

    private final StringRedisTemplate redisTemplate;
    private final AccountRepository accountRepository;

    public void validate(UUID senderId, BigDecimal amount) throws SwiftPayException {
        BigDecimal balance = getCachedBalance(senderId);
        if (balance.compareTo(amount) < 0) {
            throw new InsufficientFundsException();
        }
    }

    private BigDecimal getCachedBalance(UUID senderId) throws SwiftPayException {
        String key = PREFIX + senderId;
        String cached = redisTemplate.opsForValue().get(key);
        if (cached != null) return new BigDecimal(cached);

        AccountEntity account = accountRepository.findByOwnerId(senderId)
            .orElseThrow(() -> new SwiftPayException(404, "ACCOUNT_NOT_FOUND", "Account not found"));

        redisTemplate.opsForValue().set(key, account.getBalance().toPlainString(), Duration.ofSeconds(CACHE_TTL));
        return account.getBalance();
    }

    public void invalidateCache(UUID ownerId) {
        redisTemplate.delete(PREFIX + ownerId);
    }
}
```

---

## KafkaProducerService Implementation Guide

```java
@Service
public class KafkaProducerService {
    private static final String TOPIC = "payment-initiated";
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);

    private final KafkaTemplate<String, PaymentInitiatedEvent> kafkaTemplate;

    public void emitPaymentInitiated(TransactionEntity tx) {
        PaymentInitiatedEvent event = new PaymentInitiatedEvent(
            tx.getId(), tx.getSenderId(), tx.getReceiverId(),
            tx.getAmount(), tx.getCurrency(), OffsetDateTime.now(ZoneOffset.UTC)
        );
        kafkaTemplate.send(TOPIC, tx.getId().toString(), event)
            .whenComplete((result, ex) -> {
                if (ex != null) logger.error("Kafka emit failed for txId={}", tx.getId(), ex);
                else logger.info("Emitted to Kafka: txId={} offset={}", tx.getId(), result.getRecordMetadata().offset());
            });
    }
}
```

---

## Health Endpoint Implementation Guide

```java
@RestController
@RequestMapping("/v1")
public class HealthController {

    private final DataSource dataSource;
    private final StringRedisTemplate redisTemplate;
    private final KafkaTemplate<?, ?> kafkaTemplate;

    @GetMapping("/health")
    public ResponseEntity<HealthResponseDTO> health() {
        String db    = checkDb();
        String redis = checkRedis();
        String kafka = checkKafka();
        String overall = (db.equals("UP") && redis.equals("UP") && kafka.equals("UP")) ? "UP" : "DOWN";
        int code = overall.equals("UP") ? 200 : 503;
        return ResponseEntity.status(code).body(new HealthResponseDTO(overall, db, redis, kafka));
    }

    private String checkDb() {
        try (Connection c = dataSource.getConnection()) { c.isValid(1); return "UP"; }
        catch (Exception e) { return "DOWN"; }
    }

    private String checkRedis() {
        try { redisTemplate.opsForValue().get("__health__"); return "UP"; }
        catch (Exception e) { return "DOWN"; }
    }

    private String checkKafka() {
        // Kafka admin check or just return UP if KafkaTemplate bean loaded
        return kafkaTemplate != null ? "UP" : "DOWN";
    }
}
```

---

## application.yml

```yaml
server:
  port: 8080

spring:
  config:
    import:
      - optional:file:../.env[.properties]
      - optional:file:.env[.properties]
  application:
    name: payment-gateway
  datasource:
    # Supabase connection string — use the "Transaction pooler" URL from Supabase dashboard
    # Dashboard → Project → Settings → Database → Connection string → select "Java" or "JDBC"
    # Format: jdbc:postgresql://<project-ref>.pooler.supabase.com:6543/<db-name>?pgpassword=<password>
    # Use port 6543 (Transaction Pooler) — NOT 5432 (direct). Direct connections are limited.
    url: ${DB_URL}
    username: ${DB_USERNAME}   # Supabase: postgres.<project-ref>  (NOT just "postgres")
    password: ${DB_PASSWORD}   # Your Supabase DB password
    driver-class-name: org.postgresql.Driver
    hikari:
      maximum-pool-size: 10    # Supabase free tier allows max 15 connections — stay under
      minimum-idle: 2
      connection-timeout: 30000
      # Supabase pooler drops idle connections — these settings prevent stale connection errors
      keepalive-time: 30000
      max-lifetime: 1800000
  jpa:
    hibernate:
      ddl-auto: validate       # NEVER use create/create-drop against Supabase — run schema manually
    show-sql: false
    properties:
      hibernate:
        format_sql: false
  data:
    redis:
      host: ${REDIS_HOST:localhost}
      port: ${REDIS_PORT:6379}
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP:localhost:9092}
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all
      retries: 3
      properties:
        enable.idempotence: true

springdoc:
  api-docs:
    path: /v3/api-docs
  swagger-ui:
    path: /swagger-ui.html

logging:
  level:
    com.swiftpay.gateway: INFO
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
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
```

---

## Testing Strategy

| Test | What to test | Tool |
|---|---|---|
| `PaymentServiceTest` | Idempotency check, insufficient funds, happy path | JUnit 5 + Mockito |
| `IdempotencyServiceTest` | Redis SET NX logic, duplicate detection | JUnit 5 + Mockito (mock RedisTemplate) |
| `PaymentControllerIntegrationTest` | Full HTTP → DB → Kafka flow | Testcontainers (PG + Redis + Kafka) |

**Testcontainers setup — one base class:**
```java
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
public abstract class BaseIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine");

    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", postgres::getJdbcUrl);
        r.add("spring.datasource.username", postgres::getUsername);
        r.add("spring.datasource.password", postgres::getPassword);
        r.add("spring.data.redis.host", redis::getHost);
        r.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
        r.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }
}
```

---

## Code Generation Rules for Cursor Agent

1. **Never skip layers.** Controller calls Service. Service calls Repository. No direct repository calls from Controller.
2. **Always use `throws SwiftPayException`** on service methods that can fail.
3. **All monetary amounts are `BigDecimal`.** Never `double` or `float`.
4. **Entity IDs are `UUID`.** Never `Long` or `String`.
5. **All timestamps are `OffsetDateTime` with `ZoneOffset.UTC`.**
6. **`@Transactional(rollbackFor = Exception.class)`** on all service write methods.
7. **Enums persisted as `@Enumerated(EnumType.STRING)`.**
8. **DTOs are never exposed as entities** — always map manually or with a simple method.
9. **Logger:** `private static final Logger logger = LoggerFactory.getLogger(ClassName.class);`
10. **Constructor injection only** — never `@Autowired` on fields.
11. **`@Valid` on every `@RequestBody`** in controllers.
12. **Table prefix:** always `sp_` (e.g. `sp_transactions`, `sp_accounts`).
13. **Swagger:** Add `@Operation`, `@ApiResponse` annotations on all controller methods.