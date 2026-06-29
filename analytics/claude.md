# SwiftPay — Analytics Worker (Service C)

## Claude Code Instructions

---

## Project Overview

**Service:** analytics-worker  
**Role:** Bonus service. Consumes `PaymentCompleted` events from Kafka and writes aggregated analytics records to a Supabase PostgreSQL analytics table (`sp_analytics_events`). Exposes a read API for real-time volume monitoring.  
**Java:** 21 | **Framework:** Spring Boot 3.x | **Build:** Maven  
**DB:** PostgreSQL on Supabase — same project, same connection string as Services A and B  
**Messaging:** Kafka (consumer only)  
**Documentation:** Swagger / OpenAPI 3 (springdoc-openapi)  
**Base package:** `com.swiftpay.analytics` | **Context path:** `/v1`  
**Port:** `8082`

**Why NOT ClickHouse:** The hackathon says "ClickHouse or a mock analytics table." You are already on Supabase. Adding a separate ClickHouse instance wastes 2–3 hours of setup for zero additional score. The `sp_analytics_events` table in Supabase satisfies the requirement fully — it is an append-only write, which is exactly how ClickHouse is used. The evaluator cares that you consume the event and persist analytics data, not which engine you use.

---

## Package Structure

```
com.swiftpay.analytics/
├── AnalyticsApplication.java       ← @SpringBootApplication entry point
├── config/
│   ├── KafkaConsumerConfig         ← ConsumerFactory for PaymentCompletedEvent
│   └── OpenApiConfig               ← Swagger/OpenAPI bean
├── controller/
│   └── AnalyticsController         ← GET /v1/analytics/summary, GET /v1/analytics/volume
├── dto/
│   ├── AnalyticsSummaryDTO         ← totalTransactions, totalVolume, currency, windowStart, windowEnd
│   ├── VolumeByMinuteDTO           ← minute (timestamp), count, totalAmount
│   └── ErrorResponseDTO            ← code, message, timestamp, path
├── entity/
│   └── AnalyticsEventEntity        ← maps to sp_analytics_events table
├── event/
│   └── PaymentCompletedEvent       ← Kafka inbound (matches ledger-service output exactly)
├── exception/
│   ├── SwiftPayException           ← same pattern as other services
│   └── GlobalExceptionHandler      ← @RestControllerAdvice
├── kafka/
│   └── PaymentCompletedListener    ← @KafkaListener on payment-completed topic
├── repository/
│   └── AnalyticsEventRepository    ← JpaRepository<AnalyticsEventEntity, UUID>
└── service/
    ├── AnalyticsIngestService      ← maps event → entity → save
    └── AnalyticsQueryService       ← summary + volume queries
```

---

## Architecture — What This Service Does

```
Kafka Topic: payment-completed
  │  (emitted by ledger-service after every successful debit/credit)
  ▼
PaymentCompletedListener.onPaymentCompleted(event)
  │
  └─► AnalyticsIngestService.record(event)
        → map PaymentCompletedEvent to AnalyticsEventEntity
        → analyticsEventRepository.save(entity)
        → INSERT into sp_analytics_events (append-only, never updated)

REST API (read-only, no auth):
GET /v1/analytics/summary?currency=INR
  └─► AnalyticsQueryService.getSummary(currency)
        → COUNT + SUM of all completed transactions
        → returns { totalTransactions, totalVolume, currency }

GET /v1/analytics/volume?minutes=60
  └─► AnalyticsQueryService.getVolumeByMinute(minutes)
        → GROUP BY minute bucket (date_trunc), last N minutes
        → returns list of { minute, count, totalAmount }
```

---

## Database Schema

**Run this in Supabase SQL Editor — same project as Services A and B.**

```sql
CREATE TABLE IF NOT EXISTS sp_analytics_events (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id      UUID NOT NULL,
    sender_id           UUID NOT NULL,
    receiver_id         UUID NOT NULL,
    amount              NUMERIC(19,4) NOT NULL,
    currency            VARCHAR(10) NOT NULL DEFAULT 'INR',
    completed_at        TIMESTAMPTZ NOT NULL,
    recorded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for volume-by-minute queries (GROUP BY date_trunc on completed_at)
CREATE INDEX IF NOT EXISTS idx_analytics_completed_at ON sp_analytics_events(completed_at DESC);
CREATE INDEX IF NOT EXISTS idx_analytics_currency ON sp_analytics_events(currency);

-- No UNIQUE constraint on transaction_id — idempotency handled in code (Redis or DB upsert)
-- If Kafka delivers the same event twice, we accept a duplicate row (analytics is approximate)
```

---

## Kafka Configuration

**Consumed topic:** `payment-completed` (produced by ledger-service)  
**This service produces nothing.**

**Consumer config:**

```
bootstrap.servers      = localhost:9092
group.id               = analytics-worker-group
key.deserializer       = StringDeserializer
value.deserializer     = JsonDeserializer
trusted.packages       = com.swiftpay.ledger.event, com.swiftpay.analytics.event
auto.offset.reset      = earliest
enable.auto.commit     = false   ← manual ack
```

**Retry policy:** 3 retries with 1s backoff. On exhaustion: log and ack (analytics is best-effort — losing one event does not corrupt the ledger).

---

## PaymentCompletedEvent (inbound — must match ledger-service output exactly)

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

This is the same record ledger-service emits. Field names must match exactly — Jackson deserializes by field name.

---

## Entity Pattern

```java
@Entity
@Table(name = "sp_analytics_events")
@Getter @Setter @NoArgsConstructor @AllArgsConstructor
public class AnalyticsEventEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "transaction_id", nullable = false)
    private UUID transactionId;

    @Column(name = "sender_id", nullable = false)
    private UUID senderId;

    @Column(name = "receiver_id", nullable = false)
    private UUID receiverId;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, length = 10)
    private String currency;

    @Column(name = "completed_at", nullable = false)
    private OffsetDateTime completedAt;

    @Column(name = "recorded_at", nullable = false, updatable = false)
    private OffsetDateTime recordedAt;

    @PrePersist
    protected void onCreate() {
        recordedAt = OffsetDateTime.now(ZoneOffset.UTC);
    }
}
```

---

## AnalyticsEventRepository

```java
@Repository
public interface AnalyticsEventRepository extends JpaRepository<AnalyticsEventEntity, UUID> {

    // Summary: total count + total volume for a currency
    @Query(value = """
            SELECT COUNT(*) as total_count,
                   COALESCE(SUM(amount), 0) as total_volume
            FROM sp_analytics_events
            WHERE currency = :currency
            """, nativeQuery = true)
    Object[] getSummary(@Param("currency") String currency);

    // Volume by minute — last N minutes, grouped by minute bucket
    @Query(value = """
            SELECT date_trunc('minute', completed_at) AS minute,
                   COUNT(*) AS count,
                   SUM(amount) AS total_amount
            FROM sp_analytics_events
            WHERE completed_at >= NOW() - INTERVAL '1 minute' * :minutes
            GROUP BY date_trunc('minute', completed_at)
            ORDER BY minute DESC
            """, nativeQuery = true)
    List<Object[]> getVolumeByMinute(@Param("minutes") int minutes);
}
```

---

## Kafka Listener

```java
@Component
public class PaymentCompletedListener {

    private static final Logger logger = LoggerFactory.getLogger(PaymentCompletedListener.class);

    private final AnalyticsIngestService analyticsIngestService;

    // explicit constructor

    @KafkaListener(
        topics = "payment-completed",
        groupId = "analytics-worker-group",
        containerFactory = "analyticsKafkaListenerContainerFactory"
    )
    public void onPaymentCompleted(PaymentCompletedEvent event, Acknowledgment ack) {
        logger.info("Analytics received PaymentCompleted: transactionId={} amount={} currency={}",
            event.transactionId(), event.amount(), event.currency());
        try {
            analyticsIngestService.record(event);
            ack.acknowledge();
        } catch (Exception e) {
            logger.error("Analytics ingest failed for transactionId={}", event.transactionId(), e);
            // Analytics is best-effort — ack anyway to avoid blocking the consumer
            // On repeated failures the record will be retried by DefaultErrorHandler before this ack
            throw new RuntimeException("Analytics ingest failed", e);
        }
    }
}
```

---

## AnalyticsIngestService

```java
@Service
public class AnalyticsIngestService {

    private static final Logger logger = LoggerFactory.getLogger(AnalyticsIngestService.class);

    private final AnalyticsEventRepository analyticsEventRepository;

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
```

---

## AnalyticsQueryService

```java
@Service
public class AnalyticsQueryService {

    private final AnalyticsEventRepository analyticsEventRepository;

    public AnalyticsSummaryDTO getSummary(String currency) throws SwiftPayException {
        try {
            Object[] result = analyticsEventRepository.getSummary(currency);
            long totalTransactions = ((Number) result[0]).longValue();
            BigDecimal totalVolume = new BigDecimal(result[1].toString());
            return new AnalyticsSummaryDTO(totalTransactions, totalVolume, currency);
        } catch (Exception e) {
            throw new SwiftPayException(500, "ANALYTICS_QUERY_FAILED", "Failed to fetch summary", e);
        }
    }

    public List<VolumeByMinuteDTO> getVolumeByMinute(int minutes) throws SwiftPayException {
        if (minutes < 1 || minutes > 1440) {
            throw new SwiftPayException(400, "INVALID_MINUTES", "minutes must be between 1 and 1440");
        }
        try {
            List<Object[]> rows = analyticsEventRepository.getVolumeByMinute(minutes);
            return rows.stream().map(row -> new VolumeByMinuteDTO(
                row[0].toString(),                          // minute as string
                ((Number) row[1]).longValue(),              // count
                new BigDecimal(row[2].toString())           // totalAmount
            )).toList();
        } catch (Exception e) {
            throw new SwiftPayException(500, "ANALYTICS_QUERY_FAILED", "Failed to fetch volume", e);
        }
    }
}
```

---

## REST API Contract

### GET /v1/analytics/summary

**Query params:**

- `currency` (string, default `INR`)

**Response 200:**

```json
{
  "totalTransactions": 946087,
  "totalVolume": 47304350.00,
  "currency": "INR"
}
```

### GET /v1/analytics/volume

**Query params:**

- `minutes` (int, default 60, max 1440)

**Response 200:**

```json
[
  { "minute": "2025-01-01T10:05:00Z", "count": 1245, "totalAmount": 62250.00 },
  { "minute": "2025-01-01T10:04:00Z", "count": 1312, "totalAmount": 65600.00 }
]
```

---

## KafkaConsumerConfig — Full Config

```java
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ConsumerFactory<String, PaymentCompletedEvent> analyticsConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "analytics-worker-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(JsonDeserializer.TRUSTED_PACKAGES,
            "com.swiftpay.ledger.event,com.swiftpay.analytics.event");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, PaymentCompletedEvent.class.getName());
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
            new JsonDeserializer<>(PaymentCompletedEvent.class, false));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentCompletedEvent> analyticsKafkaListenerContainerFactory(
            ConsumerFactory<String, PaymentCompletedEvent> analyticsConsumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, PaymentCompletedEvent> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(analyticsConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // 3 retries, 1s backoff, then log and move on (analytics is best-effort)
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(1000L, 3L));
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}
```

---

## application.yml

```yaml
server:
  port: 8082

spring:
  config:
    import:
      - optional:file:../.env[.properties]
      - optional:file:.env[.properties]
  application:
    name: analytics-worker
  datasource:
    url: ${DB_URL}
    username: ${DB_USERNAME}
    password: ${DB_PASSWORD}
    driver-class-name: org.postgresql.Driver
    hikari:
      maximum-pool-size: 5     # analytics writes are low volume — small pool is fine
      minimum-idle: 1
      keepalive-time: 30000
      max-lifetime: 1800000
  jpa:
    hibernate:
      ddl-auto: validate
    show-sql: false
    properties:
      hibernate:
        dialect: org.hibernate.dialect.PostgreSQLDialect
        jdbc:
          use_server_prepared_stmts: false   # required for Supabase transaction pooler
        prepare_threshold: 0                 # required for Supabase transaction pooler
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP:localhost:9092}
    consumer:
      group-id: analytics-worker-group
      auto-offset-reset: earliest
      enable-auto-commit: false
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "com.swiftpay.ledger.event,com.swiftpay.analytics.event"

springdoc:
  api-docs:
    path: /v3/api-docs
  swagger-ui:
    path: /swagger-ui.html

logging:
  level:
    com.swiftpay.analytics: INFO
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
EXPOSE 8082
ENTRYPOINT ["java", "-jar", "app.jar"]
```

---

## pom.xml Dependencies

```xml
<dependencies>
  <dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-web</artifactId>
  </dependency>
  <dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-jpa</artifactId>
  </dependency>
  <dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-validation</artifactId>
  </dependency>
  <dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka</artifactId>
  </dependency>
  <dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <scope>runtime</scope>
  </dependency>
  <dependency>
    <groupId>org.springdoc</groupId>
    <artifactId>springdoc-openapi-starter-webmvc-ui</artifactId>
    <version>2.3.0</version>
  </dependency>
  <dependency>
    <groupId>org.projectlombok</groupId>
    <artifactId>lombok</artifactId>
    <optional>true</optional>
  </dependency>
  <dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-test</artifactId>
    <scope>test</scope>
  </dependency>
  <dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka-test</artifactId>
    <scope>test</scope>
  </dependency>
</dependencies>
```

---

## Code Generation Rules for Cursor Agent

1. **Port is 8082** — not 8080 or 8081.
2. **Table name is `sp_analytics_events`** — prefix `sp_` like all other tables.
3. **This service is consumer-only** — no KafkaTemplate, no KafkaProducerConfig needed.
4. `**PaymentCompletedEvent` field names must match ledger-service exactly** — `transactionId`, `senderId`, `receiverId`, `amount`, `currency`, `completedAt`. If they don't match, Jackson deserialization silently gives null fields.
5. **Analytics is best-effort** — on repeated ingest failure after retries, log and move on. Do NOT route to DLQ. A missing analytics row does not corrupt the financial ledger.
6. `**@Transactional(rollbackFor = Exception.class)`** on `AnalyticsIngestService.record()`.
7. **Constructor injection only** — never `@Autowired` on fields.
8. **All monetary amounts are `BigDecimal`.**
9. `**@Enumerated(EnumType.STRING)`** on all enums.
10. **Supabase pooler settings required** in `application.yml`: `use_server_prepared_stmts: false` and `prepare_threshold: 0`. Without these the service will throw `prepared statement "S_xx" does not exist` errors under load (same issue that was fixed in Services A and B).
11. **KafkaConsumerConfig bean names must be unique** — use `analyticsKafkaListenerContainerFactory` (not `kafkaListenerContainerFactory`) to avoid Spring bean name conflicts if all three services ever run in the same context.
12. `**getSummary()` returns `Object[]`** — cast `result[0]` to Number for count, `result[1]` to String then BigDecimal for volume.

