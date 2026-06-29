# SwiftPay — System Architecture & Implementation Guide

> **Purpose:** This document is the single source of truth for the SwiftPay system.  
> Save this at `docs/SWIFTPAY_ARCHITECTURE.md` in your monorepo root.  
> Cursor agent reads this before generating any code across both services.

---

## Documentation maintenance (team habit)

Whenever you change **runtime behavior** (payment flow, Redis/Kafka usage, API contracts, schema, error codes, ports, or infra), update the related docs in the **same change**:

| If you change… | Update at minimum… |
|----------------|-------------------|
| Payment or ledger flow | This file — Sections 1, 6, 8 |
| Redis keys / TTL / idempotency | Section 6 (+ README “Inspect Redis” if ops steps change) |
| REST API (path, body, status codes) | Section 7, README API examples, Swagger `@Operation` on controllers |
| Database schema | Section 3, `docs/migrations/*.sql` |
| Local infra (`docker-compose.yml`, ports) | Section 4b, README “Run locally” |
| New cross-cutting concern (auditing, logging) | Section 3b and service README notes |

**Rule of thumb:** If a reviewer cannot understand the new behavior from docs alone, the docs are behind the code.

---

## 1. System Overview

SwiftPay is a real-time P2P payment ledger built as three Spring Boot microservices sharing a single PostgreSQL database (Supabase), communicating asynchronously via Apache Kafka, with Redis used by the gateway for caching and idempotency.

```
┌─────────────────────────────────────────────────────┐
│                   Client / Load Test                 │
└──────────────────────────┬──────────────────────────┘
                           │ POST /v1/payments
                           ▼
         ┌─────────────────────────────────┐
         │      payment-gateway :8080      │
         │                                 │
         │  1. Redis idempotency check      │
         │  2. Balance validation (Redis)   │
         │  3. Save PENDING → sp_transactions│
         │  4. Commit DB transaction        │
         │  5. [afterCommit] Kafka + Redis  │
         │  6. Return 202 Accepted          │
         └──────────┬──────────────────────┘
                    │ payment-initiated topic
                    ▼
         ┌─────────────────────────────────┐
         │       ledger-service :8081      │
         │                                 │
         │  1. Consume PaymentInitiated     │
         │  2. SELECT FOR UPDATE (2 rows)   │
         │  3. Debit sender, Credit receiver│
         │  4. UPDATE status → COMPLETED    │
         │  5. Emit PaymentCompleted → Kafka│
         └──────────┬──────────────────────┘
                    │ payment-completed
                    ▼
         ┌─────────────────────────────────┐
         │      analytics-worker :8082   │
         │  1. Consume PaymentCompleted     │
         │  2. INSERT sp_analytics_events   │
         │  3. GET /analytics/summary|volume│
         └─────────────────────────────────┘
                    │
         ┌──────────┴───────────┐
         │                      │
    payment-completed      payment-failed
    (analytics worker)     (failure tracking)

Shared infrastructure:
  PostgreSQL (Supabase) — sp_accounts, sp_transactions, sp_analytics_events
  Redis :6379       — idempotency keys, balance cache (gateway only)
  Kafka :9092       — topics: payment-initiated, payment-completed, payment-failed, payment-failed-dlq
```

---

## 2. Repository / Project Structure

```
swiftpay/                          ← monorepo root
├── payment-gateway/               ← Service A (Maven project)
│   ├── src/
│   ├── mvnw / mvnw.cmd
│   └── pom.xml
├── ledger-service/                ← Service B (Maven project)
│   ├── src/
│   ├── mvnw / mvnw.cmd
│   └── pom.xml
├── analytics/                     ← Service C — analytics worker (Maven project)
│   ├── src/
│   ├── mvnw / mvnw.cmd
│   ├── Dockerfile
│   └── pom.xml
├── docs/
│   ├── SWIFTPAY_ARCHITECTURE.md   ← THIS FILE
│   ├── seed.sql                   ← load-test account seed data
│   └── migrations/
│       ├── 001_add_auditing_columns.sql
│       └── 002_create_analytics_events.sql
├── load-test/
│   ├── payment-load-test.js       ← 250 RPS; K6_TARGET_REQUESTS=1M for duration
│   └── payment-load-test-smoke.js ← 1 min smoke test
├── docker-compose.yml             ← full local stack; see Section 4b
├── docker/postgres/init/          ← schema for Compose Postgres
├── k8s/                           ← Kubernetes Deployment + Service manifests
├── .env                           ← local config (gitignored; see Section 4)
└── .github/
    └── workflows/
        └── ci.yml                 ← verify (with tests) + Docker image builds
```

---

## 3. Database Schema (Full DDL)

All three services connect to the same Supabase database. Run core DDL once at startup (flyway or manual). Run `docs/migrations/002_create_analytics_events.sql` before starting **analytics-worker**.

```sql
-- Database: swiftpay

-- Accounts table
CREATE TABLE IF NOT EXISTS sp_accounts (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_id        UUID NOT NULL,
    balance         NUMERIC(19,4) NOT NULL DEFAULT 0,
    currency        VARCHAR(10) NOT NULL DEFAULT 'INR',
    version         INTEGER NOT NULL DEFAULT 0,
    created_by      UUID,
    modified_by     UUID,
    date_created    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    date_modified   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_accounts_owner UNIQUE (owner_id),
    CONSTRAINT chk_balance_non_negative CHECK (balance >= 0)
);

CREATE INDEX IF NOT EXISTS idx_accounts_owner_id ON sp_accounts(owner_id);

-- Transactions table
CREATE TABLE IF NOT EXISTS sp_transactions (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sender_id           UUID NOT NULL,
    receiver_id         UUID NOT NULL,
    amount              NUMERIC(19,4) NOT NULL,
    currency            VARCHAR(10) NOT NULL DEFAULT 'INR',
    status              VARCHAR(30) NOT NULL DEFAULT 'PENDING',
    idempotency_key     VARCHAR(255) NOT NULL,
    created_by          UUID,
    modified_by         UUID,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_transactions_idempotency UNIQUE (idempotency_key),
    CONSTRAINT chk_amount_positive CHECK (amount > 0),
    CONSTRAINT chk_sender_not_receiver CHECK (sender_id != receiver_id)
);

CREATE INDEX IF NOT EXISTS idx_tx_sender_id      ON sp_transactions(sender_id);
CREATE INDEX IF NOT EXISTS idx_tx_receiver_id    ON sp_transactions(receiver_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_tx_idempotency ON sp_transactions(idempotency_key);

-- Seed data for load testing (1000 accounts, each starting with 1,000,000 INR)
-- Generate in application or run seed script before load test
```

**Analytics table** (`sp_analytics_events`) — append-only events for volume monitoring. Full DDL: `docs/migrations/002_create_analytics_events.sql`. No FK to `sp_transactions`; duplicates on Kafka redelivery are acceptable (best-effort analytics).

### 3b. Auditing and Observability Implementation

Both services use Spring Data JPA auditing to keep audit fields consistent without duplicating timestamp code in every entity.

| Area | payment-gateway | ledger-service | Why |
|---|---|---|---|
| Base entity | `AuditableBaseEntity` | `AuditableBaseEntity` | Centralizes `id`, `created_by`, `modified_by`, created timestamp, and modified timestamp. |
| Versioned base | `VersionedAuditableBaseEntity` | `VersionedAuditableBaseEntity` | Adds optimistic locking for account rows using the existing `version` column. |
| Account entity | Extends `VersionedAuditableBaseEntity` | Extends `VersionedAuditableBaseEntity` | Account balances are updated concurrently, so optimistic locking is useful. |
| Transaction entity | Extends `AuditableBaseEntity` with date column overrides | Extends `AuditableBaseEntity` with date column overrides | Transactions use existing `created_at` / `updated_at` columns instead of `date_created` / `date_modified`. |
| Auditing config | `JpaAuditingConfig` | `JpaAuditingConfig` | Enables `@CreatedDate`, `@LastModifiedDate`, `@CreatedBy`, and `@LastModifiedBy`. |

`created_by` and `modified_by` are nullable because there is no authentication context yet. `AuditorAware<UUID>` currently returns empty; once auth is added, it should return the current authenticated user UUID.

For existing databases, run `docs/migrations/001_add_auditing_columns.sql` before starting the services because both apps use `spring.jpa.hibernate.ddl-auto=validate`.

Structured SLF4J logging is present in controllers, services, Kafka listeners, and exception handlers. It logs transaction IDs, user IDs, request/processing milestones, business failures such as duplicates or insufficient funds, and infrastructure failures for retry/DLQ visibility.

---

## 4. Supabase Setup

> PostgreSQL is hosted on Supabase. No local postgres container needed.

### Step 1 — Create Supabase project
1. Go to https://supabase.com → New project
2. Name it `swiftpay`, set a strong DB password, choose the closest region to Hyderabad (ap-south-1)
3. Wait for project to provision (~2 min)

### Step 2 — Run schema in Supabase SQL Editor
Dashboard → SQL Editor → New Query → paste the full DDL from Section 3 → Run.  
Then run the seed data from Section 13.

### Step 3 — Get your connection string
Dashboard → Settings → Database → Connection string → select **JDBC**  
Use the **Transaction Pooler** string (port 6543). It looks like:
```
jdbc:postgresql://aws-0-ap-south-1.pooler.supabase.com:6543/postgres?user=postgres.<project-ref>&password=<your-password>
```

> **Critical:** Use port **6543** (Transaction Pooler), NOT 5432 (direct connection).  
> Direct connections on the free tier are capped at 15 max — the pooler handles hundreds.

### Step 4 — Configuration (local + CI)

Both services read the same variables via `application.yml`:

| Variable | Used by | Purpose |
|----------|---------|---------|
| `DB_URL` | both | Supabase JDBC URL (pooler port **6543**) |
| `DB_USERNAME` | both | `postgres.<project-ref>` |
| `DB_PASSWORD` | both | Supabase database password |
| `REDIS_HOST` | payment-gateway | Redis host (default `localhost`) |
| `REDIS_PORT` | payment-gateway | Redis port (default `6379`) |
| `KAFKA_BOOTSTRAP` | both | Kafka brokers (default `localhost:9092`) |

**Local development** — create `.env` at monorepo root (gitignored):

```env
DB_URL=jdbc:postgresql://aws-0-ap-south-1.pooler.supabase.com:6543/postgres
DB_USERNAME=postgres.<your-project-ref>
DB_PASSWORD=<your-supabase-db-password>
REDIS_HOST=localhost
REDIS_PORT=6379
KAFKA_BOOTSTRAP=localhost:9092
```

Each service imports this file automatically:

```yaml
spring:
  config:
    import:
      - optional:file:../.env[.properties]
      - optional:file:.env[.properties]
```

Run from `payment-gateway/` or `ledger-service/` (or set IDE working directory to either module).

**GitHub Actions (CI)** — `.github/workflows/ci.yml` runs `./mvnw -B verify` **with tests** per module (Testcontainers for gateway integration tests). A follow-up job builds Docker images for all three services (push optional; no registry secrets required for build-only).

**Entry points**

| Service | Main class | Port |
|---------|------------|------|
| payment-gateway | `com.swiftpay.gateway.PaymentGatewayApplication` | 8080 |
| ledger-service | `com.swiftpay.ledger.LedgerApplication` | 8081 |

**Run locally (two terminals)**

```bash
# Terminal 1
cd payment-gateway && ./mvnw spring-boot:run

# Terminal 2
cd ledger-service && ./mvnw spring-boot:run
```

On Windows use `mvnw.cmd` instead of `./mvnw`. Start Redis and Kafka locally (or via Docker Compose in Section 4b) before payment-gateway.

---

## 4b. Docker Compose (local infra + apps)

`docker-compose.yml` at monorepo root runs the **full stack**:

```bash
docker compose up -d --build
```

| Service | Container | Host port | Notes |
|---------|-------------|-----------|--------|
| Postgres | `swiftpay-postgres` | `5432` | **Profile `local-db` only** — `docker compose --profile local-db up -d`; init SQL in `docker/postgres/init/` |
| Zookeeper | `swiftpay-zookeeper` | (internal) | Kafka |
| Kafka | `swiftpay-kafka` | `9092` | Apps in Compose use `kafka:29092` |
| Redis | `swiftpay-redis` | `6379` | Gateway |
| payment-gateway | `swiftpay-payment-gateway` | `8080` | `DB_*` from root `.env` (Supabase transaction pooler `:6543`) |
| ledger-service | `swiftpay-ledger-service` | `8081` | Same `.env` DB + `kafka:29092` |
| analytics | `swiftpay-analytics` | `8082` | Same `.env` DB + `kafka:29092` |

**Default DB:** `docker compose up -d` loads `DB_URL` / `DB_USERNAME` / `DB_PASSWORD` from `.env` (Supabase). Seed with `docs/seed.sql` in the Supabase SQL Editor. For offline local Postgres, add `--profile local-db` and point `.env` at `jdbc:postgresql://postgres:5432/swiftpay`.

**k6 profile:** `docker compose --profile loadtest run --rm k6 run /scripts/payment-load-test-smoke.js` (targets `http://payment-gateway:8080`).

**PCAP during load tests:** submitted trace `load-test/results/RPS10.pcapng` (10 RPS smoke); [docs/LOAD_TEST_PCAP.md](LOAD_TEST_PCAP.md).

**Kubernetes:** manifests under `k8s/` — edit `secret.yaml` for `DB_*`, deploy Kafka/Redis/Postgres separately or in-cluster.

---

## 5. Kafka Topics

| Topic | Producer | Consumer | Purpose |
|---|---|---|---|
| `payment-initiated` | payment-gateway | ledger-service | Trigger transfer execution |
| `payment-completed` | ledger-service | analytics-worker | Append analytics row; read APIs for volume |
| `payment-failed` | ledger-service | (future) notification service | Notify failure |
| `payment-failed-dlq` | ledger-service (error handler) | manual ops review | Dead letter — exhausted retries |

**Topic creation (auto-created by Kafka in dev; for production use admin API):**
```bash
kafka-topics.sh --create --topic payment-initiated   --partitions 6 --replication-factor 1 --bootstrap-server localhost:9092
kafka-topics.sh --create --topic payment-completed   --partitions 6 --replication-factor 1 --bootstrap-server localhost:9092
kafka-topics.sh --create --topic payment-failed      --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
kafka-topics.sh --create --topic payment-failed-dlq  --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
```

**Load-test setup:** run `docs/scripts/prepare-kafka-topics.ps1` so `payment-initiated` has **6 partitions**. Ledger listener `concurrency` defaults to **3** (`KAFKA_LISTENER_CONCURRENCY`).

---

## 6. Redis (payment-gateway only)

Redis is **not** used by ledger-service. The gateway uses it for two core requirements: **24-hour idempotency** on `transactionId` and **fast balance pre-checks** before accepting a payment.

### 6a. Key schema

| Key | TTL | Value | Set by | Used by |
|---|---|---|---|---|
| `swiftpay:idempotency:{transactionId}` | 86400s (24h) | `"processed"` | `IdempotencyService.store()` after DB save + Kafka emit | `IdempotencyService.checkDuplicate()` |
| `swiftpay:balance:{ownerId}` | 30s | balance as plain string, e.g. `"50000.0000"` | `BalanceService` on cache miss (read from Postgres) | `BalanceService.validate()` |

**Code references:** `payment-gateway/.../IdempotencyService.java`, `BalanceService.java`.

### 6b. Runtime behavior (matches code today)

**Idempotency**
```
1. EXISTS / hasKey("swiftpay:idempotency:{txId}") → if true → HTTP 409
2. validate balance → save PENDING row → commit Postgres
3. [afterCommit] emit payment-initiated + SET "swiftpay:idempotency:{txId}" "processed" EX 86400s
```

- Postgres also enforces `UNIQUE(idempotency_key)` on `sp_transactions` as a backstop.
- Kafka emit and idempotency `SET` run **after** DB commit so rolled-back payments are not marked processed and no orphan events are published.
- **Known gap:** check-then-set is not atomic (`SET NX` would be stronger under concurrent duplicate requests).

**Balance cache (cache-aside)**
```
1. GET swiftpay:balance:{senderId}
2. On miss → SELECT from sp_accounts → SET cache EX 30
3. Compare cached/DB balance to payment amount → HTTP 422 if insufficient
```

- Ledger debits/credits in Postgres with `SELECT FOR UPDATE`; Redis is a **performance hint**, not the ledger of record.
- `BalanceService.invalidateCache()` exists but is **not called** today; expiry is TTL-only (up to 30s stale balance at gateway).
- Gateway may accept a payment that ledger later marks `FAILED` if balance changed within the TTL window.

### 6c. Inspecting Redis (dev / debug)

Connect to `localhost:6379` (container `swiftpay-redis` when using Compose).

**Redis Insight:** add database `127.0.0.1:6379`, browse keys `swiftpay:idempotency:*` and `swiftpay:balance:*`, view TTL and values.

**Terminal:**
```bash
docker exec -it swiftpay-redis redis-cli
KEYS swiftpay:*
GET swiftpay:idempotency:<transaction-uuid>
GET swiftpay:balance:<owner-uuid>
TTL swiftpay:idempotency:<transaction-uuid>
```

After a successful `POST /v1/payments` you should see the idempotency key (~24h TTL). Balance keys appear on cache miss and disappear after ~30s.

---

## 7. REST API summary

| Method | Path | Service | OpenAPI `@Operation` | Notes |
|--------|------|---------|----------------------|-------|
| `POST` | `/v1/payments` | payment-gateway :8080 | Initiate a P2P payment | Body: `transactionId`, `senderId`, `receiverId`, `amount`, `currency` (`INR` only) → **202** `PENDING` |
| `GET` | `/v1/health` or `/health` | each service | Overall dependency health | **200** / **503** |
| `GET` | `/v1/health/db` or `/health/db` | gateway, ledger, analytics | Database only | **200** / **503** |
| `GET` | `/v1/health/redis` or `/health/redis` | payment-gateway :8080 | Redis only | **200** / **503** |
| `GET` | `/v1/health/kafka` or `/health/kafka` | gateway, ledger, analytics | Kafka only (broker reachable; not bean-only) | **200** / **503** |
| `GET` | `/v1/transactions/{userId}` | ledger-service :8081 | Fetch paginated transaction history for a user | Query: `page`, `size` (max 100) |
| `GET` | `/v1/analytics/summary` | analytics-worker :8082 | Aggregate transaction summary by currency | Query: `currency` (default `INR`) |
| `GET` | `/v1/analytics/volume` | analytics-worker :8082 | Volume by minute for last N minutes | Query: `minutes` (1–1440, default 60) |

Swagger UI: `http://localhost:8080/swagger-ui.html`, `http://localhost:8081/swagger-ui.html`, and `http://localhost:8082/swagger-ui.html`.

---

## 8. End-to-End Payment Flow (Step by Step)

```
Step 1: Client sends POST /v1/payments with:
  { transactionId, senderId, receiverId, amount, currency }

Step 2: PaymentController → PaymentService.initiatePayment(dto)

Step 3: validate currency == INR (else UnsupportedCurrencyException → 422, before idempotency)

Step 4: IdempotencyService.checkDuplicate(transactionId)
  → Redis EXISTS / hasKey swiftpay:idempotency:{transactionId}
  → If exists: throw DuplicateTransactionException → 409 response

Step 5: BalanceService.validate(senderId, amount)
  → Redis GET swiftpay:balance:{senderId}
  → Cache miss: AccountRepository.findByOwnerId(senderId)
  → If not found: 404
  → If balance < amount: throw InsufficientFundsException → 422 response
  → Cache result: SET swiftpay:balance:{senderId} {balance} EX 30

Step 6: @Transactional begins
  → TransactionRepository.save(TransactionEntity{status=PENDING, currency=INR, idempotencyKey=transactionId})

Step 7: @Transactional commits (PENDING row visible in Postgres)

Step 8: [afterCommit] KafkaProducerService.emitPaymentInitiated(savedTransaction)
  → KafkaTemplate.send("payment-initiated", txId, PaymentInitiatedEvent{..., currency=INR})

Step 9: [afterCommit] IdempotencyService.store(transactionId)
  → SET swiftpay:idempotency:{transactionId} "processed" EX 86400

Step 10: Return 202 Accepted { transactionId, status: "PENDING", message, timestamp }

---

Step 11: ledger-service PaymentInitiatedListener.onPaymentInitiated(event)

Step 12: LedgerService.transfer(event)   ← @Transactional begins
  → Lock order: sort(senderId, receiverId) → lock lower UUID first
  → accountRepository.findByOwnerIdForUpdate(firstUUID)   ← SELECT FOR UPDATE
  → accountRepository.findByOwnerIdForUpdate(secondUUID)  ← SELECT FOR UPDATE
  → if sender.balance < amount: updateStatus(FAILED), throw InsufficientFundsException
  → sender.balance  -= amount → save
  → receiver.balance += amount → save
  → transactionRepository.updateStatus(txId, COMPLETED)
  → @Transactional commits

Step 13: KafkaProducerService.emitCompleted(event)
  → Topic: payment-completed

Step 14: ack.acknowledge()   ← manual Kafka ack

Step 15: analytics-worker PaymentCompletedListener.onPaymentCompleted(event)
  → AnalyticsIngestService.record() → INSERT sp_analytics_events (append-only)
  → ack.acknowledge() on success; 3 retries with 1s backoff on failure (no DLQ — best-effort)

Step 16: Clients poll GET /v1/analytics/summary?currency=INR

--- On business failure (step 12 throws InsufficientFunds/AccountNotFound):
  → emitFailed(event, reason)
  → ack.acknowledge()  ← still ack, not retryable

--- On infrastructure failure (DB down, timeout):
  → Do NOT ack
  → DefaultErrorHandler retries up to 3 times with 1s backoff
  → On exhaustion: DeadLetterPublishingRecoverer → payment-failed-dlq
```

### 8b. Transaction history flow (ledger-service)

```
GET /v1/transactions/{userId}?page=0&size=20
  → TransactionController (logged request)
  → TransactionHistoryService.getHistory()
  → Verify account exists for userId (404 if not)
  → Native query: sender OR receiver, ORDER BY created_at DESC, paginated
  → Map to TransactionPageResponseDTO (createdAt from entity audit dateCreated)
```

---

## 9. Error Handling Matrix

| Scenario | Where handled | HTTP / Kafka behavior |
|---|---|---|
| Duplicate transactionId | payment-gateway | 409 Conflict |
| Sender account not found | payment-gateway | 404 Not Found |
| Insufficient funds (gateway check) | payment-gateway | 422 Unprocessable |
| Kafka emit fails | payment-gateway | 500 (transaction rolls back via @Transactional) |
| Insufficient funds (ledger double-check) | ledger-service | Status=FAILED, PaymentFailed event, ACK |
| Account not found in ledger | ledger-service | Status=FAILED, PaymentFailed event, ACK |
| DB connection lost during transfer | ledger-service | Retry 3x → DLQ, no ACK |
| Kafka consumer deserialization fails | ledger-service | DefaultErrorHandler → DLQ |
| DB constraint violation (duplicate tx) | ledger-service | Caught as Exception → Retry → DLQ |

---

## 10. GitHub Actions CI/CD

The current repo has one workflow at `.github/workflows/ci.yml` with separate jobs for both services.

| Job | Working directory | Current command | Required secrets |
|---|---|---|---|
| `payment-gateway` | `payment-gateway` | `./mvnw -B verify -DskipTests` | `DB_URL`, `DB_USERNAME`, `DB_PASSWORD`, optional Redis/Kafka vars |
| `ledger-service` | `ledger-service` | `./mvnw -B verify -DskipTests` | `DB_URL`, `DB_USERNAME`, `DB_PASSWORD`, optional Kafka vars |

This currently verifies that both Java services compile/package in CI. For fuller production alignment, the next CI improvements are:

1. Run unit and integration tests instead of skipping tests.
2. Add Dockerfiles for both services.
3. Build Docker images in CI after the Maven verification step.

---

## 11. Load Test

**Tool:** K6  
**Target:** 250 requests/second sustained, 1,000,000 total transactions  
**File:** `load-test/payment-load-test.js`

```javascript
import http from 'k6/http';
import { check, sleep } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

// Pre-seeded account IDs (must exist in DB before test)
const SENDER_IDS   = ['<uuid-1>', '<uuid-2>', '<uuid-3>', '<uuid-4>', '<uuid-5>'];
const RECEIVER_IDS = ['<uuid-6>', '<uuid-7>', '<uuid-8>', '<uuid-9>', '<uuid-10>'];

export const options = {
  scenarios: {
    constant_rate: {
      executor: 'constant-arrival-rate',
      rate: 250,
      timeUnit: '1s',
      duration: '<from K6_TARGET_REQUESTS/rate or 72m>',  // e.g. K6_TARGET_REQUESTS=1000000 → 4000s
      preAllocatedVUs: 300,
      maxVUs: 500,
    },
  },
  thresholds: {
    http_req_failed:   ['rate<0.01'],   // <1% error rate
    http_req_duration: ['p(95)<500'],   // 95th percentile under 500ms
  },
};

export default function () {
  const senderId   = SENDER_IDS[Math.floor(Math.random() * SENDER_IDS.length)];
  const receiverId = RECEIVER_IDS[Math.floor(Math.random() * RECEIVER_IDS.length)];

  const payload = JSON.stringify({
    transactionId: uuidv4(),
    senderId:   senderId,
    receiverId: receiverId,
    amount:     (Math.random() * 100 + 1).toFixed(2),
  });

  const res = http.post('http://localhost:8080/v1/payments', payload, {
    headers: { 'Content-Type': 'application/json' },
  });

  check(res, {
    'status is 202': (r) => r.status === 202,
    'has transactionId': (r) => JSON.parse(r.body).transactionId !== undefined,
  });
}
```

**Run (Docker — no local k6 install):**
```bash
# Infra + apps running; seed data loaded
docker compose --profile loadtest run --rm k6 run \
  --out json=/results/load-test-results.json \
  /scripts/payment-load-test.js
```

Results on host: `load-test/results/load-test-results.json`, `load-test-1m-summary.json`.  
Latest full run: **999,809** requests, **~250 RPS**, p95 **~111 ms** (prior: 946,087, p95 **~1.75 s**) — [PERFORMANCE.md](PERFORMANCE.md).  
Smoke: `/scripts/payment-load-test-smoke.js` → `load-test/results/smoke-10rps.json`.

---

## 12. Performance Tuning Checklist

After the baseline load test, identify the bottleneck and fix one:

| Likely bottleneck | Symptom | Fix |
|---|---|---|
| DB connection pool exhaustion | Pool timeout errors in logs | Increase `hikari.maximum-pool-size` from 10 to 30 |
| Kafka producer linger | Low throughput, high p95 latency | Set `spring.kafka.producer.properties.linger.ms=5` and `batch-size=65536` |
| Redis round-trips | Redis CPU high, gateway latency spiky | Pipeline idempotency check + store into one command using `execute()` |
| Ledger `SELECT FOR UPDATE` contention | DB lock wait timeouts | Increase Kafka consumer `concurrency` from 1 to 3 in `ConcurrentKafkaListenerContainerFactory` |
| HikariCP idle connections | Memory pressure | Tune `minimum-idle=5`, `idle-timeout=300000` |

**Document the fix in README with before/after numbers from the K6 output.**

---

## 12. Maven pom.xml Dependencies Reference

**Both services need:**
```xml
<dependencies>
  <!-- Spring Boot starters -->
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

  <!-- Database -->
  <dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <scope>runtime</scope>
  </dependency>

  <!-- Swagger -->
  <dependency>
    <groupId>org.springdoc</groupId>
    <artifactId>springdoc-openapi-starter-webmvc-ui</artifactId>
    <version>2.3.0</version>
  </dependency>

  <!-- Lombok -->
  <dependency>
    <groupId>org.projectlombok</groupId>
    <artifactId>lombok</artifactId>
    <optional>true</optional>
  </dependency>

  <!-- Test -->
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
  <dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>junit-jupiter</artifactId>
    <scope>test</scope>
  </dependency>
  <dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>postgresql</artifactId>
    <scope>test</scope>
  </dependency>
  <dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>kafka</artifactId>
    <scope>test</scope>
  </dependency>
</dependencies>
```

**payment-gateway only:**
```xml
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-data-redis</artifactId>
</dependency>
<dependency>
  <groupId>org.testcontainers</groupId>
  <artifactId>testcontainers</artifactId>
  <scope>test</scope>
</dependency>
```

---

## 14. Seed Data Script

Save as `docs/seed.sql`. **Run this in the Supabase SQL Editor** (Dashboard → SQL Editor), not locally.  
Also used by Testcontainers integration tests — Testcontainers auto-applies schema + seed via its own init scripts.

```sql
-- Insert 20 test accounts for load testing
INSERT INTO sp_accounts (id, owner_id, balance, currency) VALUES
  ('a0000001-0000-0000-0000-000000000001', 'b0000001-0000-0000-0000-000000000001', 10000000.0000, 'INR'),
  ('a0000002-0000-0000-0000-000000000002', 'b0000002-0000-0000-0000-000000000002', 10000000.0000, 'INR'),
  ('a0000003-0000-0000-0000-000000000003', 'b0000003-0000-0000-0000-000000000003', 10000000.0000, 'INR'),
  ('a0000004-0000-0000-0000-000000000004', 'b0000004-0000-0000-0000-000000000004', 10000000.0000, 'INR'),
  ('a0000005-0000-0000-0000-000000000005', 'b0000005-0000-0000-0000-000000000005', 10000000.0000, 'INR'),
  ('a0000006-0000-0000-0000-000000000006', 'b0000006-0000-0000-0000-000000000006', 10000000.0000, 'INR'),
  ('a0000007-0000-0000-0000-000000000007', 'b0000007-0000-0000-0000-000000000007', 10000000.0000, 'INR'),
  ('a0000008-0000-0000-0000-000000000008', 'b0000008-0000-0000-0000-000000000008', 10000000.0000, 'INR'),
  ('a0000009-0000-0000-0000-000000000009', 'b0000009-0000-0000-0000-000000000009', 10000000.0000, 'INR'),
  ('a0000010-0000-0000-0000-000000000010', 'b0000010-0000-0000-0000-000000000010', 10000000.0000, 'INR')
ON CONFLICT DO NOTHING;
```

---

## 15. README Template

Save as `README.md` at monorepo root.

```markdown
# SwiftPay — Real-Time P2P Payment Ledger

## Architecture
- **Service A (payment-gateway :8080):** Accepts payment requests, enforces idempotency via Redis, validates balance, persists PENDING transaction, emits to Kafka.
- **Service B (ledger-service :8081):** Consumes Kafka events, performs atomic debit/credit with pessimistic locking, emits result events.
- **Shared infra:** PostgreSQL, Redis, Kafka.

## How to Run

```bash
docker-compose up --build
```

All services start in dependency order. Swagger UI available at:
- http://localhost:8080/swagger-ui.html (payment-gateway)
- http://localhost:8081/swagger-ui.html (ledger-service)

## API Examples

```bash
# Create a payment
curl -X POST http://localhost:8080/v1/payments \
  -H "Content-Type: application/json" \
  -d '{
    "transactionId": "550e8400-e29b-41d4-a716-446655440000",
    "senderId":      "b0000001-0000-0000-0000-000000000001",
    "receiverId":    "b0000002-0000-0000-0000-000000000002",
    "amount":        500.00
  }'

# Get transaction history
curl http://localhost:8081/v1/transactions/b0000001-0000-0000-0000-000000000001?page=0&size=20

# Health checks
curl http://localhost:8080/v1/health
curl http://localhost:8081/v1/health
```

## Load Test Results

**Full load (250 RPS, ~1M target) — latest (2026-05-25):**
- Requests: **999,809** | Throughput: **~250/s** | Error rate: **0.01%**
- p50: **~56 ms** | p95: **~111 ms** | Dropped iterations: **191**

**Full load — previous (same target, ~67 min):**
- Requests: **946,087** | Throughput: **~235/s** | Error rate: **0.01%**
- p50: **~61 ms** | p95: **~1.75 s** | Dropped iterations: **58,914**

Details: [PERFORMANCE.md](PERFORMANCE.md). Fixes: PgBouncer prepared statements, Kafka partitions + consumer concurrency, pool/Hikari tuning — [ARCHITECTURE_AND_OPTIMIZATION.md](ARCHITECTURE_AND_OPTIMIZATION.md).

## Known Limitations
- Balance cache has 30s TTL — brief read inconsistency is acceptable for this use case.
- Single Kafka partition per topic — would need partitioning strategy for production scale.
- No authentication on APIs — out of scope for this submission.
```

---

## 16. Quick-Start Code Generation Prompts for Cursor

Use these exact prompts in Cursor after placing the CLAUDE.md in each service root:

**payment-gateway:**
```
Read CLAUDE.md completely. Then generate the following in order:
1. All entity classes (AccountEntity, TransactionEntity) with exact column names from the schema in CLAUDE.md
2. All repository interfaces (AccountRepository, TransactionRepository) with the queries specified
3. All DTO classes (PaymentRequestDTO with validation, PaymentResponseDTO, HealthResponseDTO, ErrorResponseDTO)
4. TransactionStatusEnum
5. PaymentInitiatedEvent record
6. All exception classes (SwiftPayException, DuplicateTransactionException, InsufficientFundsException) and GlobalExceptionHandler
7. IdempotencyService with exact Redis key pattern from CLAUDE.md
8. BalanceService with exact Redis key pattern and cache invalidation method
9. KafkaProducerService emitting PaymentInitiatedEvent
10. PaymentService orchestrating steps 1-7 of the payment flow
11. PaymentController and HealthController
12. KafkaProducerConfig, RedisConfig, OpenApiConfig
13. application.yml with all env var placeholders
14. Dockerfile (multi-stage, Java 21)
```

**ledger-service:**
```
Read CLAUDE.md completely. Then generate the following in order:
1. All entity classes (same AccountEntity and TransactionEntity as payment-gateway — same DB)
2. AccountRepository with findByOwnerIdForUpdate native query (SELECT FOR UPDATE)
3. TransactionRepository with findByUserId paginated query and updateStatus @Modifying query
4. All DTO classes (TransactionHistoryResponseDTO, TransactionPageResponseDTO, ErrorResponseDTO)
5. TransactionStatusEnum, PaymentInitiatedEvent, PaymentCompletedEvent, PaymentFailedEvent
6. All exception classes and GlobalExceptionHandler
7. LedgerService.transfer() with UUID-sorted pessimistic locking as described in CLAUDE.md
8. KafkaProducerService emitting PaymentCompleted and PaymentFailed events
9. TransactionHistoryService.getHistory() with pagination
10. PaymentInitiatedListener with correct ACK/no-ACK behavior per error type
11. TransactionController (GET /v1/transactions/{userId})
12. KafkaConsumerConfig with DefaultErrorHandler + DeadLetterPublishingRecoverer + FixedBackOff(1000L, 3L)
13. KafkaProducerConfig and OpenApiConfig
14. application.yml with all env var placeholders
15. Dockerfile (multi-stage, Java 21)
```
