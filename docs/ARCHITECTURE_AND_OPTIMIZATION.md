# SwiftPay — Architecture, Data Stores & Performance Optimization

> How **Redis**, **Kafka**, and **PostgreSQL (Supabase)** work together in SwiftPay, what we ran **before** tuning, what we **changed**, and **why** load-test latency improved.

Related docs: [SWIFTPAY_ARCHITECTURE.md](SWIFTPAY_ARCHITECTURE.md) (full system spec), [PERFORMANCE.md](PERFORMANCE.md) (k6 numbers).

---

## 1. System overview

SwiftPay is two Spring Boot services on a **shared Postgres database**, with **async settlement** over Kafka and **fast checks** in Redis.

```
┌─────────────────────────────────────────────────────┐
│              Client / k6 load test                   │
└──────────────────────────┬──────────────────────────┘
                           │ POST /v1/payments
                           ▼
         ┌─────────────────────────────────┐
         │   payment-gateway  :8080        │  ← k6 measures this path (202)
         │   • Redis: idempotency + cache  │
         │   • Postgres: INSERT PENDING    │
         │   • Kafka: payment-initiated    │
         └──────────┬──────────────────────┘
                    │
                    ▼  Kafka (local Docker)
         ┌─────────────────────────────────┐
         │   ledger-service  :8081         │
         │   • Consume payment-initiated   │
         │   • Postgres: transfer + status │
         │   • Kafka: completed / failed   │
         └─────────────────────────────────┘

Infrastructure:
  PostgreSQL  — Supabase (transaction pooler :6543)  [remote]
  Redis       — localhost:6379                       [local Docker]
  Kafka       — localhost:9092                       [local Docker]
```

### Two speeds of the system

| Phase | Service | What happens | Client sees |
|-------|---------|--------------|-------------|
| **Accept (sync)** | payment-gateway | Validate, insert `PENDING`, return | **HTTP 202** |
| **Settle (async)** | ledger-service | Lock accounts, move money, update status | (later / other APIs) |

This is **event-driven**: the gateway does not wait for the transfer to finish before responding.

---

## 2. Database (PostgreSQL / Supabase)

### 2.1 Design we follow

- **Single database** for both services: tables `sp_accounts`, `sp_transactions`.
- **Gateway**
  - Read sender balance (Redis first, Postgres on cache miss).
  - `INSERT` into `sp_transactions` with status `PENDING`.
- **Ledger**
  - `SELECT … FOR UPDATE` on two account rows (ordered by `owner_id` to reduce deadlock).
  - Debit sender, credit receiver, set transaction `COMPLETED` or `FAILED`.
- **Pooling:** HikariCP in each service (`HIKARI_MAX_POOL_SIZE`, default **30** in `.env`).
- **Supabase mode:** **Transaction pooler** on port **6543** (PgBouncer) — correct for many short-lived connections from two JVMs.

Indexes (from schema): `owner_id`, `idempotency_key`, `sender_id`, `receiver_id`.

### 2.2 Before optimization

| Aspect | Configuration | Problem under load |
|--------|---------------|-------------------|
| Pool size | **10** per service | Threads blocked waiting for connections |
| JDBC + pooler | Default **server-side prepared statements** | `ERROR: prepared statement "S_xx" does not exist` |
| Location | **Remote** Supabase (`ap-south-1`) | Every query adds network RTT |

**What users saw in logs and k6:**

```text
ERROR: prepared statement "S_82" does not exist
insert into sp_transactions (...)
→ HTTP 500 (Unexpected error)
```

**Root cause:** PgBouncer **transaction pooling** reassigns backend connections per transaction. Hibernate/PostgreSQL JDBC cache **named** prepared statements (`S_82`, `S_85`, …) on a connection that may not be the same on the next statement → random failures under concurrency.

This drove **~40% `http_req_failed`** in early smoke tests — not primarily “missing indexes.”

### 2.3 What we optimized

| Change | File / config |
|--------|----------------|
| `prepareThreshold: 0` | `application.yml` → Hikari `data-source-properties` (both services) |
| `hibernate.jdbc.use_server_prepared_stmts: false` | `application.yml` (both services) |
| `HIKARI_MAX_POOL_SIZE=30` | `.env` |

**Stay on transaction pooler (6543)** with these settings — do not remove `prepareThreshold=0` if you keep the pooler.

### 2.4 Why speed improved (database)

1. **Eliminated ~40% HTTP 500s** — almost every request could commit the `PENDING` row.
2. **Larger pool** — less time blocked on `Connection is not available`.
3. **Stable p50/p95 at 10 and 50 RPS** after fixes (see [PERFORMANCE.md](PERFORMANCE.md)).

**What still limits us:** Postgres can be **remote** (Supabase). An earlier **250 RPS × 67 min** run had p95 **~1.75 s** (DB/contention); the latest **~1M @ 250 RPS** run reached **~111 ms** p95 with **191** dropped iterations — see [PERFORMANCE.md](PERFORMANCE.md).

---

## 3. Redis (payment-gateway only)

### 3.1 Design we follow

Redis runs in **local Docker** (`swiftpay-redis`). **Ledger-service does not use Redis.**

| Key pattern | TTL | Value | Set when | Used when |
|-------------|-----|-------|----------|-----------|
| `swiftpay:idempotency:{transactionId}` | 24 h | `"processed"` | After successful DB commit | Before accept — duplicate → **409** |
| `swiftpay:balance:{ownerId}` | 30 s | balance string | On cache miss after DB read | Before accept — insufficient → **422** |

**Cache-aside balance:**

```
GET swiftpay:balance:{senderId}
  → hit: use cached balance
  → miss: SELECT sp_accounts, SET cache EX 30s
```

Postgres `UNIQUE(idempotency_key)` is a **backstop** if Redis is empty.

**Important:** Redis balance is a **performance hint**. Ledger uses Postgres locks for the real transfer. Gateway can accept a payment that ledger later marks `FAILED` if balance changed within the TTL window.

### 3.2 Before vs after (Redis)

Redis location and TTLs did **not** change for the big performance win — it was already local and fast.

**Behavior change (with DB/Kafka tuning):**

| Before | After |
|--------|--------|
| Kafka emit + idempotency store timing mixed with open DB transaction | **After `afterCommit`:** Kafka send + idempotency `SET` only run once Postgres commit succeeds |

Code reference: `PaymentService.initiatePayment()` — `TransactionSynchronization.afterCommit()`.

### 3.3 Why Redis matters for latency

- **Idempotency check:** ~sub-ms local `EXISTS` vs no DB round-trip for duplicates.
- **Balance hit:** avoids Supabase read on most smoke/load requests when the same senders repeat (k6 uses a small set of account IDs).
- At **50 RPS**, cache stays **hot** → median gateway time often **lower** than sparse **10 RPS** runs (see §6).

---

## 4. Kafka

### 4.1 Design we follow

Kafka runs in **local Docker** (`swiftpay-kafka`). Services on the host use `KAFKA_BOOTSTRAP=localhost:9092`.

| Topic | Producer | Consumer | Purpose |
|-------|----------|----------|---------|
| `payment-initiated` | payment-gateway | ledger-service | Start transfer |
| `payment-completed` | ledger-service | (future) | Success notification |
| `payment-failed` | ledger-service | (future) | Failure notification |
| `payment-failed-dlq` | error handler | manual | Exhausted retries |

**Gateway flow (current):**

```
INSERT PENDING → COMMIT → [afterCommit] → kafkaTemplate.send("payment-initiated")
```

**Ledger flow:**

```
@KafkaListener → LedgerService.transfer() → emit completed/failed → manual ack
```

### 4.2 Before optimization

| Aspect | Before | Impact |
|--------|--------|--------|
| Topic partitions | Often **1** (auto-created) | Only one consumer lane — ledger falls behind |
| Listener concurrency | **1** | One transfer at a time |
| Producer `acks` | **`all`** + idempotence | Higher latency on produce path |
| Emit timing | Inside `@Transactional`, before commit (async send) | Side effects before commit guaranteed; extra coupling |
| Batching | Defaults | At low RPS, many small individual sends |

When the gateway accepted **250 RPS** but ledger processed **~1 message at a time**, Kafka lag and DB row-lock contention grew — hurting tail latency everywhere.

### 4.3 What we optimized

| Change | Purpose |
|--------|---------|
| **6 partitions** on `payment-initiated` | Parallel consumption (`docs/scripts/prepare-kafka-topics.ps1`) |
| **`spring.kafka.listener.concurrency: 3`** | Up to 3 parallel transfers (needs ≥3 partitions) |
| **`KAFKA_PRODUCER_ACKS=1`** (default) | Faster producer than `all` |
| **`linger.ms=5`**, `batch.size=16384` | Batch messages under steady load |
| **Emit after DB commit** | Shorter critical path; no events for rolled-back txs |
| **Tomcat `threads.max: 200`** (gateway) | More concurrent HTTP workers |

For maximum durability (slower), set in `.env`:

```properties
KAFKA_PRODUCER_ACKS=all
KAFKA_PRODUCER_IDEMPOTENCE=true
```

### 4.4 Why speed improved (Kafka)

1. **Gateway 202 path** — producer returns faster (`acks=1`, batching); commit completes before side effects.
2. **Ledger throughput** — ~3× parallel consumers drain `payment-initiated` faster → less backlog → less shared DB pressure over time.
3. **Smoke tests** — at 10/50 RPS the ledger keeps up; latency distributions tightened (e.g. 50 RPS p95 **~61 ms**, 0% errors).

**250 RPS sustained:** Kafka local is fine. **Previous:** ~946k requests, p95 **~1.75 s**, 58.9k dropped iterations. **Latest:** **~999.8k** requests, p95 **~111 ms**, **191** dropped — throughput at target; tune Supabase/partitions if tail latency regresses on remote DB only.

---

## 5. End-to-end: one payment

### 5.1 Gateway (synchronous — what k6 measures)

```
1. Redis     EXISTS idempotency key          (local, ~µs–ms)
2. Redis     GET balance                     (hit: local | miss: Postgres ~50–100+ ms)
3. Postgres  INSERT sp_transactions PENDING  (remote Supabase)
4. Postgres  COMMIT
5. [afterCommit] Kafka produce payment-initiated (async, local broker)
6. [afterCommit] Redis SET idempotency       (local)
7. HTTP 202 Accepted
```

**Typical median after tuning:** ~50–80 ms (depends on cache hit and Supabase RTT).

### 5.2 Ledger (asynchronous)

```
1. Consume PaymentInitiatedEvent from payment-initiated
2. BEGIN transaction
3. SELECT FOR UPDATE on two accounts (ordered UUIDs)
4. UPDATE balances; UPDATE transaction status
5. COMMIT
6. Publish payment-completed or payment-failed; ack offset
```

Does **not** block the client’s 202.

---

## 6. Load-test results (summary)

| Run | Target | Requests | Error rate | p50 | p95 | Notes |
|-----|--------|----------|------------|-----|-----|-------|
| Smoke | 10/s | 600–601 | 0.00% | ~80 ms | ~100–128 ms | Latest re-runs |
| Stress | 50/s | 3001 | 0.00% | ~50 ms | ~61 ms | Full minute, 0 dropped |
| Full | 250/s × 1M | **999,809** | 0.01% | ~56 ms | **~111 ms** | ✓ all k6 thresholds |
| Full (prior) | 250/s × 67 min | 946,087 | 0.01% | ~61 ms | ~1.75 s | ✓ errors; ✗ p95 &lt; 500 ms |

Details and commands: [PERFORMANCE.md](PERFORMANCE.md).

### Why 50 RPS can look faster than 10 RPS

Both runs are **after** optimization. Higher **steady** load can show **better** p50/p95 because:

1. **JVM / Hikari / Kafka** stay warm (JIT, pooled connections, full batches).
2. **Redis balance cache** stays hot for repeated sender IDs in k6 scripts.
3. **10 RPS** may include **rare multi-second outliers** (Supabase blip, GC) — one spike raises p95; **50 RPS** run had max ~145 ms.
4. This is **not** linear — an earlier **250 RPS** full run had **~1.75 s** p95; the latest **~1M** run at **250 RPS** achieved **~111 ms** p95 (see [PERFORMANCE.md](PERFORMANCE.md)).

```
Latency (conceptual)

  high │                              ╱ prior 250 RPS tail (DB)
       │         outliers at very low RPS
       │        ╱
       │   sweet spot (10–50 RPS on laptop)
       │  ╱‾‾‾‾╲
  low  │          ╲___
       └────────────────────────► load
```

---

## 7. Before / after checklist

| Layer | Before | After | Main effect |
|-------|--------|-------|-------------|
| **DB** | Prepared stmts + pooler → 500s; pool 10 | `prepareThreshold=0`, pool 30 | **Stable commits**, ~0% errors |
| **Kafka** | 1 partition, 1 consumer, acks=all | 6 partitions, 3 consumers, acks=1, batching, afterCommit | **Faster 202**, faster settlement |
| **Redis** | Local; same keys/TTL | Idempotency after commit | **Correctness** + cleaner request path |
| **HTTP** | Default Tomcat threads | max 200 | More concurrent accepts |

---

## 8. Operations quick reference

**Supabase:** Transaction pooler, JDBC, port **6543**, with `prepareThreshold=0`.

**Prepare Kafka topics (once):**

```powershell
cd D:\hackathon\Swift-pay
powershell -ExecutionPolicy Bypass -File .\docs\scripts\prepare-kafka-topics.ps1
```

**Verify partitions:**

```powershell
docker exec swiftpay-kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic payment-initiated
```

**Inspect Redis:**

```powershell
docker exec -it swiftpay-redis redis-cli
KEYS swiftpay:*
```

**Restart** payment-gateway and ledger-service after any `application.yml` or `.env` change.

---

## 9. Reviewer talking points

1. **Pattern:** Synchronous **accept** (202) + asynchronous **settle** (Kafka + ledger) — standard for payment platforms.
2. **Postgres** is the **source of truth**; Redis is **acceleration**; Kafka is **decoupling and durability**.
3. **Critical fix:** Supabase **transaction pooler** requires disabling server-side prepared statements — explained our early 500s and fix.
4. **Throughput fix:** Kafka **partitions + consumer concurrency** so ledger is not single-threaded.
5. **Honest limit:** **~999.8k** payments at **~250 RPS** with **0.01%** errors and **~111 ms** p95 (latest); prior **~946k** run at **~235 RPS** with **p95 ~1.75 s** documents when remote DB / single-consumer ledger was the bottleneck.

---

*Last updated from k6 runs and code in repo (payment-gateway + ledger-service `application.yml`, `PaymentService`, `KafkaConsumerConfig`).*
