# SwiftPay failure scenarios

How the system behaves under common failure modes and how operators can demo them for reviews.

**Canonical local stack:** `docker compose up -d` (Postgres, Redis, Kafka, three services). Ports: gateway **8080**, ledger **8081**, analytics **8082**.

---

## 1. Insufficient funds (HTTP 422)

**Where:** `payment-gateway` before persisting or publishing.

**Flow:**

1. Client `POST /v1/payments` with amount greater than sender balance (Redis `swiftpay:balance:{senderId}` or DB `sp_accounts`).
2. `BalanceService.validate` throws `InsufficientFundsException`.
3. Response: **422** `INSUFFICIENT_FUNDS` — no row in `sp_transactions`, no Kafka publish, no idempotency key stored.

**Demo:**

```bash
# Use a seeded sender with low balance or drain balance first
curl -s -o /dev/null -w "%{http_code}\n" -X POST http://localhost:8080/v1/payments \
  -H "Content-Type: application/json" \
  -d '{"transactionId":"<uuid>","senderId":"b0000001-0000-0000-0000-000000000001","receiverId":"b0000006-0000-0000-0000-000000000006","amount":999999.99,"currency":"INR"}'
# Expect 422
```

Ledger is not involved; no `payment-initiated` message.

---

## 2. Ledger transfer failure → `payment-failed`

**Where:** `ledger-service` consumer on `payment-initiated`.

**Flow:**

1. Gateway accepted payment (**202** `PENDING`), published `payment-initiated`.
2. `LedgerService.transfer` fails (e.g. insufficient funds if gateway check was stale, DB error, deadlock after retries).
3. Listener publishes **`payment-failed`** with transaction metadata; may still ack offset depending on exception type.
4. For business insufficient funds inside ledger: transaction row moves to **FAILED**, `payment-failed` emitted, no `payment-completed`.

**Inspect:**

```bash
# Ledger logs: "payment-failed" publish
# Kafka (inside container):
docker exec swiftpay-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic payment-failed --from-beginning --max-messages 5
```

---

## 3. Kafka consumer retry and DLQ (`payment-failed-dlq`)

**Where:** `ledger-service` `KafkaConsumerConfig` — `DefaultErrorHandler` + `DeadLetterPublishingRecoverer`.

**Flow:**

1. Transient errors (e.g. `RetryableLedgerException`, serialization) → Spring Kafka retries with backoff.
2. After retries exhausted → record copied to topic **`payment-failed-dlq`** (partition 0).
3. Original offset committed so the consumer does not block the partition indefinitely.

**Operator check:**

```bash
docker exec swiftpay-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic payment-failed-dlq --from-beginning --max-messages 10
```

---

## 4. Duplicate transaction (HTTP 409)

**Where:** `IdempotencyService` — Redis `swiftpay:idempotency:{transactionId}` (~24h TTL).

**Flow:**

1. First `POST` with same `transactionId` succeeds (**202**).
2. Second `POST` with same `transactionId` → **409** `DUPLICATE_TRANSACTION` before balance check.

**DB angle:** `sp_transactions.transaction_id` is unique; a race without Redis would still fail on insert, but gateway rejects earlier via Redis.

---

## 5. Database constraint / idempotency at persistence

**Where:** Gateway insert into `sp_transactions` with unique `transaction_id`.

If Redis idempotency were bypassed, duplicate insert hits unique constraint → handled as server/duplicate error path (gateway should prevent via Redis first).

---

## 6. Kafka broker outage (operator demo)

### Setup

```bash
docker compose up -d
# Confirm health
curl -s http://localhost:8080/v1/health/kafka
curl -s http://localhost:8080/health/kafka   # alias
```

### Stop Kafka

```bash
docker stop swiftpay-kafka
```

### Expected behavior

| Component | Behavior |
|-----------|----------|
| **Gateway `GET /health/kafka`** | **503** DOWN (AdminClient cannot reach cluster) |
| **`POST /v1/payments`** | May still return **202** if DB/Redis UP — publish uses `KafkaTemplate`; send can fail asynchronously or block depending on producer config; monitor logs for send failures |
| **Ledger consumer** | Cannot poll; consumer lag grows; no new `payment-completed` |
| **Analytics consumer** | Same — no new events in `sp_analytics_events` |

### Restart

```bash
docker start swiftpay-kafka
# Wait ~10–30s
curl -s http://localhost:8080/v1/health/kafka
```

Consumers resume from last committed offset; backlog processes in order per partition.

### Notes for demos

- Use a **new** `transactionId` per payment attempt during the outage.
- Show **health** before/after stop: `/health`, `/health/kafka`, and `/v1/health/*` (both alias families work).
- Pair with ledger logs or `kafka-consumer-groups --describe` to show lag recovery.

---

## 7. Unsupported currency (HTTP 422)

`currency` must be **`INR`**. Any other value → **422** `UNSUPPORTED_CURRENCY` before idempotency or balance checks.

---

## Quick reference

| Scenario | HTTP / topic | Service |
|----------|----------------|---------|
| Insufficient funds (pre-check) | 422 | payment-gateway |
| Duplicate `transactionId` | 409 | payment-gateway |
| Non-INR currency | 422 | payment-gateway |
| Ledger failure | `payment-failed` | ledger-service |
| Exhausted retries | `payment-failed-dlq` | ledger-service |
| Kafka down | 503 on `/health/kafka`; async publish issues | all Kafka clients |

See also [SWIFTPAY_ARCHITECTURE.md](SWIFTPAY_ARCHITECTURE.md) §8 for the happy-path sequence.
