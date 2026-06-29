# Performance

Load tests for SwiftPay **payment-gateway** (`POST /v1/payments` → **202 Accepted**).  
Tool: [k6](https://k6.io/) via Docker (`docker compose --profile loadtest`).  
Target URL: `http://host.docker.internal:8080` (gateway on host port **8080**).

**Environment:** Windows host, Supabase Postgres (transaction pooler `:6543`), local Kafka + Redis, Hikari `maximum-pool-size=30` on both services.

**Architecture & optimization (Redis, Kafka, DB):** [ARCHITECTURE_AND_OPTIMIZATION.md](ARCHITECTURE_AND_OPTIMIZATION.md).

**Fixes in effect for all runs below:** `prepareThreshold=0`, `hibernate.jdbc.use_server_prepared_stmts=false` (Supabase PgBouncer compatibility).

---

## Summary

| Run | Script | Duration | Target rate | Completed HTTP | Achieved throughput | Error rate | p50 latency | p95 latency | Pass |
|-----|--------|----------|-------------|----------------|---------------------|------------|-------------|-------------|------|
| Smoke | `payment-load-test-smoke.js` | 1 min | 10/s | 600–601 | ~10.0/s | **0.00%** | ~80 ms | ~100–128 ms | ✓ thresholds |
| Stress | `payment-load-test-smoke.js` + `K6_RATE=50` | 1 min | 50/s | 3001 | ~50.0/s | **0.00%** | ~50 ms | ~61 ms | ✓ thresholds |
| Full | `payment-load-test.js` + `K6_TARGET_REQUESTS=1000000` | **1h 6m 40s** | 250/s | **999,809** | ~**250.0/s** avg | **0.01%** | ~56 ms | ~**111 ms** | ✓ all thresholds |

**Full load — previous vs latest (same 250 RPS target):**

| Metric | Previous run | Latest run (2026-05-25) | Change |
|--------|----------------|-------------------------|--------|
| Completed HTTP | 946,087 | **999,809** | **+53,722** (~99.98% of 1M) |
| Scenario duration | ~67 min | **1h 6m 40s** (`K6_TARGET_REQUESTS=1M`) | Sized for 1M at 250/s |
| Dropped iterations | 58,914 | **191** | **−58,723** |
| Achieved throughput | ~234.7/s | **~250.0/s** | **+~15/s** (hits target) |
| `http_req_failed` | 0.01% (121) | 0.01% (109) | Similar |
| p50 | ~61 ms | **~56 ms** | ~8% lower |
| p90 | ~662 ms | **~77 ms** | **~8.6× lower** |
| p95 | **~1.75 s** | **~111 ms** | **~16× lower** |
| max | ~33.1 s | **~6.2 s** | Lower tail outliers |
| k6 `p(95)<500` | ✗ Fail | **✓ Pass** | Tail latency within goal |

---

## 1. Smoke — 10 RPS

**Command:**

```powershell
docker compose --profile loadtest run --rm --no-deps `
  -e BASE_URL=http://host.docker.internal:8080 `
  k6 run --out json=/results/smoke-10rps.json /scripts/payment-load-test-smoke.js
```

**Date:** 2026-05-25 (latest re-runs; gateway on host :8080, Docker Kafka/Redis/Postgres)

| Metric | Value |
|--------|-------|
| Target rate | 10 iter/s |
| Completed HTTP requests | **600–601** |
| Achieved throughput | ~10.0/s |
| Dropped iterations | 0 |
| Checks (`status is 202`) | **100%** |
| `http_req_failed` | **0.00%** |
| p50 (`http_req_duration`, median) | **~80 ms** |
| p90 | ~94–106 ms |
| p95 | **~101–128 ms** |
| max | ~1.1–1.2 s |
| max VUs | 15 |

**Notes:** vs earlier 10 RPS run (591 req, p95 ~143 ms): similar completion at target rate, **lower p95** on latest re-run (~101 ms best). All completed requests returned 202.

---

## 2. Stress smoke — 50 RPS

**Command:**

```powershell
docker compose --profile loadtest run --rm `
  -e K6_RATE=50 -e K6_PRE_ALLOCATED_VUS=50 -e K6_MAX_VUS=100 `
  k6 run --out json=/results/smoke-50rps.json /scripts/payment-load-test-smoke.js
```

**Date:** 2026-05-25 (re-run after Kafka 6 partitions + gateway/ledger perf tuning)

| Metric | Value |
|--------|-------|
| Target rate | 50 iter/s |
| Completed HTTP requests | 3001 |
| Achieved throughput | ~49.98/s |
| Dropped iterations | 0 |
| Checks (`status is 202`) | **100%** (3001/3001) |
| `http_req_failed` | **0.00%** |
| p50 (`http_req_duration`, median) | **~50 ms** |
| p90 | ~57 ms |
| p95 | **~61 ms** |
| max | 145.48 ms |
| max VUs allocated | 50 (peak active ~4) |

**Notes:** vs earlier 50 RPS run (2944 req, 57 dropped, p95 ~658 ms): **full 3000+ load**, **zero drops**, **~10× lower p95**.

---

## 3. Full submission load — 250 RPS (~1M requests)

**Command:**

```powershell
docker compose --profile loadtest run --rm --no-deps `
  -e BASE_URL=http://host.docker.internal:8080 `
  -e K6_TARGET_REQUESTS=1000000 -e K6_RATE=250 `
  k6 run --summary-export=/results/load-test-1m-summary.json `
  --out json=/results/load-test-results.json /scripts/payment-load-test.js
```

`K6_TARGET_REQUESTS` sets scenario duration to `ceil(target / rate)` seconds (1M ÷ 250 = **4000s** = **1h 6m 40s**). Override with `K6_DURATION` when `K6_TARGET_REQUESTS` is unset (default **72m**).

**Date:** 2026-05-25 — wall time **~1h 06m 40s**

| Metric | Previous | Latest |
|--------|----------|--------|
| Target rate | 250 iter/s | 250 iter/s |
| Scenario duration | ~67 min | **1h 6m 40s** |
| Completed HTTP requests | 946,087 | **999,809** |
| Dropped iterations | 58,914 | **191** |
| Achieved throughput (avg) | ~234.7/s | **~250.0/s** |
| Checks (`status is 202`) | 99% | **99.98%** (999,700 / 999,809) |
| `http_req_failed` | 0.01% (121) | **0.01%** (109) |
| p50 (`http_req_duration`, median) | ~61 ms | **~56 ms** |
| p90 | ~662 ms | **~77 ms** |
| p95 | ~1.75 s | **~111 ms** |
| max | 33.13 s | **~6.2 s** |
| max VUs | 500 | 491 |
| Data sent / received | ~333 MB / ~257 MB | **~348 MB / ~272 MB** |

### k6 thresholds (`payment-load-test.js`)

| Threshold | Previous | Latest |
|-----------|----------|--------|
| `http_req_failed` &lt; 1% | **Pass** (0.01%) | **Pass** (0.01%) |
| `http_req_duration` p95 &lt; 500 ms | **Fail** (p95 ≈ 1.75 s) | **Pass** (p95 ≈ 111 ms) |

**Interpretation:** Latest run completed **~999.8k** payments at **~250 RPS** with only **191** dropped iterations (vs **58.9k** dropped and **~235 RPS** on the prior full run). Median latency is similar (~56 vs ~61 ms); **p90/p95 dropped sharply** (~662 ms / ~1.75 s → ~77 ms / ~111 ms), so the gateway path stayed within the script’s 500 ms p95 goal. Remaining **109** failures are **0.01%** of requests (same order of magnitude as before). **191** short of exactly 1M is scheduler drop at scenario end, not sustained overload.

**Previous run (for comparison):** ~946k over ~67 min, **58.9k** dropped iterations, p95 **~1.75 s** — ledger/Kafka/DB contention under remote Supabase and single-threaded consumer limits (see [ARCHITECTURE_AND_OPTIMIZATION.md](ARCHITECTURE_AND_OPTIMIZATION.md)).

---

## How to improve performance further

Your stack: **local Redis + Kafka**, **remote Supabase transaction pooler** (correct for many connections; keep `prepareThreshold=0`).

### Do these first (code + one-time setup)

| Priority | Action | Why |
|----------|--------|-----|
| 1 | **Restart both apps** after pulling latest config | Picks up Kafka/Tomcat/after-commit changes |
| 2 | **6 Kafka partitions** on `payment-initiated` | Ledger uses 3 consumer threads; 1 partition = only 1 thread works |
| 3 | Run `docs/scripts/prepare-kafka-topics.ps1` | Creates/alters topics with enough partitions |
| 4 | Keep **transaction pooler** in Supabase UI (your screenshot) | Good for gateway + ledger connection churn; do **not** switch to direct URL without changing pool settings |

```powershell
cd D:\hackathon\Swift-pay
.\docs\scripts\prepare-kafka-topics.ps1
```

### Already applied in the repo

| Change | Effect on 202 path |
|--------|-------------------|
| Kafka emit **after DB commit** | Shorter request thread hold; no events for rolled-back txs |
| `KAFKA_PRODUCER_ACKS=1` (default) | Faster producer than `acks=all` |
| `linger.ms=5` + batching | Fewer small Kafka packets under load |
| Ledger **listener concurrency = 3** | Up to 3 parallel transfers (needs ≥3 partitions) |
| Tomcat **200** threads (gateway) | More concurrent accepts |

For **maximum durability** (slower): in `.env` set `KAFKA_PRODUCER_ACKS=all` and `KAFKA_PRODUCER_IDEMPOTENCE=true`.

### Supabase (biggest remaining latency)

| Option | Trade-off |
|--------|-----------|
| Stay on **transaction pooler :6543** + `prepareThreshold=0` | Best connection count; ~50–80 ms+ per query to `ap-south-1` |
| **Session pooler** (port **5432** on pooler host) | Slightly better for prepared statements; fewer concurrent server connections |
| **Direct** connection (IPv6 / add-on) | Lowest DB latency; limited connections — not ideal for 2 services × 30 pool |

You cannot move Postgres local without changing the architecture; **tune app + Kafka + ledger parallelism** first.

### Optional next steps

- Lower **Hikari** to **20** per service if Supabase shows connection limits (60 total at 30+30).
- Warm **balance cache**: a few payments per seed account before k6.
- Relax k6 threshold `p(95)<500` in `payment-load-test.js` if reporting remote DB realistically (or keep as stretch goal).
- Latest **~250 RPS × 1M** run hit target throughput on one laptop; if p95 regresses with remote Supabase only, add gateway/ledger instances or tune pooler/partitions per § above.

### Re-test

```powershell
docker compose --profile loadtest run --rm -e K6_RATE=50 -e K6_PRE_ALLOCATED_VUS=50 -e K6_MAX_VUS=100 k6 run /scripts/payment-load-test-smoke.js
```

Compare p95 and error rate to the tables above.

---

## Bottleneck history

| Issue | Symptom | Fix |
|-------|---------|-----|
| Supabase transaction pooler + prepared statements | `prepared statement "S_xx" does not exist`, ~40% HTTP 500 at 10–50 RPS | `prepareThreshold: 0`, `use_server_prepared_stmts: false` |
| Connection pool (earlier tuning) | Pool wait under load | `HIKARI_MAX_POOL_SIZE=30`, `HIKARI_MIN_IDLE=5` |

---

## How to reproduce

1. `docker compose up -d` — Kafka, Redis, Zookeeper  
2. Run `docs/seed.sql` in Supabase  
3. `.env` at repo root (`DB_URL`, `REDIS_HOST`, `KAFKA_BOOTSTRAP`, `HIKARI_MAX_POOL_SIZE=30`)  
4. Start **payment-gateway** (8080) and **ledger-service** (8081)  
5. Run commands in sections 1–3 above  

See [README.md — Load test](../README.md#load-test-step-21) for step-by-step commands.

---

## Artifacts

| Run | JSON output | PCAP |
|-----|-------------|------|
| 10 RPS | `load-test/results/smoke-10rps.json` | [`load-test/results/RPS10.pcapng`](../load-test/results/RPS10.pcapng) (in repo) |
| 50 RPS | `load-test/results/smoke-50rps.json` (if saved) | — |
| 250 RPS / ~1M | `load-test/results/load-test-results.json`, `load-test-1m-summary.json` | Not committed (multi‑GB if unfiltered); see [LOAD_TEST_PCAP.md](LOAD_TEST_PCAP.md) |
