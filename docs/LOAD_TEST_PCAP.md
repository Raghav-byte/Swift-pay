# Load test PCAP trace

A **PCAP** (packet capture) records raw network traffic during a load test. Reviewers can open it in Wireshark to verify that HTTP requests hit `POST /v1/payments` and that responses match k6 summaries.

## Submitted trace (in repository)

| File | Run | What it shows |
|------|-----|----------------|
| [`load-test/results/RPS10.pcapng`](../load-test/results/RPS10.pcapng) | **10 RPS** smoke (~1 min) | **586** `POST /v1/payments`, **HTTP 202**; loopback capture while k6 ran `payment-load-test-smoke.js` |

**Wireshark display filter:**

```text
http.request.method == "POST" && http.request.uri contains "/v1/payments"
```

**Correlate with k6:** [`load-test/results/smoke-10rps.json`](../load-test/results/smoke-10rps.json) (same smoke scenario).

### Why not a PCAP for the full ~1M @ 250 TPS run?

The submission load is documented in [PERFORMANCE.md](PERFORMANCE.md) (~999,809 requests, ~250 req/s, ~111 ms p95). A full unfiltered capture for that run would be **several GB** (gateway **8080**, plus Redis **6379** and Kafka **9092** on loopback), which is impractical to store in Git or review in Wireshark. The **primary proof** for the 1M run is k6 JSON under `load-test/results/` (`load-test-results.json`, `load-test-1m-summary.json`).

## Capture your own trace

### What to capture

- **Interface:** loopback (`lo` / `Loopback`) when gateway runs on `localhost:8080`
- **Filter:** `tcp port 8080` (adjust if the gateway port differs)
- **Duration:** overlap with your k6 smoke or stress run (e.g. 1–5 minutes)

### Windows (Wireshark / tshark)

1. Install [Wireshark](https://www.wireshark.org/download.html) (includes `tshark`).
2. Start capture on the loopback adapter, or from an elevated PowerShell:

```powershell
mkdir -Force load-test\results
tshark -i 1 -f "tcp port 8080" -w load-test\results\swiftpay-load.pcapng
```

> Use `tshark -D` to list interface indices; pick the loopback adapter.

3. In another terminal, run k6 (gateway must be up):

```powershell
docker compose --profile loadtest run --rm k6 run /scripts/payment-load-test-smoke.js
```

4. Stop `tshark` with Ctrl+C. Open the `.pcapng` in Wireshark.
5. Display filter: `http.request.method == "POST" && http.request.uri contains "/v1/payments"`

### Linux / macOS (tcpdump)

```bash
mkdir -p load-test/results
sudo tcpdump -i lo -w load-test/results/swiftpay-load.pcap 'tcp port 8080'
```

Run k6 in another terminal, then stop tcpdump (Ctrl+C). Inspect with Wireshark or `tcpdump -r load-test/results/swiftpay-load.pcap -nn`.

### Helper script (repo)

From the repo root:

```powershell
.\load-test\scripts\capture-pcap.ps1
```

```bash
./load-test/scripts/capture-pcap.sh
```

Scripts print the exact `tshark`/`tcpdump` command and remind you to run k6 while capture is active.

## Correlating with k6

- Export k6 summary: `k6 run --summary-export=load-test/results/summary.json ...`
- PCAP proves packets on the wire; k6 proves application-level status codes (202, 422, etc.).

## Full Docker Compose stack

When all services run in Compose, capture on the host still works if port `8080` is published:

```bash
docker compose up -d
docker compose --profile loadtest run --rm k6 run /scripts/payment-load-test-smoke.js
```

Capture filter remains `tcp port 8080` on the host loopback.
