#!/usr/bin/env bash
# Start PCAP capture while k6 runs in another terminal.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUT="$ROOT/load-test/results/swiftpay-load.pcap"
mkdir -p "$(dirname "$OUT")"

echo "Capture (sudo) — stop with Ctrl+C after k6 finishes:"
echo "  sudo tcpdump -i lo -w \"$OUT\" 'tcp port 8080'"
echo ""
echo "Then run k6:"
echo "  docker compose --profile loadtest run --rm k6 run /scripts/payment-load-test-smoke.js"
echo ""
echo "Open in Wireshark; filter: http.request.uri contains \"/v1/payments\""
