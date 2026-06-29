# Blog images (SVG — ready to embed or export as PNG)

| File | Use in post |
|------|-------------|
| `swiftpay-architecture.svg` | Hero — after Key engineering lessons |
| `pgbouncer-failure-timeline.svg` | PgBouncer section |
| `k6-summary-250rps.svg` | Load test section (metrics from PERFORMANCE.md) |
| `load-test-p95-comparison.svg` | After k6 summary |
| `kafka-topic-flow.svg` | Architecture section |
| `redis-keys.svg` | Architecture section |

**Before publishing on raghavdev.in:** export PNG/WebP if your CMS does not serve SVG. Replace k6 terminal with a real screenshot from your latest run if you have `load-test/results/*.json`. Optional: Kafka UI screenshot can supplement `kafka-topic-flow.svg`.

**Generate PNG (optional):**

```powershell
# Inkscape CLI example
inkscape docs/images/swiftpay-architecture.svg -o docs/images/swiftpay-architecture.png
```
