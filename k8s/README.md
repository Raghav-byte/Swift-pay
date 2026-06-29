# Kubernetes manifests (SwiftPay)

**Local development:** use root [`docker-compose.yml`](../docker-compose.yml) — it is the **canonical** way to run Postgres, Redis, Kafka, and all three services together.

These manifests are a **minimal** cluster layout for demos or hackathon “deploy to k8s” checkpoints.

## Deploy order

1. **Namespace** (optional): `kubectl create namespace swiftpay`
2. **Infra** (same namespace):
   - `postgres.yaml` — Postgres 15 + PVC (schema via init or run migrations manually)
   - `redis.yaml` — Redis 7
   - `kafka.yaml` — single-broker Kafka + Zookeeper (dev only; not HA)
3. **Config:** `configmap.yaml`, `secret.yaml` — set `DB_URL`, credentials, `KAFKA_BOOTSTRAP`, `REDIS_HOST`
4. **Apps:** `payment-gateway.yaml`, `ledger-service.yaml`, `analytics.yaml`

```bash
kubectl apply -f k8s/postgres.yaml
kubectl apply -f k8s/redis.yaml
kubectl apply -f k8s/kafka.yaml
kubectl wait --for=condition=ready pod -l app=postgres --timeout=120s
kubectl apply -f k8s/configmap.yaml -f k8s/secret.yaml
kubectl apply -f k8s/payment-gateway.yaml -f k8s/ledger-service.yaml -f k8s/analytics.yaml
```

## Production Kafka

The bundled `kafka.yaml` is intentionally small (Zookeeper + one broker). For production, prefer [Strimzi](https://strimzi.io/) or a managed Kafka service; point `KAFKA_BOOTSTRAP` in the ConfigMap/Secret at that cluster.

## External Postgres (Supabase)

Skip `postgres.yaml` and set `DB_URL` / `DB_USERNAME` / `DB_PASSWORD` in `secret.yaml` to your pooler URL (see root `README.md`).

## Health checks

Services expose both **`/v1/health`** and brief-compatible **`/health`** (and `/health/db`, `/health/kafka`, etc.).

## Images

Manifests reference `swiftpay/payment-gateway:latest` (and siblings). Build and load locally:

```bash
docker build -t swiftpay/payment-gateway:latest payment-gateway
# kind load docker-image swiftpay/payment-gateway:latest
```
