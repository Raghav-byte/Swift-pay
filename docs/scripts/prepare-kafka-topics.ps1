# Run once before high-throughput load tests (6 partitions for parallel ledger consumers).
# Requires swiftpay-kafka container from docker compose.

$bootstrap = "localhost:9092"

$topics = @(
    @{ Name = "payment-initiated"; Partitions = 6 },
    @{ Name = "payment-completed"; Partitions = 6 },
    @{ Name = "payment-failed"; Partitions = 3 },
    @{ Name = "payment-failed-dlq"; Partitions = 1 }
)

foreach ($t in $topics) {
    docker exec swiftpay-kafka kafka-topics `
        --bootstrap-server $bootstrap `
        --create `
        --if-not-exists `
        --topic $t.Name `
        --partitions $t.Partitions `
        --replication-factor 1
    Write-Host "Topic $($t.Name) -> $($t.Partitions) partitions"
}

# If payment-initiated already exists with 1 partition, increase (cannot decrease):
docker exec swiftpay-kafka kafka-topics `
    --bootstrap-server $bootstrap `
    --alter `
    --topic payment-initiated `
    --partitions 6 2>$null

Write-Host "Done. Verify: docker exec swiftpay-kafka kafka-topics --bootstrap-server $bootstrap --describe --topic payment-initiated"
