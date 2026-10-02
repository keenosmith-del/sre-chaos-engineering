# Bounded demonstration

1. Run make up and make demo. The latter creates real transactions and one measured outage, restores a real dump, and writes artifact JSON locally.
2. Open http://localhost:5175. Overview shows readiness and actual metrics. Newly started Prometheus windows can show unavailable until sufficient scrape samples exist.
3. Transactions: create a mouse order, inspect confirmed state and reservation/payment records, follow its Jaeger trace. Retry the same key: one order. Change the payload with the old key: conflict. Use a new key with deterministic decline: cancellation and reservation release.
4. Experiments: use payment_latency, magnitude 1500, duration 15, baseline/recovery 20, stop fraction 1. Observe actual state transitions and metrics. Wait for cleanup/reconciliation. Do not launch concurrent destructive experiments; the database rejects that.
5. Telemetry: compare completion rates and gateway latency; inspect retry/circuit/outbox/queue evidence. Business declines do not count as HTTP infrastructure failures. Inspect Grafana and Jaeger for more detail.
6. Recovery: create backup while transactions are quiescent, then restore and verify. Observe the isolated database watermark, checksum and objective comparison. This does not recover RabbitMQ.
7. Reports: download JSON or open HTML to review measurement phases, PromQL, verification and errors. Missing data remains labeled.
8. Run make down when finished. It retains named volumes and reports. No external services are required.
