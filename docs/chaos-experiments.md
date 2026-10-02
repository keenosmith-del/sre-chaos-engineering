# Chaos execution

An asynchronous job persists CREATED, PREPARING, BASELINE, INJECTING, OBSERVING, RECOVERING, VERIFYING and a terminal COMPLETED/FAILED/ABORTED state. A partial unique index admits one active destructive experiment. State transitions and query evidence are persisted. A control restart cleans resources and marks interrupted jobs ABORTED; jobs are not silently resumed.

| Preset | Target / mechanism | Expected measurable effect |
| --- | --- | --- |
| inventory_unavailable | Disable inventory HTTP proxy | Catalog/reservation errors, retries, pending orders or 5xx, reduced completion |
| payment_latency | Deterministic downstream latency toxic, 0..3000 ms | Longer latency, ambiguous outcomes, reconciliation, circuit activity |
| redis_outage | Disable Redis proxy | Catalog fallback counter; transactions can continue through inventory API |
| rabbitmq_interruption | Disable AMQP proxy | Outbox/backlog grows, confirms fail; delivery resumes after reconnect |
| worker_restart | Stop only the managed worker container, then start | Durable queue accumulates and is later consumed |
| postgres_interruption | Disable application PostgreSQL proxy | SQL failures/readiness loss; control DB connection remains available |

PostgreSQL interruption is a dependency network outage, not database disk corruption or process termination. Inventory unavailable also means application access unavailable while its process remains live. Worker uses an actual controlled Docker stop/start. Traffic genuinely crosses each affected proxy. Request accepted states are distinct from completed orders, so faults can increase outstanding work without producing only HTTP errors.

Parameters: duration 5..120 seconds; baseline/recovery 10..120 seconds; magnitude 0..3000 ms; stop_error_rate .01..1. Every run automatically starts a small sustained k6 profile. If the measured infrastructure error fraction exceeds the stop threshold, the run is aborted and cleaned. An error rate of 1 allows observation of complete failure. Cleanup removes toxics, reenables proxies and starts the worker. Runner watchdog also cleans after duration + 5 seconds if the API job dies. The control cleanup deadline is bounded; errors are reported rather than suppressed. A failed verification fails the experiment.

If the runner itself is killed while a fault is active, its in-memory watchdog is lost. Restart control/runner and use Recover to restore resources. Engine compromise and abrupt simultaneous runner/control loss are outside automatic cleanup guarantees. No arbitrary operation arguments are accepted.

To replay recoverable dead-lettered messages use `make replay-deadletters`; malformed payloads will be dead-lettered again, so inspect them in the RabbitMQ management UI first.
