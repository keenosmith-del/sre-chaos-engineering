# Executed validation

Executed locally on 2026-10-02 (Africa/Johannesburg), with Docker Desktop arm64. The primary stack remains running on localhost:5175.

## Build and configuration

- make check passed: Go formatting, focused state-transition test, all six Go builds, TypeScript/Vite build, Compose configuration and Python syntax.
- Native macOS TypeScript/Vite build also passed. Container checks isolate Linux node_modules in a named volume.
- npm audit reported zero vulnerabilities after compatible Vite/PostCSS patches. This is not a full container/Go security audit.
- make up passed with the final configuration; all required containers run. Business services, PostgreSQL, Redis and RabbitMQ have passing configured health checks.
- Kubernetes manifests generated successfully; shell/Python syntax checked. kubectl exists, kind is not installed, so kind deployment/self-healing was not executed.

## Real workflows

- Four concurrent identical order submissions returned one durable ID. Reusing that key with changed quantity returned HTTP 409.
- A real successful order reached CONFIRMED. Deterministic payment decline reached CANCELLED, released its reservation and restored stock.
- Gateway/order/inventory/payment/worker spans were retrieved from Jaeger for one real order (eight spans, all five services).
- Rendered dashboard inspection showed all six screens in navigation, connected SSE and real service/metric values. This was a short browser check, not extensive browser automation.
- Bounded acceptance demo passed twice after runtime fixes; it generated stored reports and verified isolated restore.
- JSON reports, readable HTML phase-comparison report, and persisted load-run history endpoints were checked.

## Faults

| Preset | Actual terminal result |
| --- | --- |
| inventory_unavailable | COMPLETED; cleanup and zero business invariant violations |
| payment_latency | COMPLETED; cleanup and zero business invariant violations |
| redis_outage | COMPLETED; cleanup and zero business invariant violations |
| rabbitmq_interruption | COMPLETED; cleanup and zero business invariant violations |
| worker_restart | COMPLETED; cleanup and zero business invariant violations |
| postgres_interruption | COMPLETED; cleanup and zero business invariant violations |

Inventory report `b9a6c2aa628b8c328a970c146510cb83` measured baseline infrastructure error fraction 0.000, failure 1.000, recovery 0.000. Completion was zero during failure and resumed after recovery. Some short-window metrics are unavailable, preserved explicitly in report evidence.

A focused worker rerun after the sampler correction measured queue depth 0 -> 12 -> 0. Durable backlog now refreshes independently instead of retaining a previous phase's sample.

## Final isolated recovery

make backup and make restore-verify passed against quiescent final data: SHA-256 verified, exported snapshot counts/watermark matched, restored invariants passed. Restored order count: 431. RTO restore-drill duration: 1.594 seconds; order-watermark RPO: 0.000 seconds. Configured local objectives were met.

RTO is an isolated PostgreSQL restore drill; RPO uses the documented order-creation watermark. This is not coordinated RabbitMQ recovery or a production service-recovery guarantee.

Reports and detailed evidence are persisted in control PostgreSQL and under ignored artifacts/*.json. The backup artifact is in the backups named volume. Dead-letter replay returned replayed:0 against the empty queue; recovery of a nonempty poison/crash queue was not exercised. Long soak tests, exhaustive failures, kind runtime, production HA/security and coordinated recovery were not tested.
