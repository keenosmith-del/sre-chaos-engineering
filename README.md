# Distributed Systems Reliability, Chaos Engineering & Disaster Recovery

A local e-commerce workload and reliability control plane. Orders use a durable saga, schema-owned PostgreSQL data, idempotent reservations/payments, a transactional outbox, and RabbitMQ delivery. The control plane runs allowlisted faults under k6 traffic, records real Prometheus evidence, verifies consistency, and restores PostgreSQL backups into an isolated recovery database.

## Start on macOS

Install Docker Desktop (Compose included), make and Python 3. Allocate at least 4 GB RAM; 6–8 GB is more comfortable. Initial image downloads and Go builds need internet access. No cloud credentials or AI API are required. Run from this repository root:

```sh
make up
make seed
make demo
```

`make up` builds and waits for the stack. `make seed` adds missing products without resetting stock. `make demo` verifies concurrent idempotency, a payload conflict, a confirmed order, payment-decline compensation, a measured inventory outage, automatic recovery, an experiment report, and an isolated backup restore. It takes roughly two minutes after startup. Its evidence is saved under `artifacts/` (ignored by Git).

Open [the dashboard](http://localhost:5175). Its six screens use actual REST responses and SSE connection status. In Transactions choose a product, submit an order, inspect its lifecycle, and open its trace. Reusing the displayed key retries the same logical transaction; choose **New key** for another purchase. Check **Deterministic decline** to exercise compensation.

In Experiments choose one of the six fixed faults and run it. The job starts laptop-sized load, observes baseline/failure/recovery, cleans up, and verifies consistency. Stop cancels the job and cleans resources; Recover also requests cleanup. Automatic recovery stops traffic, replays up to 20 dead letters, and verifies durable event delivery alongside transaction consistency. Recovery offers an explicit bounded dead-letter replay button. In Reports inspect persisted evidence or download JSON/HTML. Unavailable metrics indicate insufficient samples, never a generated value.

| Interface | Local URL |
| --- | --- |
| Dashboard | http://localhost:5175 |
| Gateway REST / SSE | http://localhost:8080/api |
| Grafana provisioned dashboard | http://localhost:3000/d/reliability |
| Prometheus | http://localhost:9090 |
| Jaeger | http://localhost:16686 |
| RabbitMQ management | http://localhost:15672 (demo / local_rabbit) |

All published ports bind to loopback. PostgreSQL, Redis, Toxiproxy, control and the privileged runner are not published. The runner alone mounts Docker's socket, on a separate administration network. Local credentials in Compose/SQL are demonstration credentials; this is not an internet-facing deployment.

## Commands

```sh
make check           # format, Go tests/build, TypeScript/Vite build, Compose, Python syntax
make backup          # snapshot-consistent pg_dump + checksum + watermark
make restore-verify  # restores latest dump to isolated recovery PostgreSQL
make logs
make down            # stop approved load; retain named data volumes
```

Example direct request:

```sh
curl -sS http://localhost:8080/api/orders -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: example-order-001' \
  -d '{"product":"mouse","quantity":1,"decline":false}'
```

Do not remove volumes unless deliberately discarding all demo state. Schema initialization runs only on a fresh PostgreSQL volume. Existing state survives `make down` / `make up`. The initial migration is not an upgrade migration framework.

## Design and operational boundaries

See [architecture](docs/architecture.md), [service contracts](docs/service-contracts.md), [transaction lifecycle](docs/transaction-lifecycle.md), [resilience](docs/reliability-patterns.md), [chaos](docs/chaos-experiments.md), [observability](docs/observability.md), [recovery](docs/disaster-recovery.md), [kind deployment](docs/kubernetes.md), [walkthrough](docs/demo-walkthrough.md), and [limitations](docs/limitations.md).

The optional kind path has namespace-scoped manual worker termination and network-fault scripts. Dashboard-triggered Compose operations and disaster-recovery automation are explicitly unavailable in kind. The primary Compose environment implements those operations.

Runtime checks and actual results are recorded in [validation](docs/validation.md). See the [engineering checkpoint](docs/engineering-completion.md) for the audit and corrections.
