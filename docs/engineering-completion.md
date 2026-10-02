# Engineering completion checkpoint — 2026-10-03

The existing implementation was inspected before changes: six Go services and shared middleware/client, PostgreSQL initialization/seeds, Redis fallback, RabbitMQ publication/consumption/replay, dashboard routes, Compose/networking, generated kind resources/scripts, k6, telemetry provisioning, recovery scripts, CI and runbooks.

| Area | Existing implementation | Corrections in this checkpoint |
| --- | --- | --- |
| Transactions | Persisted saga; unique request/operation IDs; atomic stock reservation/release; deterministic payment; ambiguous outcome reconciliation | Dispose of a database session if saga advisory unlock fails; rollback outbox transactions on all publication failures |
| Resilience | Bounded dependency attempts, backoff/jitter, timeouts, concurrency gate and half-open circuits | Expected 4xx responses no longer count as dependency outages; focused regression verifies open and recovery behavior |
| Messaging | Durable queues/messages, confirms, transactional outbox, manual acknowledgements, idempotent delivery and DLQ | Recovery invokes bounded replay and checks missing worker deliveries rather than accepting terminal orders alone |
| Chaos | Six allowlisted real proxy/container controls, watchdog cleanup, persisted phases and measured Prometheus results | Read back injected configuration; reject zero-latency experiments; persist injection evidence; generate recovery traffic then quiesce before verification |
| Consistency/recovery | Snapshot-consistent backup, checksum, isolated restore, watermark RPO and measured restore duration | Check payment amount/reservation payload matches, cancelled payment state, pending publication, active orphan reservations and delivery gaps; released diagnostic reservations remain informational |
| Dashboard | Six operational views, real requests, SSE, reports and trace links | Preserve successful responses when another endpoint fails; expose bounded dead-letter replay |
| CI | Go/frontend builds and Compose build | Add one bounded local integration demonstration, evidence upload and cleanup |
| Deployment | Compose primary; kind application/storage/probes and manual fault scripts | Existing kind limitations remain explicit; kind runtime is unavailable locally |

The system uses one PostgreSQL server with schema-owned services and a local simulated payment provider. Workers record durable delivery projections. Those are intentional workload boundaries. No frontend redesign, additional cloud infrastructure or architectural replacement was performed.

Fresh runtime results are recorded in validation.md. Existing historical evidence is distinguished from this checkpoint. Local PostgreSQL restore is not coordinated broker recovery or multi-region disaster recovery. CI execution on GitHub and kind runtime require separate verification.
