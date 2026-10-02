# Capability boundaries and justified deviations

This is a production-oriented local platform, not a production deployment certification. Compose is the full supported operational path; the secondary kind path is limited as described in kubernetes.md.

- PostgreSQL fault is network interruption through Toxiproxy, preserving control-plane persistence; it does not simulate server loss, WAL corruption or disk failure. Inventory fault is a network-unavailable service. Worker fault actually stops/restarts its container.
- Backups cover PostgreSQL only, use one latest retained artifact, and restore into isolation. RTO/RPO are explicitly scoped local measurements. Cross-broker coordinated snapshots, PITR, encryption and automated restore-to-application reconciliation are not implemented.
- Metrics use Prometheus native instrumentation; OpenTelemetry instruments traces in every Go service. No fake samples are substituted. Short windows can miss sparse events; unavailable evidence is expected.
- Reconciliation batches are bounded but a long-lived unresolved order can keep retrying on later passes; circuit breaking prevents rapid storms. There is no operator-configurable dead-letter retry budget persisted per message.
- Local runner is isolated and operation allowlisted but internally holds full Docker-engine privileges. No authentication/authorization, TLS, secret manager, multi-tenancy or remote operation is provided. Loopback bindings and internal runner networking are mandatory local safeguards.
- Single database and broker instances, low load, no HA/failover; no external payment provider. Redis is a best-effort catalog cache and intentionally falls back. Catalog prices are seeded, integer cents; this workload omits tax, shipping and multi-line carts.
- Worker state is an idempotent delivery projection, not real shipping. A published event may replay after publisher crash. DLQ entries require explicit operator replay/inspection.
- Experiment interruption is recorded ABORTED and cleaned on control restart; durable jobs are not resumed. Runner watchdog is in memory. Combined process/host failures can require manual cleanup.
- Order list/history endpoints are bounded to recent 100 rows, with no pagination/search. UI is deliberately functional and minimal. Metrics refresh every five seconds; SSE indicates connectivity and service events.
- Schema initialization is an initial migration for new volumes, not an online migration manager. CI validates builds and limited tests; integration demo is explicitly executed locally, not automatically a large CI chaos suite.
- Container/library versions are pinned. A frontend npm audit was checked during implementation; this does not imply all container dependencies have passed a comprehensive security assessment.

Executed validation is recorded separately in validation.md. Implemented behavior must not be confused with behavior exercised successfully in this environment.
