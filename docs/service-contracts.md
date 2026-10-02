# Service contracts

All public routes are under `/api` through the gateway. JSON error bodies use `{"error":"actionable explanation"}`. Request bodies are capped at 64 KiB; unknown fields and trailing JSON are rejected by business/control command handlers. Dependency responses are capped at 2 MiB. Internal service routes are not gateway-exposed.

| Public application route | Contract |
| --- | --- |
| GET /api/products, /api/inventory | Inventory-owned product list with integer prices in cents and available stock |
| POST /api/orders | `{product, quantity:1..100, decline:boolean}`; `Idempotency-Key` required, 8..128 characters; 201 initial acceptance, 200 replay, 409 changed payload; response includes durable current state |
| GET /api/orders | Most recent 100 orders |
| GET /api/orders/{id} | Durable order state |
| GET /api/transactions/{id} | Order, transitions, reservation, authorization, trace ID and dependency errors |

Acceptance may be `PENDING` or `RECONCILIATION_REQUIRED`; inspect state rather than treating every 2xx as completed. The simulated payment service deterministically declines when `decline` is true. There is no real financial processing.

| Control route under /api/control | Contract |
| --- | --- |
| GET /services | Measured readiness checks; unreachable services are unavailable |
| GET /metrics | Query/value/availability objects in a rolling 30-second window |
| GET, POST /experiments | History or `{fault,target,magnitude,duration,baseline,recovery,stop_error_rate}`; POST returns 202 with ID |
| GET /experiments/{id} | State, parameters, transitions, evidence and errors |
| POST /experiments/{id}/stop, /recover | Cancellation / explicit allowlisted cleanup |
| GET /load | Durable profile/run status and natural completion metadata |
| POST /deadletters/replay | Republish at most 20 durable dead-letter events with confirms |
| POST /load/start | `{profile,experiment_id?}`; fixed profile names only |
| POST /load/stop | Stop the managed k6 workload |
| GET /recovery | Persisted backup/restore evidence |
| POST /recovery/backup, /verify | Snapshot backup / isolated restore and verification |
| GET /reports, /reports/{id} | Terminal experiment histories / downloadable evidence; `?format=html` for readable report |
| GET /events | SSE `connected` then `update` events with actual service checks |

Services expose `/health/live`, `/health/ready`, and `/metrics`. Readiness checks SQL when the service owns a database; worker consumer connectivity is separately visible through queue metrics and processing outcomes, not implied by its SQL readiness.

Internal inventory routes are GET `/products`, POST `/reservations`, GET/DELETE `/reservations/{id}`. Payment routes are POST `/authorizations`, GET `/authorizations/{id}`. Operation IDs are stable order IDs. Changed reservation/payment operation payloads return 409. Released reservations are tombstones and cannot be reacquired by replaying an old ID.
