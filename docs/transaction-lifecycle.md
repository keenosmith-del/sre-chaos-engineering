# Transaction lifecycle and correctness

Allowed transitions:

```text
PENDING -> INVENTORY_RESERVED -> PAYMENT_PENDING -> CONFIRMED
PENDING -> FAILED (definitive insufficient stock)
PAYMENT_PENDING -> RECONCILIATION_REQUIRED -> CONFIRMED
PAYMENT_PENDING or RECONCILIATION_REQUIRED -> COMPENSATING -> CANCELLED
```

A canonical JSON request hash and unique idempotency key select one durable order. Concurrent INSERT uses ON CONFLICT, then checks the winning hash. A replay retrieves the same order even if the catalog is unavailable. Transitions and confirmation outbox insertion are transactional. A session advisory lock serializes each saga across requests/reconciliation without holding SQL transactions over HTTP. Interrupted requests leave durable states for the background reconciler.

Inventory serializes the same reservation ID with a transaction advisory lock. A conditional UPDATE subtracts stock only when sufficient; CHECK stock >= 0 and transaction rollback prevent overselling. Reservation insert commits with the decrement. Release locks the reservation, restores quantity once, and marks it inactive in the same transaction. Released operation tombstones prevent duplicate acquisition.

Payments inserts one authorization per stable order ID and returns its durable outcome; amount/decline conflicts are rejected. This simulated service has an atomic durable result. It has no external payment provider or capture step. Ordering queries the result after an ambiguous outcome, then repeats only the same idempotent operation if genuinely absent. An unknown result stays unresolved and is never confirmed implicitly.

A decline enters COMPENSATING before requesting release. A failed release remains retryable in that state. CANCELLED commits only after a successful idempotent release. CONFIRMED requires observed active reservation and successful payment. The runner verifies these invariants against all three schemas.

The outbox publisher locks one pending row, publishes a persistent RabbitMQ message to a durable queue with mandatory routing and publisher confirms, then records published_at. A crash between confirm and SQL commit can publish twice; worker unique delivery IDs make that safe. HTTP trace context is persisted in outbox headers and propagated into the worker consume span. Worker acknowledges only after durable projection insert. Three bounded SQL attempts precede dead-lettering. Durable dead-letter messages remain recoverable through the documented replay command.

There is no cross-system atomic transaction between PostgreSQL and RabbitMQ. The guarantees are at-least-once delivery and idempotent processing, not exactly-once transport.
