CREATE ROLE ordering LOGIN PASSWORD 'local_ordering';
CREATE ROLE inventory LOGIN PASSWORD 'local_inventory';
CREATE ROLE payments LOGIN PASSWORD 'local_payments';
CREATE ROLE worker LOGIN PASSWORD 'local_worker';
CREATE ROLE control LOGIN PASSWORD 'local_control';
CREATE SCHEMA AUTHORIZATION ordering;
CREATE SCHEMA AUTHORIZATION inventory;
CREATE SCHEMA AUTHORIZATION payments;
CREATE SCHEMA AUTHORIZATION worker;
CREATE SCHEMA AUTHORIZATION control;
SET ROLE ordering;
CREATE TABLE ordering.orders (
 id text PRIMARY KEY, idem text UNIQUE NOT NULL, hash text NOT NULL,
 product text NOT NULL, quantity integer NOT NULL CHECK(quantity>0), amount integer NOT NULL CHECK(amount>0),
 decline boolean NOT NULL DEFAULT false, state text NOT NULL CHECK(state IN ('PENDING','INVENTORY_RESERVED','PAYMENT_PENDING','CONFIRMED','RECONCILIATION_REQUIRED','COMPENSATING','CANCELLED','FAILED')),
 reason text NOT NULL DEFAULT '', trace_id text NOT NULL DEFAULT '', traceparent text NOT NULL DEFAULT '',
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE ordering.transitions (seq bigserial PRIMARY KEY, order_id text REFERENCES ordering.orders(id), state text NOT NULL, reason text NOT NULL DEFAULT '', at timestamptz NOT NULL DEFAULT now());
CREATE TABLE ordering.outbox (id text PRIMARY KEY REFERENCES ordering.orders(id), payload jsonb NOT NULL, headers jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), published_at timestamptz);
RESET ROLE;
SET ROLE inventory;
CREATE TABLE inventory.products (id text PRIMARY KEY, name text NOT NULL, price integer NOT NULL CHECK(price>0), stock integer NOT NULL CHECK(stock>=0));
CREATE TABLE inventory.reservations (id text PRIMARY KEY, product text REFERENCES inventory.products(id), quantity integer NOT NULL CHECK(quantity>0), active boolean NOT NULL DEFAULT true);
INSERT INTO inventory.products VALUES ('keyboard','Mechanical keyboard',7500,10000),('mouse','Wireless mouse',2500,10000),('monitor','27 inch monitor',25000,10000);
RESET ROLE;
SET ROLE payments;
CREATE TABLE payments.authorizations (id text PRIMARY KEY, amount integer NOT NULL CHECK(amount>0), decline boolean NOT NULL, status text NOT NULL CHECK(status IN ('AUTHORIZED','DECLINED')), at timestamptz NOT NULL DEFAULT now());
RESET ROLE;
SET ROLE worker;
CREATE TABLE worker.deliveries (id text PRIMARY KEY, payload jsonb NOT NULL, at timestamptz NOT NULL DEFAULT now());
RESET ROLE;
SET ROLE control;
CREATE TABLE control.experiments (id text PRIMARY KEY, state text NOT NULL, spec jsonb NOT NULL, evidence jsonb NOT NULL DEFAULT '[]', error text NOT NULL DEFAULT '', created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now());
CREATE UNIQUE INDEX one_active_experiment ON control.experiments ((true)) WHERE state NOT IN ('COMPLETED','FAILED','ABORTED');
CREATE TABLE control.transitions (seq bigserial PRIMARY KEY, experiment_id text NOT NULL REFERENCES control.experiments(id), state text NOT NULL, at timestamptz NOT NULL DEFAULT now());
CREATE TABLE control.load_runs (id text PRIMARY KEY, experiment_id text, profile text NOT NULL, state text NOT NULL, started_at timestamptz NOT NULL DEFAULT now(), ended_at timestamptz);
CREATE UNIQUE INDEX one_active_load ON control.load_runs ((true)) WHERE state='RUNNING';
CREATE TABLE control.recovery (id text PRIMARY KEY, record jsonb NOT NULL, at timestamptz NOT NULL DEFAULT now());
RESET ROLE;
