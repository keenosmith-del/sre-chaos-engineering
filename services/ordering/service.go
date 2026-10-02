package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"math"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"net/http"
	"reliability/internal/contracts"
	"reliability/internal/platform"
	"reliability/internal/resilience"
	"time"
)

const columns = "id,product,quantity,amount,decline,state,reason,trace_id,created_at::text"

func scan(row pgx.Row) (contracts.Order, error) {
	var o contracts.Order
	e := row.Scan(&o.ID, &o.Product, &o.Quantity, &o.Amount, &o.Decline, &o.State, &o.Reason, &o.TraceID, &o.CreatedAt)
	return o, e
}
func allowed(from, to string) bool {
	switch from {
	case "PENDING":
		return to == "INVENTORY_RESERVED" || to == "FAILED"
	case "INVENTORY_RESERVED":
		return to == "PAYMENT_PENDING"
	case "PAYMENT_PENDING":
		return to == "CONFIRMED" || to == "RECONCILIATION_REQUIRED" || to == "COMPENSATING"
	case "RECONCILIATION_REQUIRED":
		return to == "CONFIRMED" || to == "COMPENSATING"
	case "COMPENSATING":
		return to == "CANCELLED"
	}
	return false
}
func transition(ctx context.Context, tx pgx.Tx, o *contracts.Order, state, reason string) error {
	if !allowed(o.State, state) {
		return errors.New("invalid state transition")
	}
	_, e := tx.Exec(ctx, "UPDATE ordering.orders SET state=$2,reason=$3,updated_at=now() WHERE id=$1", o.ID, state, reason)
	if e != nil {
		return e
	}
	_, e = tx.Exec(ctx, "INSERT INTO ordering.transitions(order_id,state,reason) VALUES($1,$2,$3)", o.ID, state, reason)
	o.State = state
	o.Reason = reason
	return e
}
func process(a *platform.App, ctx context.Context, id string) error {
	// Session advisory lock avoids duplicate sagas without holding a SQL transaction across HTTP.
	conn, e := a.DB.Acquire(ctx)
	if e != nil {
		return e
	}
	defer conn.Release()
	var locked bool
	e = conn.QueryRow(ctx, "SELECT pg_try_advisory_lock(hashtextextended($1,1))", id).Scan(&locked)
	if e != nil || !locked {
		return e
	}
	defer func() {
		c, x := context.WithTimeout(context.Background(), time.Second)
		defer x()
		if _, err := conn.Exec(c, "SELECT pg_advisory_unlock(hashtextextended($1,1))", id); err != nil {
			// Never return a session with an advisory lock to the pool.
			conn.Conn().Close(c)
		}
	}()
	o, e := scan(conn.QueryRow(ctx, "SELECT "+columns+" FROM ordering.orders WHERE id=$1", id))
	if e != nil {
		return e
	}
	var parent string
	conn.QueryRow(ctx, "SELECT traceparent FROM ordering.orders WHERE id=$1", id).Scan(&parent)
	ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier{"traceparent": parent})
	ctx, span := otel.Tracer("ordering").Start(ctx, "order saga")
	defer span.End()
	change := func(state, reason string) error {
		tx, e := conn.Begin(ctx)
		if e != nil {
			return e
		}
		defer tx.Rollback(ctx)
		if e = transition(ctx, tx, &o, state, reason); e != nil {
			return e
		}
		if state == "CONFIRMED" {
			b, _ := json.Marshal(o)
			h := propagation.MapCarrier{}
			otel.GetTextMapPropagator().Inject(ctx, h)
			hb, _ := json.Marshal(h)
			_, e = tx.Exec(ctx, "INSERT INTO ordering.outbox(id,payload,headers) VALUES($1,$2,$3) ON CONFLICT DO NOTHING", id, b, hb)
			if e != nil {
				return e
			}
		}
		e = tx.Commit(ctx)
		if e == nil {
			platform.Outcomes.WithLabelValues("ordering", state).Inc()
		}
		return e
	}
	op := contracts.Operation{ID: o.ID, Product: o.Product, Quantity: o.Quantity, Amount: o.Amount, Decline: o.Decline}
	if o.State == "PENDING" {
		var reservation map[string]any
		e = a.Client.Call(ctx, "POST", platform.Env("INVENTORY_URL", "http://toxiproxy:8081")+"/reservations", op, &reservation)
		if e != nil {
			var dep *resilience.Error
			if errors.As(e, &dep) && dep.Status == 409 {
				return change("FAILED", dep.Message)
			}
			return e
		}
		if reservation["active"] != true {
			return errors.New("reservation inactive")
		}
		if e = change("INVENTORY_RESERVED", ""); e != nil {
			return e
		}
	}
	if o.State == "INVENTORY_RESERVED" {
		if e = change("PAYMENT_PENDING", ""); e != nil {
			return e
		}
	}
	if o.State == "PAYMENT_PENDING" || o.State == "RECONCILIATION_REQUIRED" {
		var result map[string]string
		// Query a previously durable operation before retrying an ambiguous authorization.
		if o.State == "RECONCILIATION_REQUIRED" {
			e = a.Client.Call(ctx, "GET", platform.Env("PAYMENTS_URL", "http://toxiproxy:8082")+"/authorizations/"+id, nil, &result)
			var dep *resilience.Error
			if e != nil && !(errors.As(e, &dep) && dep.Status == 404) {
				return e
			}
		}
		if result["status"] == "" {
			e = a.Client.Call(ctx, "POST", platform.Env("PAYMENTS_URL", "http://toxiproxy:8082")+"/authorizations", op, &result)
		}
		if e != nil {
			if o.State == "PAYMENT_PENDING" {
				return change("RECONCILIATION_REQUIRED", e.Error())
			}
			return e
		}
		switch result["status"] {
		case "AUTHORIZED":
			return change("CONFIRMED", "")
		case "DECLINED":
			if e = change("COMPENSATING", "payment declined"); e != nil {
				return e
			}
		default:
			return errors.New("unknown payment status")
		}
	}
	if o.State == "COMPENSATING" {
		e = a.Client.Call(ctx, "DELETE", platform.Env("INVENTORY_URL", "http://toxiproxy:8081")+"/reservations/"+id, nil, nil)
		if e != nil {
			return e
		}
		return change("CANCELLED", "payment declined; reservation released")
	}
	return nil
}
func setup(a *platform.App) {
	cache := redis.NewClient(&redis.Options{Addr: platform.Env("REDIS_ADDR", "toxiproxy:6379"), DialTimeout: 300 * time.Millisecond, ReadTimeout: 300 * time.Millisecond, WriteTimeout: 300 * time.Millisecond, MaxRetries: 1})
	deferClose := func() { <-a.Ctx.Done(); cache.Close() }
	go deferClose()
	a.Mux.HandleFunc("POST /orders", func(w http.ResponseWriter, r *http.Request) {
		var req contracts.OrderRequest
		if !platform.Decode(w, r, &req) {
			return
		}
		key := r.Header.Get("Idempotency-Key")
		if len(key) < 8 || len(key) > 128 || req.Quantity < 1 || req.Quantity > 100 || req.Product == "" {
			platform.Fail(w, 400, "Idempotency-Key (8..128 chars), product and quantity (1..100) required")
			return
		}
		b, _ := json.Marshal(req)
		h := sha256.Sum256(b)
		hash := hex.EncodeToString(h[:])
		// Existing keys work even when catalog dependency is unavailable.
		var id, oldhash string
		e := a.DB.QueryRow(r.Context(), "SELECT id,hash FROM ordering.orders WHERE idem=$1", key).Scan(&id, &oldhash)
		if e == nil {
			if hash != oldhash {
				platform.Fail(w, 409, "idempotency key reused with different payload")
				return
			}
			process(a, r.Context(), id)
			o, e := scan(a.DB.QueryRow(r.Context(), "SELECT "+columns+" FROM ordering.orders WHERE id=$1", id))
			if e != nil {
				platform.Fail(w, 503, e.Error())
				return
			}
			platform.JSON(w, 200, o)
			return
		}
		if e != pgx.ErrNoRows {
			platform.Fail(w, 503, e.Error())
			return
		}
		var products []struct {
			ID    string `json:"id"`
			Price int    `json:"price"`
		}
		cached, ce := cache.Get(r.Context(), "catalog").Bytes()
		if ce == nil {
			json.Unmarshal(cached, &products)
		} else {
			platform.Outcomes.WithLabelValues("ordering", "cache_fallback").Inc()
		}
		if len(products) == 0 {
			e = a.Client.Call(r.Context(), "GET", platform.Env("INVENTORY_URL", "http://toxiproxy:8081")+"/products", nil, &products)
			if e != nil {
				platform.Fail(w, 503, e.Error())
				return
			}
			v, _ := json.Marshal(products)
			cache.Set(r.Context(), "catalog", v, 10*time.Second)
		}
		price := 0
		for _, p := range products {
			if p.ID == req.Product {
				price = p.Price
			}
		}
		if price == 0 {
			platform.Fail(w, 400, "unknown product")
			return
		}
		id = platform.ID()
		carrier := propagation.MapCarrier{}
		otel.GetTextMapPropagator().Inject(r.Context(), carrier)
		tx, e := a.DB.Begin(r.Context())
		if e != nil {
			platform.Fail(w, 503, e.Error())
			return
		}
		defer tx.Rollback(r.Context())
		tag, e := tx.Exec(r.Context(), "INSERT INTO ordering.orders(id,idem,hash,product,quantity,amount,decline,state,trace_id,traceparent) VALUES($1,$2,$3,$4,$5,$6,$7,'PENDING',$8,$9) ON CONFLICT(idem) DO NOTHING", id, key, hash, req.Product, req.Quantity, req.Quantity*price, req.Decline, trace.SpanContextFromContext(r.Context()).TraceID().String(), carrier["traceparent"])
		if e != nil {
			platform.Fail(w, 503, e.Error())
			return
		}
		if tag.RowsAffected() == 1 {
			_, e = tx.Exec(r.Context(), "INSERT INTO ordering.transitions(order_id,state) VALUES($1,'PENDING')", id)
		} else {
			e = tx.QueryRow(r.Context(), "SELECT id,hash FROM ordering.orders WHERE idem=$1", key).Scan(&id, &oldhash)
			if e == nil && oldhash != hash {
				platform.Fail(w, 409, "idempotency key reused with different payload")
				return
			}
		}
		if e == nil {
			e = tx.Commit(r.Context())
		}
		if e != nil {
			platform.Fail(w, 503, e.Error())
			return
		}
		process(a, r.Context(), id)
		o, e := scan(a.DB.QueryRow(r.Context(), "SELECT "+columns+" FROM ordering.orders WHERE id=$1", id))
		if e != nil {
			platform.Fail(w, 503, e.Error())
			return
		}
		platform.JSON(w, 201, o)
	})
	a.Mux.HandleFunc("GET /orders", func(w http.ResponseWriter, r *http.Request) {
		rows, e := a.DB.Query(r.Context(), "SELECT "+columns+" FROM ordering.orders ORDER BY created_at DESC LIMIT 100")
		if e != nil {
			platform.Fail(w, 503, e.Error())
			return
		}
		defer rows.Close()
		out := []contracts.Order{}
		for rows.Next() {
			o, e := scan(rows)
			if e == nil {
				out = append(out, o)
			}
		}
		platform.JSON(w, 200, out)
	})
	a.Mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		o, e := scan(a.DB.QueryRow(r.Context(), "SELECT "+columns+" FROM ordering.orders WHERE id=$1", r.PathValue("id")))
		if e == pgx.ErrNoRows {
			platform.Fail(w, 404, "order absent")
			return
		}
		if e != nil {
			platform.Fail(w, 503, e.Error())
			return
		}
		platform.JSON(w, 200, o)
	})
	a.Mux.HandleFunc("GET /transactions/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		o, e := scan(a.DB.QueryRow(r.Context(), "SELECT "+columns+" FROM ordering.orders WHERE id=$1", id))
		if e != nil {
			platform.Fail(w, 404, "order unavailable")
			return
		}
		var inv, pay map[string]any
		ie := a.Client.Call(r.Context(), "GET", platform.Env("INVENTORY_URL", "http://toxiproxy:8081")+"/reservations/"+id, nil, &inv)
		pe := a.Client.Call(r.Context(), "GET", platform.Env("PAYMENTS_URL", "http://toxiproxy:8082")+"/authorizations/"+id, nil, &pay)
		rows, e := a.DB.Query(r.Context(), "SELECT state,reason,at::text FROM ordering.transitions WHERE order_id=$1 ORDER BY seq", id)
		history := []any{}
		if e == nil {
			defer rows.Close()
			for rows.Next() {
				var s, reason, at string
				rows.Scan(&s, &reason, &at)
				history = append(history, map[string]string{"state": s, "reason": reason, "at": at})
			}
		}
		platform.JSON(w, 200, map[string]any{"order": o, "inventory": inv, "payment": pay, "inventory_error": errText(ie), "payment_error": errText(pe), "history": history})
	})
	a.Mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		result, e := readStats(a, r.Context())
		if e != nil {
			platform.Fail(w, 503, e.Error())
			return
		}
		platform.JSON(w, 200, result)
	})
	go monitorStats(a)
	go reconcile(a)
	go publish(a)
}
func errText(e error) string {
	if e != nil {
		return e.Error()
	}
	return ""
}
func reconcile(a *platform.App) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-a.Ctx.Done():
			return
		case <-t.C:
			ctx, x := context.WithTimeout(a.Ctx, 20*time.Second)
			rows, e := a.DB.Query(ctx, "SELECT id FROM ordering.orders WHERE state NOT IN ('CONFIRMED','CANCELLED','FAILED') ORDER BY updated_at LIMIT 20")
			if e == nil {
				ids := []string{}
				for rows.Next() {
					var id string
					rows.Scan(&id)
					ids = append(ids, id)
				}
				rows.Close()
				for _, id := range ids {
					process(a, ctx, id)
				}
			}
			x()
		}
	}
}
func connect() (*amqp.Connection, *amqp.Channel, error) {
	c, e := amqp.DialConfig(platform.Env("AMQP_URL", "amqp://demo:local_rabbit@toxiproxy:5672/"), amqp.Config{Dial: amqp.DefaultDial(2 * time.Second)})
	if e != nil {
		return nil, nil, e
	}
	ch, e := c.Channel()
	if e != nil {
		c.Close()
		return nil, nil, e
	}
	if e = ch.ExchangeDeclare("orders.dlx", "direct", true, false, false, false, nil); e == nil {
		_, e = ch.QueueDeclare("orders.dead", true, false, false, false, nil)
	}
	if e == nil {
		e = ch.QueueBind("orders.dead", "dead", "orders.dlx", false, nil)
	}
	if e == nil {
		_, e = ch.QueueDeclare("orders.confirmed", true, false, false, false, amqp.Table{"x-dead-letter-exchange": "orders.dlx", "x-dead-letter-routing-key": "dead"})
	}
	if e != nil {
		ch.Close()
		c.Close()
		return nil, nil, e
	}
	return c, ch, nil
}
func publish(a *platform.App) {
	for a.Ctx.Err() == nil {
		func() {
			c, ch, e := connect()
			if e != nil {
				return
			}
			defer c.Close()
			defer ch.Close()
			if ch.Confirm(false) != nil {
				return
			}
			confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
			returns := ch.NotifyReturn(make(chan amqp.Return, 1))
			for a.Ctx.Err() == nil {
				ctx, x := context.WithTimeout(a.Ctx, 5*time.Second)
				tx, e := a.DB.Begin(ctx)
				if e != nil {
					x()
					return
				}
				var id string
				var payload, headers []byte
				e = tx.QueryRow(ctx, "SELECT id,payload,headers FROM ordering.outbox WHERE published_at IS NULL ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1").Scan(&id, &payload, &headers)
				if e == pgx.ErrNoRows {
					tx.Rollback(ctx)
					x()
					time.Sleep(time.Second)
					continue
				}
				if e != nil {
					tx.Rollback(ctx)
					x()
					return
				}
				var h map[string]string
				json.Unmarshal(headers, &h)
				table := amqp.Table{}
				for k, v := range h {
					table[k] = v
				}
				e = ch.PublishWithContext(ctx, "", "orders.confirmed", true, false, amqp.Publishing{ContentType: "application/json", DeliveryMode: amqp.Persistent, MessageId: id, Body: payload, Headers: table})
				if e == nil {
					select {
					case confirm := <-confirms:
						if !confirm.Ack {
							e = errors.New("publisher nack")
						}
					case <-ctx.Done():
						e = ctx.Err()
					}
					select {
					case <-returns:
						e = errors.New("message unroutable")
					default:
					}
				}
				if e == nil {
					_, e = tx.Exec(ctx, "UPDATE ordering.outbox SET published_at=now() WHERE id=$1", id)
					if e == nil {
						e = tx.Commit(ctx)
					}
				} else {
					tx.Rollback(ctx)
				}
				tx.Rollback(ctx)
				x()
				if e != nil {
					return
				}
			}
		}()
		select {
		case <-a.Ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func monitorStats(a *platform.App) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-a.Ctx.Done():
			return
		case <-t.C:
			ctx, x := context.WithTimeout(a.Ctx, time.Second)
			_, _ = readStats(a, ctx)
			x()
		}
	}
}

func readStats(a *platform.App, ctx context.Context) (map[string]any, error) {
	var outstanding, outbox int
	var age float64
	e := a.DB.QueryRow(ctx, "SELECT count(*) FROM ordering.orders WHERE state NOT IN ('CONFIRMED','CANCELLED','FAILED')").Scan(&outstanding)
	if e == nil {
		e = a.DB.QueryRow(ctx, "SELECT count(*),coalesce(extract(epoch FROM now()-min(created_at)),0) FROM ordering.outbox WHERE published_at IS NULL").Scan(&outbox, &age)
	}
	if e != nil {
		platform.Backlog.WithLabelValues("reconciliation").Set(math.NaN())
		platform.Backlog.WithLabelValues("outbox").Set(math.NaN())
		platform.Age.Set(math.NaN())
		return nil, e
	}
	platform.Backlog.WithLabelValues("reconciliation").Set(float64(outstanding))
	platform.Backlog.WithLabelValues("outbox").Set(float64(outbox))
	platform.Age.Set(age)
	return map[string]any{"outstanding": outstanding, "outbox": outbox, "outbox_age_seconds": age}, nil
}
