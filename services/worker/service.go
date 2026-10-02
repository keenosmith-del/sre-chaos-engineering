package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"net/http"
	"reliability/internal/contracts"
	"reliability/internal/platform"
	"time"
)

var processing = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "worker_processing_duration_seconds", Help: "Durable message processing time"})

func init() { prometheus.MustRegister(processing) }
func setup(a *platform.App) {
	a.Mux.HandleFunc("POST /replay", func(w http.ResponseWriter, r *http.Request) {
		n, e := replay(r.Context())
		if e != nil {
			platform.Fail(w, 503, e.Error())
			return
		}
		platform.JSON(w, 200, map[string]int{"replayed": n})
	})
	a.Mux.HandleFunc("GET /deliveries/{id}", func(w http.ResponseWriter, r *http.Request) {
		var at string
		e := a.DB.QueryRow(r.Context(), "SELECT at::text FROM worker.deliveries WHERE id=$1", r.PathValue("id")).Scan(&at)
		if e != nil {
			platform.Fail(w, 404, "delivery absent or unavailable")
			return
		}
		platform.JSON(w, 200, map[string]string{"id": r.PathValue("id"), "processed_at": at})
	})
	go consume(a)
}
func consume(a *platform.App) {
	for a.Ctx.Err() == nil {
		func() {
			c, e := amqp.DialConfig(platform.Env("AMQP_URL", "amqp://demo:local_rabbit@toxiproxy:5672/"), amqp.Config{Dial: amqp.DefaultDial(2 * time.Second)})
			if e != nil {
				return
			}
			defer c.Close()
			ch, e := c.Channel()
			if e != nil {
				return
			}
			defer ch.Close()
			ch.ExchangeDeclare("orders.dlx", "direct", true, false, false, false, nil)
			ch.QueueDeclare("orders.dead", true, false, false, false, nil)
			ch.QueueBind("orders.dead", "dead", "orders.dlx", false, nil)
			_, e = ch.QueueDeclare("orders.confirmed", true, false, false, false, amqp.Table{"x-dead-letter-exchange": "orders.dlx", "x-dead-letter-routing-key": "dead"})
			if e != nil {
				return
			}
			ch.Qos(8, 0, false)
			msgs, e := ch.Consume("orders.confirmed", "", false, false, false, false, nil)
			if e != nil {
				return
			}
			for {
				select {
				case <-a.Ctx.Done():
					return
				case msg, ok := <-msgs:
					if !ok {
						return
					}
					func() {
						started := time.Now()
						defer func() { processing.Observe(time.Since(started).Seconds()) }()
						carrier := propagation.MapCarrier{}
						for k, v := range msg.Headers {
							if s, ok := v.(string); ok {
								carrier[k] = s
							}
						}
						ctx := otel.GetTextMapPropagator().Extract(a.Ctx, carrier)
						ctx, span := otel.Tracer("worker").Start(ctx, "consume order.confirmed")
						defer span.End()
						var o contracts.Order
						if json.Unmarshal(msg.Body, &o) != nil || o.ID != msg.MessageId || o.State != "CONFIRMED" {
							msg.Nack(false, false)
							platform.Outcomes.WithLabelValues("worker", "dead_letter").Inc()
							return
						}
						var e error
						for attempt := 0; attempt < 3; attempt++ {
							c, x := context.WithTimeout(ctx, 2*time.Second)
							_, e = a.DB.Exec(c, "INSERT INTO worker.deliveries(id,payload) VALUES($1,$2) ON CONFLICT DO NOTHING", o.ID, msg.Body)
							x()
							if e == nil {
								break
							}
							time.Sleep(time.Duration(attempt+1) * 200 * time.Millisecond)
						}
						if e != nil {
							span.RecordError(e)
							msg.Nack(false, false)
							platform.Outcomes.WithLabelValues("worker", "dead_letter").Inc()
						} else {
							msg.Ack(false)
							platform.Outcomes.WithLabelValues("worker", "processed").Inc()
						}
					}()
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

func replay(ctx context.Context) (int, error) {
	c, e := amqp.DialConfig(platform.Env("AMQP_URL", "amqp://demo:local_rabbit@toxiproxy:5672/"), amqp.Config{Dial: amqp.DefaultDial(2 * time.Second)})
	if e != nil {
		return 0, e
	}
	defer c.Close()
	ch, e := c.Channel()
	if e != nil {
		return 0, e
	}
	defer ch.Close()
	if e = ch.Confirm(false); e != nil {
		return 0, e
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	n := 0
	for n < 20 {
		msg, ok, e := ch.Get("orders.dead", false)
		if e != nil {
			return n, e
		}
		if !ok {
			return n, nil
		}
		e = ch.PublishWithContext(ctx, "", "orders.confirmed", false, false, amqp.Publishing{ContentType: msg.ContentType, DeliveryMode: amqp.Persistent, MessageId: msg.MessageId, Headers: msg.Headers, Body: msg.Body})
		if e == nil {
			select {
			case confirm := <-confirms:
				if !confirm.Ack {
					e = fmt.Errorf("publisher nack")
				}
			case <-ctx.Done():
				e = ctx.Err()
			}
		}
		if e != nil {
			msg.Nack(false, true)
			return n, e
		}
		if e = msg.Ack(false); e != nil {
			return n, e
		}
		n++
	}
	return n, nil
}
