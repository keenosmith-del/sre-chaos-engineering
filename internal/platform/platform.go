package platform

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"reliability/internal/resilience"
	"strings"
	"syscall"
	"time"
)

var Requests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "http_requests_total", Help: "HTTP requests excluding health and metrics"}, []string{"service", "method", "route", "status"})
var Duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "http_request_duration_seconds", Help: "HTTP duration", Buckets: []float64{.005, .025, .05, .1, .25, .5, 1, 2, 5, 10}}, []string{"service", "route"})
var Outcomes = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "business_outcomes_total", Help: "Durable business outcomes"}, []string{"service", "outcome"})
var Backlog = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "durable_backlog", Help: "Durable work backlog"}, []string{"kind"})
var Age = prometheus.NewGauge(prometheus.GaugeOpts{Name: "outbox_oldest_seconds", Help: "Age of oldest unpublished event"})

func init() { prometheus.MustRegister(Requests, Duration, Outcomes, Backlog, Age) }

type App struct {
	Name   string
	DB     *pgxpool.Pool
	Mux    *http.ServeMux
	Client *resilience.Client
	Ctx    context.Context
}

func Env(k, f string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return f
}
func ID() string { var b [16]byte; rand.Read(b[:]); return hex.EncodeToString(b[:]) }
func JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
func Fail(w http.ResponseWriter, status int, msg string) {
	JSON(w, status, map[string]any{"error": msg})
}
func Decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		Fail(w, 400, "invalid JSON: "+e.Error())
		return false
	}
	var extra any
	if e := d.Decode(&extra); !errors.Is(e, io.EOF) {
		Fail(w, 400, "expected one JSON object")
		return false
	}
	return true
}

func Run(name string, setup func(*App)) {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	exp, e := otlptracehttp.New(ctx, otlptracehttp.WithEndpoint(Env("OTEL_ENDPOINT", "otel:4318")), otlptracehttp.WithInsecure())
	if e != nil {
		panic(e)
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(resource.NewWithAttributes("", attribute.String("service.name", name))))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer tp.Shutdown(context.Background())
	timeout, e := time.ParseDuration(Env("DEPENDENCY_TIMEOUT", "800ms"))
	if e != nil {
		panic(e)
	}
	a := &App{Name: name, Mux: http.NewServeMux(), Client: resilience.New(timeout), Ctx: ctx}
	for _, dep := range []struct{ key, host, setting string }{{"INVENTORY_URL", "http://toxiproxy:8081", "INVENTORY_TIMEOUT"}, {"PAYMENTS_URL", "http://toxiproxy:8082", "PAYMENTS_TIMEOUT"}} {
		u, err := url.Parse(Env(dep.key, dep.host))
		if err != nil {
			panic(err)
		}
		d, err := time.ParseDuration(Env(dep.setting, timeout.String()))
		if err != nil || d <= 0 {
			panic("invalid dependency timeout")
		}
		a.Client.SetTimeout(u.Host, d)
	}
	if url := os.Getenv("DATABASE_URL"); url != "" {
		a.DB, e = pgxpool.New(ctx, url)
		if e != nil {
			panic(e)
		}
		defer a.DB.Close()
	}
	a.Mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) { JSON(w, 200, map[string]string{"status": "live"}) })
	a.Mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		c, x := context.WithTimeout(r.Context(), time.Second)
		defer x()
		if a.DB != nil {
			if e := a.DB.Ping(c); e != nil {
				Fail(w, 503, e.Error())
				return
			}
		}
		JSON(w, 200, map[string]string{"status": "ready"})
	})
	a.Mux.Handle("GET /metrics", promhttp.Handler())
	setup(a)
	server := &http.Server{Addr: ":8080", Handler: a.instrument(), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 32 << 10}
	go func() {
		<-ctx.Done()
		c, x := context.WithTimeout(context.Background(), 5*time.Second)
		defer x()
		server.Shutdown(c)
	}()
	slog.Info("listening", "service", name)
	if e := server.ListenAndServe(); e != nil && e != http.ErrServerClosed {
		panic(e)
	}
}

type recorder struct {
	http.ResponseWriter
	status int
}

func (w *recorder) WriteHeader(s int) { w.status = s; w.ResponseWriter.WriteHeader(s) }
func (w *recorder) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
func (a *App) instrument() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" || r.URL.Path == "/health/live" || r.URL.Path == "/health/ready" {
			a.Mux.ServeHTTP(w, r)
			return
		}
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := otel.Tracer(a.Name).Start(ctx, r.Method+" "+r.URL.Path, trace.WithSpanKind(trace.SpanKindServer))
		defer span.End()
		id := r.Header.Get("X-Correlation-ID")
		if len(id) == 0 || len(id) > 128 {
			id = ID()
		}
		w.Header().Set("X-Correlation-ID", id)
		w.Header().Set("X-Trace-ID", span.SpanContext().TraceID().String())
		rw := &recorder{w, 200}
		start := time.Now()
		r = r.WithContext(ctx)
		a.Mux.ServeHTTP(rw, r)
		route := r.Pattern
		if a.Name == "gateway" {
			if r.URL.Path == "/api/orders" {
				route = "/api/orders"
			} else if strings.HasPrefix(r.URL.Path, "/api/control/") {
				route = "/api/control/"
			} else {
				route = "/api/read/"
			}
		}
		if route == "" {
			route = "unmatched"
		}
		Requests.WithLabelValues(a.Name, r.Method, route, http.StatusText(rw.status)).Inc()
		Duration.WithLabelValues(a.Name, route).Observe(time.Since(start).Seconds())
		slog.Info("request", "service", a.Name, "route", route, "status", rw.status, "correlation_id", id, "trace_id", span.SpanContext().TraceID().String())
	})
}
