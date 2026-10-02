package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"html/template"
	"net/http"
	"net/url"
	"reliability/internal/platform"
	"reliability/internal/resilience"
	"strconv"
	"sync"
	"time"
)

type Spec struct {
	Fault         string  `json:"fault"`
	Target        string  `json:"target"`
	Magnitude     int     `json:"magnitude"`
	Duration      int     `json:"duration"`
	Baseline      int     `json:"baseline"`
	Recovery      int     `json:"recovery"`
	StopErrorRate float64 `json:"stop_error_rate"`
}

var targets = map[string]string{"inventory_unavailable": "inventory", "payment_latency": "payments", "redis_outage": "redis", "rabbitmq_interruption": "rabbitmq", "worker_restart": "worker", "postgres_interruption": "postgres"}

func (s Spec) validate() error {
	if t, ok := targets[s.Fault]; !ok || t != s.Target {
		return errors.New("fault/target is not allowlisted")
	}
	if s.Duration < 5 || s.Duration > 120 || s.Baseline < 10 || s.Baseline > 120 || s.Recovery < 10 || s.Recovery > 120 {
		return errors.New("duration 5..120, baseline and recovery 10..120 seconds required")
	}
	if s.Magnitude < 0 || s.Magnitude > 3000 || (s.Fault == "payment_latency" && s.Magnitude == 0) || s.StopErrorRate < .01 || s.StopErrorRate > 1 {
		return errors.New("magnitude 0..3000 milliseconds, stop_error_rate .01..1 required")
	}
	return nil
}

var activeExperiment = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "active_experiment_info", Help: "Active experiment identifier"}, []string{"experiment_id", "fault"})

func init() { prometheus.MustRegister(activeExperiment) }

type controller struct {
	a       *platform.App
	runner  *resilience.Client
	mu      sync.Mutex
	loadMu  sync.Mutex
	cancels map[string]context.CancelFunc
}

func (c *controller) runnerCall(ctx context.Context, path string, in, out any) error {
	return c.runner.Call(ctx, "POST", platform.Env("RUNNER_URL", "http://runner:8080")+path, in, out)
}
func setup(a *platform.App) {
	c := &controller{a: a, runner: resilience.New(150 * time.Second), cancels: map[string]context.CancelFunc{}}
	a.Mux.HandleFunc("POST /deadletters/replay", func(w http.ResponseWriter, r *http.Request) {
		var result map[string]any
		client := resilience.New(20 * time.Second)
		if e := client.Call(r.Context(), "POST", "http://worker:8080/replay", nil, &result); e != nil {
			platform.Fail(w, 503, e.Error())
			return
		}
		platform.JSON(w, 200, result)
	})
	a.Mux.HandleFunc("GET /services", func(w http.ResponseWriter, r *http.Request) { platform.JSON(w, 200, c.services(r.Context())) })
	a.Mux.HandleFunc("GET /operational-metrics", func(w http.ResponseWriter, r *http.Request) { platform.JSON(w, 200, c.snapshot(r.Context(), 30)) })
	a.Mux.HandleFunc("GET /experiments", func(w http.ResponseWriter, r *http.Request) { c.list(w, r, "experiments") })
	a.Mux.HandleFunc("POST /experiments", func(w http.ResponseWriter, r *http.Request) {
		var s Spec
		if !platform.Decode(w, r, &s) {
			return
		}
		if e := s.validate(); e != nil {
			platform.Fail(w, 400, e.Error())
			return
		}
		id := platform.ID()
		b, _ := json.Marshal(s)
		_, e := a.DB.Exec(r.Context(), "INSERT INTO control.experiments(id,state,spec) VALUES($1,'CREATED',$2)", id, b)
		if e != nil {
			platform.Fail(w, 409, "another active experiment or persistence unavailable: "+e.Error())
			return
		}
		a.DB.Exec(r.Context(), "INSERT INTO control.transitions(experiment_id,state) VALUES($1,'CREATED')", id)
		go c.execute(id, s)
		platform.JSON(w, 202, map[string]string{"id": id, "state": "CREATED"})
	})
	a.Mux.HandleFunc("GET /experiments/{id}", func(w http.ResponseWriter, r *http.Request) { c.detail(w, r, false) })
	a.Mux.HandleFunc("POST /experiments/{id}/stop", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		c.mu.Lock()
		cancel := c.cancels[id]
		c.mu.Unlock()
		if cancel == nil {
			platform.Fail(w, 409, "experiment is not running")
			return
		}
		cancel()
		platform.JSON(w, 202, map[string]string{"state": "stopping"})
	})
	a.Mux.HandleFunc("POST /experiments/{id}/recover", func(w http.ResponseWriter, r *http.Request) {
		var state string
		e := a.DB.QueryRow(r.Context(), "SELECT state FROM control.experiments WHERE id=$1", r.PathValue("id")).Scan(&state)
		if e != nil {
			platform.Fail(w, 404, "experiment absent")
			return
		}
		var other bool
		_ = a.DB.QueryRow(r.Context(), "SELECT EXISTS(SELECT 1 FROM control.experiments WHERE id<>$1 AND state NOT IN ('COMPLETED','FAILED','ABORTED'))", r.PathValue("id")).Scan(&other)
		if other {
			platform.Fail(w, 409, "another active experiment owns the environment")
			return
		}
		c.mu.Lock()
		cancel := c.cancels[r.PathValue("id")]
		c.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		if e = c.runnerCall(r.Context(), "/cleanup", nil, nil); e != nil {
			platform.Fail(w, 503, e.Error())
			return
		}
		platform.JSON(w, 200, map[string]string{"state": "resources restored; verification pending"})
	})
	go c.monitorLoad()
	a.Mux.HandleFunc("GET /load", func(w http.ResponseWriter, r *http.Request) {
		rows, e := a.DB.Query(r.Context(), "SELECT id,coalesce(experiment_id,''),profile,state,started_at::text,coalesce(ended_at::text,'') FROM control.load_runs ORDER BY started_at DESC LIMIT 30")
		if e != nil {
			platform.Fail(w, 503, e.Error())
			return
		}
		defer rows.Close()
		out := []any{}
		for rows.Next() {
			var id, exp, profile, state, start, end string
			rows.Scan(&id, &exp, &profile, &state, &start, &end)
			out = append(out, map[string]string{"id": id, "experiment_id": exp, "profile": profile, "state": state, "started_at": start, "ended_at": end})
		}
		platform.JSON(w, 200, out)
	})
	a.Mux.HandleFunc("POST /load/start", func(w http.ResponseWriter, r *http.Request) {
		var v struct {
			Profile      string `json:"profile"`
			ExperimentID string `json:"experiment_id"`
		}
		if !platform.Decode(w, r, &v) {
			return
		}
		if !validProfile(v.Profile) {
			platform.Fail(w, 400, "unknown load profile")
			return
		}
		id, e := c.startLoad(r.Context(), v.Profile, v.ExperimentID)
		if e != nil {
			platform.Fail(w, 409, e.Error())
			return
		}
		platform.JSON(w, 202, map[string]string{"id": id})
	})
	a.Mux.HandleFunc("POST /load/stop", func(w http.ResponseWriter, r *http.Request) {
		if e := c.stopLoad(r.Context()); e != nil {
			platform.Fail(w, 503, e.Error())
			return
		}
		platform.JSON(w, 200, map[string]string{"state": "stopped"})
	})
	a.Mux.HandleFunc("GET /recovery", func(w http.ResponseWriter, r *http.Request) {
		rows, e := a.DB.Query(r.Context(), "SELECT record FROM control.recovery ORDER BY at DESC LIMIT 30")
		if e != nil {
			platform.Fail(w, 503, e.Error())
			return
		}
		defer rows.Close()
		out := []json.RawMessage{}
		for rows.Next() {
			var b []byte
			rows.Scan(&b)
			out = append(out, b)
		}
		platform.JSON(w, 200, out)
	})
	for _, action := range []string{"backup", "verify"} {
		action := action
		a.Mux.HandleFunc("POST /recovery/"+action, func(w http.ResponseWriter, r *http.Request) {
			var record map[string]any
			if e := c.runnerCall(r.Context(), "/"+action, nil, &record); e != nil {
				platform.Fail(w, 503, e.Error())
				return
			}
			b, _ := json.Marshal(record)
			_, e := a.DB.Exec(r.Context(), "INSERT INTO control.recovery(id,record) VALUES($1,$2)", platform.ID(), b)
			if e != nil {
				platform.Fail(w, 503, "operation performed but recording failed: "+e.Error())
				return
			}
			platform.JSON(w, 200, record)
		})
	}
	a.Mux.HandleFunc("GET /reports", func(w http.ResponseWriter, r *http.Request) { c.list(w, r, "reports") })
	a.Mux.HandleFunc("GET /reports/{id}", func(w http.ResponseWriter, r *http.Request) { c.detail(w, r, true) })
	a.Mux.HandleFunc("GET /events", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Accel-Buffering", "no")
		t := time.NewTicker(3 * time.Second)
		defer t.Stop()
		fmt.Fprint(w, "event: connected\ndata: {}\n\n")
		w.(http.Flusher).Flush()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-t.C:
				b, _ := json.Marshal(map[string]any{"at": time.Now().UTC(), "services": c.services(r.Context())})
				fmt.Fprintf(w, "event: update\ndata: %s\n\n", b)
				w.(http.Flusher).Flush()
			}
		}
	})
	// Restart recovery is explicit: clean resources before terminating orphaned jobs.
	go func() {
		ctx, x := context.WithTimeout(a.Ctx, 30*time.Second)
		defer x()
		if e := c.runnerCall(ctx, "/cleanup", nil, nil); e == nil {
			a.DB.Exec(ctx, "UPDATE control.experiments SET state='ABORTED',error='control restarted; resources cleaned',updated_at=now() WHERE state NOT IN ('COMPLETED','FAILED','ABORTED')")
			c.stopLoad(ctx)
		}
	}()
}
func validProfile(p string) bool {
	return p == "baseline" || p == "concurrent" || p == "sustained" || p == "burst" || p == "recovery"
}
func (c *controller) startLoad(ctx context.Context, profile, exp string) (string, error) {
	c.loadMu.Lock()
	defer c.loadMu.Unlock()
	id := platform.ID()
	_, e := c.a.DB.Exec(ctx, "INSERT INTO control.load_runs(id,experiment_id,profile,state) VALUES($1,$2,$3,'RUNNING')", id, exp, profile)
	if e != nil {
		return "", e
	}
	e = c.runnerCall(ctx, "/load/start", map[string]string{"profile": profile}, nil)
	if e != nil {
		c.a.DB.Exec(ctx, "UPDATE control.load_runs SET state='FAILED',ended_at=now() WHERE id=$1", id)
	}
	return id, e
}
func (c *controller) stopLoad(ctx context.Context) error {
	c.loadMu.Lock()
	defer c.loadMu.Unlock()
	e := c.runnerCall(ctx, "/load/stop", nil, nil)
	if e == nil {
		_, e = c.a.DB.Exec(ctx, "UPDATE control.load_runs SET state='STOPPED',ended_at=now() WHERE state='RUNNING'")
	}
	return e
}
func (c *controller) state(ctx context.Context, id, state string) error {
	tx, e := c.a.DB.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	_, e = tx.Exec(ctx, "UPDATE control.experiments SET state=$2,updated_at=now() WHERE id=$1", id, state)
	if e == nil {
		_, e = tx.Exec(ctx, "INSERT INTO control.transitions(experiment_id,state) VALUES($1,$2)", id, state)
	}
	if e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func (c *controller) execute(id string, s Spec) {
	activeExperiment.WithLabelValues(id, s.Fault).Set(1)
	defer activeExperiment.DeleteLabelValues(id, s.Fault)
	ctx, cancel := context.WithCancel(c.a.Ctx)
	c.mu.Lock()
	c.cancels[id] = cancel
	c.mu.Unlock()
	defer func() { cancel(); c.mu.Lock(); delete(c.cancels, id); c.mu.Unlock() }()
	var runErr error
	final := "COMPLETED"
	evidence := []any{}
	defer func() {
		cleanup, x := context.WithTimeout(context.Background(), 30*time.Second)
		defer x()
		if e := c.runnerCall(cleanup, "/cleanup", nil, nil); e != nil {
			runErr = fmt.Errorf("cleanup: %w; job: %v", e, runErr)
			final = "FAILED"
		}
		c.stopLoad(cleanup)
		if runErr != nil {
			if final != "ABORTED" {
				final = "FAILED"
			}
			c.a.DB.Exec(cleanup, "UPDATE control.experiments SET error=$2 WHERE id=$1", id, runErr.Error())
		}
		b, _ := json.Marshal(evidence)
		c.a.DB.Exec(cleanup, "UPDATE control.experiments SET evidence=$2 WHERE id=$1", id, b)
		c.state(cleanup, id, final)
	}()
	if runErr = c.state(ctx, id, "PREPARING"); runErr != nil {
		return
	}
	if runErr = c.runnerCall(ctx, "/cleanup", nil, nil); runErr != nil {
		return
	}
	if _, runErr = c.startLoad(ctx, "sustained", id); runErr != nil {
		return
	}
	wait := func(seconds int) bool {
		t := time.NewTimer(time.Duration(seconds) * time.Second)
		defer t.Stop()
		select {
		case <-ctx.Done():
			final = "ABORTED"
			runErr = ctx.Err()
			return false
		case <-t.C:
			return true
		}
	}
	phase := func(name string, seconds int) {
		snap := c.snapshot(ctx, seconds)
		snap["phase"] = name
		snap["window_end"] = time.Now().UTC()
		snap["window_seconds"] = seconds
		evidence = append(evidence, snap)
		b, _ := json.Marshal(evidence)
		c.a.DB.Exec(ctx, "UPDATE control.experiments SET evidence=$2 WHERE id=$1", id, b)
	}
	if runErr = c.state(ctx, id, "BASELINE"); runErr != nil {
		return
	}
	if !wait(s.Baseline) {
		return
	}
	phase("BASELINE", s.Baseline)
	disruption := time.Now()
	if runErr = c.state(ctx, id, "INJECTING"); runErr != nil {
		return
	}
	var injection map[string]any
	if runErr = c.runnerCall(ctx, "/inject", map[string]any{"fault": s.Fault, "magnitude": s.Magnitude, "duration": s.Duration}, &injection); runErr != nil {
		return
	}
	evidence = append(evidence, map[string]any{"injection": injection})
	if runErr = c.state(ctx, id, "OBSERVING"); runErr != nil {
		return
	}
	observed := 0
	for observed < s.Duration {
		step := 3
		if step > s.Duration-observed {
			step = s.Duration - observed
		}
		if !wait(step) {
			return
		}
		observed += step
		m := c.snapshot(ctx, observed)
		if metric, ok := m["error_rate"].(map[string]any); ok {
			if v, ok := metric["value"].(float64); ok && v > s.StopErrorRate {
				final = "ABORTED"
				runErr = fmt.Errorf("stop condition: infrastructure error rate %.3f exceeds %.3f", v, s.StopErrorRate)
				break
			}
		}
	}
	phase("FAILURE", observed)
	cleanup, x := context.WithTimeout(context.Background(), 30*time.Second)
	defer x()
	if e := c.state(cleanup, id, "RECOVERING"); e != nil {
		runErr = e
		return
	}
	start := time.Now()
	if e := c.runnerCall(cleanup, "/cleanup", nil, nil); e != nil {
		runErr = e
		return
	}
	// Quiesce traffic before evaluating durable backlog and terminal invariants.
	if e := c.stopLoad(cleanup); e != nil {
		runErr = e
		return
	}
	var replayed map[string]any
	replayClient := resilience.New(20 * time.Second)
	if e := replayClient.Call(ctx, "POST", "http://worker:8080/replay", nil, &replayed); e != nil {
		runErr = e
		return
	}
	evidence = append(evidence, map[string]any{"deadletter_replay": replayed})
	if _, e := c.startLoad(ctx, "sustained", id); e != nil {
		runErr = e
		return
	}
	if !wait(s.Recovery) {
		return
	}
	phase("RECOVERY", s.Recovery)
	drainctx, dx := context.WithTimeout(ctx, 30*time.Second)
	if e := c.stopLoad(drainctx); e != nil {
		dx()
		runErr = e
		return
	}
	dx()
	if !wait(3) {
		return
	}
	verifyctx, vx := context.WithTimeout(context.Background(), 30*time.Second)
	defer vx()
	if e := c.state(verifyctx, id, "VERIFYING"); e != nil {
		runErr = e
		return
	}
	var verification map[string]any
	if e := c.runnerCall(verifyctx, "/consistency", nil, &verification); e != nil {
		runErr = e
		return
	}
	verification["restoration_seconds"] = time.Since(start).Seconds()
	verification["rto_seconds"] = time.Since(disruption).Seconds()
	verification["scope"] = "fault injection to verified business consistency"
	evidence = append(evidence, map[string]any{"verification": verification})
	if verification["consistent"] != true {
		runErr = errors.New("business consistency verification failed")
	}
}
func (c *controller) services(ctx context.Context) []any {
	names := []string{"gateway", "ordering", "inventory", "payments", "worker", "control"}
	out := []any{}
	for _, name := range names {
		u := "http://" + name + ":8080/health/ready"
		if name == "inventory" {
			u = platform.Env("INVENTORY_URL", "http://toxiproxy:8081") + "/health/ready"
		}
		if name == "payments" {
			u = platform.Env("PAYMENTS_URL", "http://toxiproxy:8082") + "/health/ready"
		}
		start := time.Now()
		req, _ := http.NewRequestWithContext(ctx, "GET", u, nil)
		client := http.Client{Timeout: 400 * time.Millisecond}
		resp, e := client.Do(req)
		ready := e == nil && resp.StatusCode == 200
		if resp != nil {
			resp.Body.Close()
		}
		out = append(out, map[string]any{"name": name, "ready": ready, "latency_ms": time.Since(start).Milliseconds(), "error": errorText(e)})
	}
	return out
}
func errorText(e error) string {
	if e != nil {
		return e.Error()
	}
	return ""
}
func (c *controller) query(ctx context.Context, q string) map[string]any {
	u := platform.Env("PROMETHEUS_URL", "http://prometheus:9090") + "/api/v1/query?query=" + url.QueryEscape(q)
	req, _ := http.NewRequestWithContext(ctx, "GET", u, nil)
	client := http.Client{Timeout: 2 * time.Second}
	resp, e := client.Do(req)
	if e != nil {
		return map[string]any{"available": false, "query": q, "reason": e.Error()}
	}
	defer resp.Body.Close()
	var result struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Value []any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if json.NewDecoder(resp.Body).Decode(&result) != nil || result.Status != "success" || len(result.Data.Result) == 0 || len(result.Data.Result[0].Value) != 2 {
		return map[string]any{"available": false, "query": q, "reason": "no usable samples"}
	}
	raw, ok := result.Data.Result[0].Value[1].(string)
	if !ok {
		return map[string]any{"available": false, "query": q}
	}
	v, e := strconv.ParseFloat(raw, 64)
	if e != nil || raw == "NaN" || raw == "+Inf" || raw == "-Inf" {
		return map[string]any{"available": false, "query": q, "reason": "insufficient denominator or samples"}
	}
	return map[string]any{"available": true, "value": v, "query": q}
}
func (c *controller) snapshot(ctx context.Context, seconds int) map[string]any {
	var stats map[string]any
	_ = c.a.Client.Call(ctx, "GET", "http://ordering:8080/stats", nil, &stats)
	if seconds < 1 {
		seconds = 1
	}
	window := fmt.Sprintf("[%ds]", seconds)
	requests := "sum(increase(http_requests_total{service=\"gateway\",route=\"/api/orders\",method=\"POST\"}" + window + "))"
	q := map[string]string{"worker_processing_p95": "histogram_quantile(0.95,sum by(le)(rate(worker_processing_duration_seconds_bucket" + window + ")))", "worker_errors": "sum(increase(business_outcomes_total{service=\"worker\",outcome=\"dead_letter\"}" + window + "))", "inventory_failures": "sum(increase(business_outcomes_total{service=\"inventory\",outcome=\"insufficient_stock\"}" + window + "))", "payment_declines": "sum(increase(business_outcomes_total{service=\"payments\",outcome=\"DECLINED\"}" + window + "))", "cache_fallbacks": "sum(increase(business_outcomes_total{service=\"ordering\",outcome=\"cache_fallback\"}" + window + "))", "throughput": requests + "/" + strconv.Itoa(seconds), "error_rate": "sum(increase(http_requests_total{service=\"gateway\",route=\"/api/orders\",method=\"POST\",status=~\"(Internal Server Error|Service Unavailable|Bad Gateway|Gateway Timeout)\"}" + window + "))/" + requests, "completion_rate": "sum(increase(business_outcomes_total{service=\"ordering\",outcome=\"CONFIRMED\"}" + window + "))/" + strconv.Itoa(seconds), "retry_count": "sum(increase(dependency_retries_total" + window + "))", "circuit_transitions": "sum(increase(circuit_transitions_total" + window + "))", "outstanding_orders": "sum(durable_backlog{kind=\"reconciliation\"})", "outbox_backlog": "sum(durable_backlog{kind=\"outbox\"})", "queue_backlog": "sum(rabbitmq_queue_messages{queue=\"orders.confirmed\"})", "dead_letters": "sum(rabbitmq_queue_messages{queue=\"orders.dead\"})", "redeliveries": "sum(increase(rabbitmq_channel_messages_redelivered_total" + window + "))"}
	for _, p := range []struct {
		name string
		q    float64
	}{{"p50", .5}, {"p95", .95}, {"p99", .99}} {
		q[p.name] = fmt.Sprintf("histogram_quantile(%g,sum by(le)(rate(http_request_duration_seconds_bucket{service=\"gateway\",route=\"/api/orders\"}%s)))", p.q, window)
	}
	out := map[string]any{"measured_at": time.Now().UTC(), "window_seconds": seconds, "denominator": "gateway API requests; error rate counts HTTP 5xx; business declines are successful HTTP responses"}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for k, v := range q {
		wg.Add(1)
		go func(k, v string) { defer wg.Done(); result := c.query(ctx, v); mu.Lock(); out[k] = result; mu.Unlock() }(k, v)
	}
	wg.Wait()
	return out
}
func (c *controller) list(w http.ResponseWriter, r *http.Request, kind string) {
	filter := ""
	if kind == "reports" {
		filter = "WHERE state IN ('COMPLETED','FAILED','ABORTED')"
	}
	rows, e := c.a.DB.Query(r.Context(), "SELECT id,state,spec,error,created_at::text,updated_at::text FROM control.experiments "+filter+" ORDER BY created_at DESC LIMIT 100")
	if e != nil {
		platform.Fail(w, 503, e.Error())
		return
	}
	defer rows.Close()
	out := []any{}
	for rows.Next() {
		var id, state, err, created, updated string
		var b []byte
		rows.Scan(&id, &state, &b, &err, &created, &updated)
		out = append(out, map[string]any{"id": id, "state": state, "spec": json.RawMessage(b), "error": err, "created_at": created, "updated_at": updated})
	}
	platform.JSON(w, 200, out)
}
func (c *controller) detail(w http.ResponseWriter, r *http.Request, report bool) {
	var id, state, err string
	var spec, evidence []byte
	e := c.a.DB.QueryRow(r.Context(), "SELECT id,state,spec,evidence,error FROM control.experiments WHERE id=$1", r.PathValue("id")).Scan(&id, &state, &spec, &evidence, &err)
	if e == pgx.ErrNoRows {
		platform.Fail(w, 404, "experiment absent")
		return
	}
	if e != nil {
		platform.Fail(w, 503, e.Error())
		return
	}
	rows, e := c.a.DB.Query(r.Context(), "SELECT state,at::text FROM control.transitions WHERE experiment_id=$1 ORDER BY seq", id)
	history := []any{}
	if e == nil {
		defer rows.Close()
		for rows.Next() {
			var s, at string
			rows.Scan(&s, &at)
			history = append(history, map[string]string{"state": s, "at": at})
		}
	}
	result := map[string]any{"id": id, "state": state, "spec": json.RawMessage(spec), "evidence": json.RawMessage(evidence), "error": err, "transitions": history}
	if report && r.URL.Query().Get("format") == "html" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		b, _ := json.MarshalIndent(result, "", "  ")
		var phases []map[string]any
		json.Unmarshal(evidence, &phases)
		t := template.Must(template.New("report").Parse(`<!doctype html><html lang="en"><meta charset="utf-8"><title>Experiment report</title><style>body{font:16px system-ui;max-width:1100px;margin:32px auto}td,th{padding:8px;border-bottom:1px solid #ddd;text-align:left}pre{white-space:pre-wrap;overflow-wrap:anywhere}</style><h1>Measured experiment report</h1><p>{{.ID}} · {{.State}}</p><p>{{.Error}}</p><table><tr><th>Phase</th><th>Requests/s</th><th>Error fraction</th><th>Completion/s</th><th>p95 seconds</th><th>Queue backlog</th></tr>{{range .Phases}}{{$phase := .}}{{if .phase}}<tr><th>{{.phase}}</th>{{range $key := $.Keys}}<td>{{with index $phase $key}}{{if .available}}{{printf "%.3f" .value}}{{else}}Unavailable{{end}}{{else}}Unavailable{{end}}</td>{{end}}</tr>{{end}}{{end}}</table><h2>Raw query and verification evidence</h2><pre>{{.JSON}}</pre></html>`))
		t.Execute(w, map[string]any{"ID": id, "State": state, "Error": err, "Phases": phases, "Keys": []string{"throughput", "error_rate", "completion_rate", "p95", "queue_backlog"}, "JSON": string(b)})
		return
	}
	if report {
		w.Header().Set("Content-Disposition", "attachment; filename=experiment-"+id+".json")
	}
	platform.JSON(w, 200, result)
}

func (c *controller) monitorLoad() {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-c.a.Ctx.Done():
			return
		case <-t.C:
			if !c.loadMu.TryLock() {
				continue
			}
			ctx, x := context.WithTimeout(c.a.Ctx, 4*time.Second)
			var status struct {
				Running  bool `json:"running"`
				ExitCode int  `json:"exit_code"`
			}
			e := c.runnerCall(ctx, "/load/status", nil, &status)
			if e == nil && !status.Running {
				state := "COMPLETED"
				if status.ExitCode != 0 {
					state = "FAILED"
				}
				c.a.DB.Exec(ctx, "UPDATE control.load_runs SET state=$1,ended_at=now() WHERE state='RUNNING'", state)
			}
			x()
			c.loadMu.Unlock()
		}
	}
}
