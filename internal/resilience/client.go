package resilience

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"io"
	"math/rand"
	"net/http"
	urlpkg "net/url"
	"sync"
	"time"
)

var Retries = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "dependency_retries_total", Help: "Bounded transient retries"}, []string{"dependency"})
var Circuits = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "circuit_state", Help: "0 closed, 1 open, 2 half open"}, []string{"dependency"})
var Transitions = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "circuit_transitions_total", Help: "Circuit state changes"}, []string{"dependency"})

func init() { prometheus.MustRegister(Retries, Circuits, Transitions) }

type circuit struct {
	failures int
	until    time.Time
	probing  bool
}
type Client struct {
	mu       sync.Mutex
	circuits map[string]*circuit
	slots    chan struct{}
	timeout  time.Duration
	timeouts map[string]time.Duration
}

func New(timeout time.Duration) *Client {
	return &Client{circuits: map[string]*circuit{}, slots: make(chan struct{}, 16), timeout: timeout, timeouts: map[string]time.Duration{}}
}

type Error struct {
	Status  int
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("dependency status %d: %s", e.Status, e.Message) }
func (c *Client) SetTimeout(host string, d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.timeouts[host] = d
}
func (c *Client) Call(ctx context.Context, method, url string, in, out any) error {
	select {
	case c.slots <- struct{}{}:
		defer func() { <-c.slots }()
	case <-ctx.Done():
		return ctx.Err()
	}
	parsed, parseErr := urlpkg.Parse(url)
	if parseErr != nil {
		return parseErr
	}
	dependency := parsed.Host
	c.mu.Lock()
	timeout := c.timeout
	if d, ok := c.timeouts[dependency]; ok {
		timeout = d
	}
	b := c.circuits[dependency]
	if b == nil {
		b = &circuit{}
		c.circuits[dependency] = b
	}
	if !b.until.IsZero() {
		if time.Now().Before(b.until) || b.probing {
			c.mu.Unlock()
			return &Error{503, "circuit open"}
		}
		b.probing = true
		Circuits.WithLabelValues(dependency).Set(2)
		Transitions.WithLabelValues(dependency).Inc()
	}
	c.mu.Unlock()
	payload, err := json.Marshal(in)
	if err != nil {
		return err
	}
	var final error
	for attempt := 0; attempt < 3; attempt++ {
		callctx, cancel := context.WithTimeout(ctx, timeout)
		callctx, span := otel.Tracer("dependency").Start(callctx, method+" "+url)
		req, e := http.NewRequestWithContext(callctx, method, url, bytes.NewReader(payload))
		if e != nil {
			span.End()
			cancel()
			return e
		}
		req.Header.Set("Content-Type", "application/json")
		otel.GetTextMapPropagator().Inject(callctx, propagation.HeaderCarrier(req.Header))
		resp, e := http.DefaultClient.Do(req)
		status := 0
		if e == nil {
			status = resp.StatusCode
			data, readErr := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
			resp.Body.Close()
			if readErr != nil {
				e = readErr
			} else if status >= 300 {
				e = &Error{status, string(data)}
			} else if out != nil {
				e = json.Unmarshal(data, out)
			}
		}
		if e != nil {
			span.RecordError(e)
		}
		span.End()
		cancel()
		final = e
		if e == nil || (status >= 400 && status < 500 && status != 429) {
			break
		}
		if attempt < 2 {
			Retries.WithLabelValues(dependency).Inc()
			select {
			case <-ctx.Done():
				final = ctx.Err()
				attempt = 3
			case <-time.After(time.Duration(100*(1<<attempt)+rand.Intn(100)) * time.Millisecond):
			}
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	b.probing = false
	if final != nil {
		b.failures++
		if b.failures >= 3 {
			b.until = time.Now().Add(5 * time.Second)
			Circuits.WithLabelValues(dependency).Set(1)
			Transitions.WithLabelValues(dependency).Inc()
		}
	} else {
		if !b.until.IsZero() {
			Transitions.WithLabelValues(dependency).Inc()
		}
		b.failures = 0
		b.until = time.Time{}
		Circuits.WithLabelValues(dependency).Set(0)
	}
	return final
}
