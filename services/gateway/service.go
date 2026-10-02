package main

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"reliability/internal/platform"
	"strings"
	"time"
)

func setup(a *platform.App) {
	for _, status := range []string{"OK", "Created", "Bad Request", "Conflict", "Internal Server Error", "Service Unavailable", "Bad Gateway", "Gateway Timeout"} {
		platform.Requests.WithLabelValues("gateway", "POST", "/api/orders", status).Add(0)
	}
	transport := &http.Transport{DialContext: (&net.Dialer{Timeout: time.Second}).DialContext, ResponseHeaderTimeout: 20 * time.Second, IdleConnTimeout: 30 * time.Second}
	controlTransport := transport.Clone()
	controlTransport.ResponseHeaderTimeout = 180 * time.Second
	a.Mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		target := platform.Env("ORDERING_URL", "http://ordering:8080")
		path := strings.TrimPrefix(r.URL.Path, "/api")
		if strings.HasPrefix(path, "/control/") {
			target = platform.Env("CONTROL_URL", "http://control:8080")
			path = strings.TrimPrefix(path, "/control")
			if path == "/metrics" {
				path = "/operational-metrics"
			}
		} else if path == "/products" || path == "/inventory" {
			target = platform.Env("INVENTORY_URL", "http://toxiproxy:8081")
			path = "/products"
		} else if !strings.HasPrefix(path, "/orders") && !strings.HasPrefix(path, "/transactions/") {
			platform.Fail(w, 404, "unknown API route")
			return
		}
		u, e := url.Parse(target)
		if e != nil {
			platform.Fail(w, 500, "invalid configured dependency")
			return
		}
		proxy := httputil.NewSingleHostReverseProxy(u)
		proxy.Transport = transport
		if target == platform.Env("CONTROL_URL", "http://control:8080") {
			proxy.Transport = controlTransport
		}
		proxy.ModifyResponse = func(resp *http.Response) error {
			resp.Header.Del("X-Correlation-ID")
			resp.Header.Del("X-Trace-ID")
			return nil
		}
		forward := r.Clone(r.Context())
		forwardURL := *r.URL
		forward.URL = &forwardURL
		forward.URL.Path = path
		forward.Header.Set("X-Correlation-ID", w.Header().Get("X-Correlation-ID"))
		otel.GetTextMapPropagator().Inject(r.Context(), propagation.HeaderCarrier(forward.Header))
		proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, e error) { platform.Fail(w, 503, e.Error()) }
		proxy.ServeHTTP(w, forward)
	})
}
