package resilience

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCircuitCountsDependencyFailures(t *testing.T) {
	status := http.StatusNotFound
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(status)
	}))
	defer server.Close()
	client := New(time.Second)
	for i := 0; i < 4; i++ {
		err := client.Call(context.Background(), "GET", server.URL, nil, nil)
		var response *Error
		if !errors.As(err, &response) || response.Status != 404 {
			t.Fatalf("lookup miss opened circuit: %v", err)
		}
	}
	status = http.StatusServiceUnavailable
	for i := 0; i < 3; i++ {
		_ = client.Call(context.Background(), "GET", server.URL, nil, nil)
	}
	before := calls
	_ = client.Call(context.Background(), "GET", server.URL, nil, nil)
	if calls != before {
		t.Fatal("open circuit called dependency")
	}
	client.mu.Lock()
	for _, c := range client.circuits {
		c.until = time.Now().Add(-time.Second)
	}
	client.mu.Unlock()
	status = http.StatusNoContent
	if err := client.Call(context.Background(), "GET", server.URL, nil, nil); err != nil {
		t.Fatalf("half-open recovery: %v", err)
	}
	if err := client.Call(context.Background(), "GET", server.URL, nil, nil); err != nil {
		t.Fatalf("closed circuit: %v", err)
	}
}
