package client

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryBusy(t *testing.T) {
	for _, tc := range []struct {
		name, body, retryAfter string
		timeout                time.Duration
		wantCalls              int
		recover, noReplay      bool
		want                   string
	}{
		{"proxy", "Rate exceeded.", "", time.Second, 2, true, false, ""},
		{"admission", `{"error":{"code":"admission_limited"}}`, "", time.Second, 2, true, false, ""},
		{"admission exhausted", `{"error":{"code":"admission_limited"}}`, "2", time.Second, 1, false, false, "127.0.0.1 is busy"},
		{"rule", `{"error":{"code":"sibling_throttle","message":"wait"}}`, "", time.Second, 1, false, false, "sibling_throttle"},
		{"relay", `{"error":{"code":"server_busy","message":"root.example is busy; try again in a minute"}}`, "", time.Second, 2, true, false, ""},
		{"attempt cap", "Rate exceeded.", "0", time.Second, 4, false, false, "127.0.0.1 is busy"},
		{"non-replayable", "Rate exceeded.", "", time.Second, 1, false, true, "127.0.0.1 is busy"},
		{"hint waits", "Rate exceeded.", "1", 3 * time.Second, 2, true, false, ""},
		{"zero hint", "Rate exceeded.", "0", time.Second, 2, true, false, ""},
		{"hint exceeds budget", "Rate exceeded.", "2", time.Second, 1, false, false, "127.0.0.1 is busy"},
		{"unbounded request long hint", "Rate exceeded.", "60", 0, 1, false, false, "127.0.0.1 is busy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				body, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				assert.Equal(t, "payload", string(body))
				if !tc.recover || calls == 1 {
					if tc.retryAfter != "" {
						w.Header().Set("Retry-After", tc.retryAfter)
					}
					w.WriteHeader(429)
					_, _ = w.Write([]byte(tc.body))
					return
				}
				w.WriteHeader(200)
			}))
			t.Cleanup(server.Close)
			hc := &http.Client{Timeout: tc.timeout}
			RetryBusy(hc)
			var body io.Reader = strings.NewReader("payload")
			if tc.noReplay {
				body = io.NopCloser(body)
			}
			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL, body)
			require.NoError(t, err)
			start := time.Now()
			resp, err := hc.Do(req) //nolint:gosec // loopback fixture
			require.NoError(t, err)
			raw, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			assert.Equal(t, tc.wantCalls, calls)
			if tc.want != "" {
				assert.Contains(t, string(raw), tc.want)
			}
			if tc.name == "hint waits" {
				assert.GreaterOrEqual(t, time.Since(start), time.Second)
			}
		})
	}
}

type busyCloseBody struct {
	io.Reader
	closed chan struct{}
}

func (b *busyCloseBody) Close() error { close(b.closed); return nil }

type busyCloseTransport struct{ body *busyCloseBody }

func (t busyCloseTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": []string{"10"}}, Body: t.body, Request: req}, nil
}

func TestRetryBusyClosesResponseBeforeDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		body := &busyCloseBody{Reader: strings.NewReader(`{"error":{"code":"server_busy","message":"hub.example is busy"}}`), closed: make(chan struct{})}
		hc := &http.Client{Transport: busyCloseTransport{body: body}, Timeout: 20 * time.Second}
		RetryBusy(hc)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://hub.example", nil)
		require.NoError(t, err)
		done := make(chan error, 1)
		go func() { _, err := hc.Do(req); done <- err }()
		synctest.Wait()
		select {
		case <-body.closed:
		default:
			t.Error("response stayed open during the retry delay")
		}
		cancel()
		<-done
	})
}
