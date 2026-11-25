package main

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
		wait                   time.Duration
	}{
		{"admission exhausted", `{"error":{"code":"admission_limited"}}`, "2", time.Second, 1, false, false, "127.0.0.1 is busy", 0},
		{"rule", `{"error":{"code":"sibling_throttle","message":"wait"}}`, "", time.Second, 1, false, false, "sibling_throttle", 0},
		{"relay", `{"error":{"code":"server_busy","message":"root.example is busy; try again in a minute"}}`, "0", time.Second, 4, false, false, "root.example is busy", 0},
		{"attempt cap", "Rate exceeded.", "0", time.Second, 4, false, false, "127.0.0.1 is busy", 0},
		{"non-replayable", "Rate exceeded.", "", time.Second, 1, false, true, "127.0.0.1 is busy", 0},
		{"hint waits", "Rate exceeded.", "1", 3 * time.Second, 2, true, false, "", time.Second},
		{"unbounded request long hint", "Rate exceeded.", "60", 0, 1, false, false, "127.0.0.1 is busy", 0},
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
			RetryBusyWithBudget(hc, BusyRetryBudget)
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
			if tc.wait > 0 {
				assert.GreaterOrEqual(t, time.Since(start), tc.wait)
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
		RetryBusyWithBudget(hc, BusyRetryBudget)
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

type busyTimeoutTransport struct{ calls int }

func (t *busyTimeoutTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.calls++
	if t.calls == 1 {
		return &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": []string{"1"}}, Body: io.NopCloser(strings.NewReader("Rate exceeded.")), Request: req}, nil
	}
	<-req.Context().Done()
	return nil, req.Context().Err()
}

func TestRetryBusyReportsBusyWhenRetryTimesOut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		transport := &busyTimeoutTransport{}
		hc := &http.Client{Transport: transport, Timeout: 2 * time.Second}
		RetryBusyWithBudget(hc, BusyRetryBudget)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://hub.example", nil)
		require.NoError(t, err)
		resp, err := hc.Do(req) //nolint:gosec // fake transport
		require.NoError(t, err)
		require.Equal(t, 429, resp.StatusCode)
		require.Equal(t, 2, transport.calls)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		cliErr := apiErrFromBody(resp.StatusCode, body)
		assert.Equal(t, kindServerBusy, cliErr.Kind)
		assert.Equal(t, ExitDaemonUnavail, cliErr.ExitCode)
		assert.Contains(t, cliErr.Message, "hub.example is busy")
	})
}

func TestRetryBusyWriteRetryTimeoutReturnsError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		transport := &busyTimeoutTransport{}
		hc := &http.Client{Transport: transport, Timeout: 2 * time.Second}
		RetryBusyWithBudget(hc, BusyRetryBudget)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "https://hub.example", strings.NewReader("payload"))
		require.NoError(t, err)
		_, err = hc.Do(req) //nolint:gosec // fake transport
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, 2, transport.calls)
	})
}
