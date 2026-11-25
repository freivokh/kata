package client

import (
	"bytes"
	"encoding/json/v2"
	"io"
	"net/http"
	"strings"
	"time"

	backoff "github.com/cenkalti/backoff/v5"
	"go.kenn.io/kata/internal/api"
	"go.kenn.io/kata/internal/httpurl"
)

// BusyRetryBudget leaves room for a five-second hub hint and a retry.
const BusyRetryBudget = 11 * time.Second

// RetryBusy retries replayable requests refused by a busy server.
func RetryBusy(c *http.Client) {
	RetryBusyWithBudget(c, BusyRetryBudget)
}

// RetryBusyWithBudget bounds when another retry may start after a refusal.
func RetryBusyWithBudget(c *http.Client, budget time.Duration) {
	configureBusy(c, budget, false)
}

// RetryBusyStream lets a long-lived stream reconnect after busy retries.
func RetryBusyStream(c *http.Client, budget time.Duration) {
	configureBusy(c, budget, true)
}

func configureBusy(c *http.Client, budget time.Duration, stream bool) {
	next := c.Transport
	if next == nil {
		next = http.DefaultTransport
	}
	c.Transport = busyTransport{next: next, budget: budget, stream: stream}
}

type busyTransport struct {
	next   http.RoundTripper
	budget time.Duration
	stream bool
}

// BusyStreamError lets a tail reconnect without treating capacity as a rule refusal.
type BusyStreamError struct {
	Message    string
	RetryAfter time.Duration
}

func (e *BusyStreamError) Error() string { return e.Message }

func (t busyTransport) CloseIdleConnections() {
	if closer, ok := t.next.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

func (t busyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var deadline time.Time
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval, bo.MaxInterval, bo.Multiplier, bo.RandomizationFactor = 250*time.Millisecond, time.Second, 2, 0
	bo.Reset()
	request := req
	for attempt := 0; ; attempt++ {
		resp, err := t.next.RoundTrip(request)
		if err != nil {
			return resp, err
		}
		message, code, busy := api.InspectBusyResponse(resp, req.URL.Hostname())
		if !busy {
			return resp, nil
		}
		if attempt == 0 {
			deadline = time.Now().Add(t.budget)
		}
		if code != api.CodeServerBusy {
			encoded, _ := json.Marshal(api.NewError(http.StatusTooManyRequests, api.CodeServerBusy, message, "", nil).Envelope())
			_ = resp.Body.Close()
			resp.Body = io.NopCloser(bytes.NewReader(encoded))
			resp.ContentLength = int64(len(encoded))
			resp.Header.Set("Content-Type", "application/json")
		}
		delay := bo.NextBackOff()
		value := resp.Header.Get("Retry-After")
		hint := httpurl.ParseRetryAfter(value, time.Now(), time.Duration(1<<63-1), time.Duration(1<<63-1))
		_, valid := httpurl.RetryAfterWait(value, time.Now())
		if value != strings.TrimSpace(value) || strings.HasPrefix(value, "+") || strings.HasPrefix(value, "-") {
			valid = false
		}
		if hint > 0 || valid {
			delay = hint
		}
		if attempt == 3 || (req.Body != nil && req.GetBody == nil) {
			if t.stream {
				_ = resp.Body.Close()
				return nil, &BusyStreamError{Message: message, RetryAfter: delay}
			}
			return resp, nil
		}
		if callerDeadline, ok := req.Context().Deadline(); ok {
			if time.Until(callerDeadline) <= delay {
				if t.stream {
					_ = resp.Body.Close()
					return nil, &BusyStreamError{Message: message, RetryAfter: delay}
				}
				return resp, nil
			}
		}
		if t.budget > 0 && time.Until(deadline) <= delay {
			if t.stream {
				_ = resp.Body.Close()
				return nil, &BusyStreamError{Message: message, RetryAfter: delay}
			}
			return resp, nil
		}
		_ = resp.Body.Close()
		timer := time.NewTimer(delay)
		select {
		case <-req.Context().Done():
			timer.Stop()
			return nil, req.Context().Err()
		case <-timer.C:
		}
		request = req.Clone(req.Context())
		if req.Body != nil {
			request.Body, err = req.GetBody()
			if err != nil {
				return nil, err
			}
		}
	}
}
