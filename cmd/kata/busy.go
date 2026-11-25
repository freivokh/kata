package main

import (
	"bytes"
	"encoding/json/v2"
	"io"
	"net/http"
	"time"

	"go.kenn.io/kata/internal/api"
)

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

func (t busyTransport) CloseIdleConnections() {
	if closer, ok := t.next.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

func (t busyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var retry *BusyRetry
	request := req
	for {
		resp, err := t.next.RoundTrip(request)
		if err != nil {
			return resp, err
		}
		message, code, busy := api.InspectBusyResponse(resp, req.URL.Hostname())
		if !busy {
			return resp, nil
		}
		if retry == nil {
			retry = NewBusyRetry(t.budget)
		}
		if code != api.CodeServerBusy {
			encoded, _ := json.Marshal(api.NewError(http.StatusTooManyRequests, api.CodeServerBusy, message, "", nil).Envelope())
			_ = resp.Body.Close()
			resp.Body = io.NopCloser(bytes.NewReader(encoded))
			resp.ContentLength = int64(len(encoded))
			resp.Header.Set("Content-Type", "application/json")
		}
		delay, retryable := retry.Next(resp.Header.Get("Retry-After"))
		callerDeadline, hasDeadline := req.Context().Deadline()
		if !retryable || (req.Body != nil && req.GetBody == nil) ||
			(hasDeadline && time.Until(callerDeadline) <= delay) {
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
