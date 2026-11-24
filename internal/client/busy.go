package client

import (
	"bytes"
	"encoding/json/v2"
	"io"
	"net/http"
	"strconv"
	"time"

	backoff "github.com/cenkalti/backoff/v5"
	"go.kenn.io/kata/internal/api"
	"go.kenn.io/kata/internal/httpurl"
)

// RetryBusy retries replayable requests refused by a busy server.
func RetryBusy(c *http.Client) {
	next := c.Transport
	if next == nil {
		next = http.DefaultTransport
	}
	c.Transport = busyTransport{next: next}
}

type busyTransport struct{ next http.RoundTripper }

func (t busyTransport) CloseIdleConnections() {
	if closer, ok := t.next.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

func (t busyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval, bo.MaxInterval, bo.Multiplier, bo.RandomizationFactor = 250*time.Millisecond, time.Second, 2, 0
	bo.Reset()
	request := req
	for attempt := 0; ; attempt++ {
		resp, err := t.next.RoundTrip(request)
		if err != nil || resp == nil || resp.StatusCode != http.StatusTooManyRequests || resp.Body == nil {
			return resp, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body = struct {
			io.Reader
			io.Closer
		}{io.MultiReader(bytes.NewReader(body), resp.Body), resp.Body}
		message, busy := api.BusyRefusal(resp.StatusCode, body, req.URL.Hostname())
		if readErr != nil || !busy {
			return resp, nil
		}
		var envelope api.ErrorEnvelope
		_ = json.Unmarshal(body, &envelope)
		if envelope.Error.Code != api.CodeServerBusy {
			encoded, _ := json.Marshal(api.NewError(http.StatusTooManyRequests, api.CodeServerBusy, message, "", nil).Envelope())
			_ = resp.Body.Close()
			resp.Body = io.NopCloser(bytes.NewReader(encoded))
			resp.ContentLength = int64(len(encoded))
			resp.Header.Set("Content-Type", "application/json")
		}
		if attempt == 3 || (req.Body != nil && req.GetBody == nil) {
			return resp, nil
		}
		delay := bo.NextBackOff()
		value := resp.Header.Get("Retry-After")
		hint := httpurl.ParseRetryAfter(value, time.Now(), time.Duration(1<<63-1), time.Duration(1<<63-1))
		_, numberErr := strconv.ParseUint(value, 10, 64)
		_, dateErr := http.ParseTime(value)
		if hint > 0 || numberErr == nil || dateErr == nil {
			delay = hint
		}
		if deadline, ok := req.Context().Deadline(); ok {
			if time.Until(deadline) <= delay {
				return resp, nil
			}
		} else if delay > DefaultHTTPTimeout {
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
