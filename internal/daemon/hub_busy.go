package daemon

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"sync"

	"go.kenn.io/kata/internal/api"
)

type hubBusyKey struct{}

type hubBusyRecord struct {
	mu         sync.Mutex
	message    string
	retryAfter string
}

type hubBusyTransport struct{ next http.RoundTripper }

func observeHubBusy(c *http.Client) {
	transport := c.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	c.Transport = hubBusyTransport{next: transport}
}

func (t hubBusyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.next.RoundTrip(req)
	record, _ := req.Context().Value(hubBusyKey{}).(*hubBusyRecord)
	if err != nil || resp == nil || resp.StatusCode != http.StatusTooManyRequests || resp.Body == nil || record == nil {
		return resp, err
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
	resp.Body = struct {
		io.Reader
		io.Closer
	}{io.MultiReader(bytes.NewReader(body), resp.Body), resp.Body}
	if readErr != nil {
		return resp, nil
	}
	if message, busy := api.BusyRefusal(resp.StatusCode, body, req.URL.Hostname()); busy {
		record.mu.Lock()
		if record.message == "" {
			record.message, record.retryAfter = message, resp.Header.Get("Retry-After")
		}
		record.mu.Unlock()
	}
	return resp, nil
}

func (t hubBusyTransport) CloseIdleConnections() {
	if closer, ok := t.next.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

func withHubBusy(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		record := &hubBusyRecord{}
		next.ServeHTTP(&hubBusyWriter{ResponseWriter: w, record: record}, r.WithContext(context.WithValue(r.Context(), hubBusyKey{}, record)))
	})
}

type hubBusyWriter struct {
	http.ResponseWriter
	record   *hubBusyRecord
	wrote    bool
	replaced bool
}

func (w *hubBusyWriter) WriteHeader(status int) {
	if w.wrote {
		return
	}
	w.wrote = true
	w.record.mu.Lock()
	message, retryAfter := w.record.message, w.record.retryAfter
	w.record.mu.Unlock()
	if status >= 400 && message != "" {
		w.replaced = true
		w.Header().Del("Content-Length")
		w.Header().Del("Content-Encoding")
		if retryAfter != "" {
			w.Header().Set("Retry-After", retryAfter)
		}
		api.WriteEnvelope(w.ResponseWriter, http.StatusTooManyRequests, api.CodeServerBusy, message)
		return
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *hubBusyWriter) Write(p []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	if w.replaced {
		return len(p), nil
	}
	return w.ResponseWriter.Write(p)
}

func (w *hubBusyWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *hubBusyWriter) Flush() {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}
func (w *hubBusyWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return http.NewResponseController(w.ResponseWriter).Hijack()
}
