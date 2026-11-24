package daemon

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHubBusyForwarding(t *testing.T) {
	for _, tc := range []struct {
		name, body  string
		final, want int
		code        string
	}{
		{"refusal", "Rate exceeded.", 502, 429, "server_busy"},
		{"rule", `{"error":{"code":"sibling_throttle"}}`, 502, 502, ""},
		{"fallback", "Rate exceeded.", 200, 200, ""},
		{"relay", `{"error":{"code":"server_busy","message":"root.example is busy; try again in a minute"}}`, 404, 429, "root.example is busy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Retry-After", "60")
				w.WriteHeader(429)
				_, _ = w.Write([]byte(tc.body))
			}))
			t.Cleanup(hub.Close)
			hc := hub.Client()
			observeHubBusy(hc)
			handler := withHubBusy(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, hub.URL, nil)
				require.NoError(t, err)
				response, err := hc.Do(req) //nolint:gosec // loopback fixture
				require.NoError(t, err)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				require.NoError(t, response.Body.Close())
				assert.Equal(t, tc.body, string(body))
				w.WriteHeader(tc.final)
				_, _ = w.Write([]byte("original"))
			}))
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
			assert.Equal(t, tc.want, recorder.Code)
			if tc.want == 429 {
				assert.Equal(t, "60", recorder.Header().Get("Retry-After"))
				assert.Contains(t, recorder.Body.String(), tc.code)
			} else {
				assert.Equal(t, "original", recorder.Body.String())
			}
		})
	}
}

type busyHijackWriter struct {
	http.ResponseWriter
	conn net.Conn
}

func (w *busyHijackWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.conn, bufio.NewReadWriter(bufio.NewReader(w.conn), bufio.NewWriter(w.conn)), nil
}

func TestHubBusyWriterHijack(t *testing.T) {
	server, peer := net.Pipe()
	t.Cleanup(func() { _ = server.Close(); _ = peer.Close() })
	underlying := &busyHijackWriter{ResponseWriter: httptest.NewRecorder(), conn: server}
	wrapped := &hubBusyWriter{ResponseWriter: underlying, record: &hubBusyRecord{}}
	conn, _, err := wrapped.Hijack()
	require.NoError(t, err)
	assert.Equal(t, server, conn)
}
