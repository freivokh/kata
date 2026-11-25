package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAttachmentUploadRetriesBusy(t *testing.T) {
	t.Setenv("KATA_HOME", t.TempDir())
	filename := filepath.Join(t.TempDir(), "example.txt")
	require.NoError(t, os.WriteFile(filename, []byte("attachment content"), 0600))
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		assert.Equal(t, "attachment content", string(body))
		if calls == 1 {
			w.WriteHeader(429)
			_, _ = w.Write([]byte("Rate exceeded."))
			return
		}
		_, _ = w.Write([]byte(`{"attachment_id":"file-id"}`))
	}))
	t.Cleanup(srv.Close)
	hc, err := longRunningClientFor(t.Context(), srv.URL)
	require.NoError(t, err)
	id, err := uploadAttachmentCLI(t.Context(), hc, srv.URL, 1, "issue", "agent", filename, false)
	require.NoError(t, err)
	assert.Equal(t, "file-id", id)
	assert.Equal(t, 2, calls)
}

func TestAttachmentUploadDoesNotTimeoutBeforeBusy(t *testing.T) {
	t.Setenv("KATA_HOME", t.TempDir())
	t.Setenv("KATA_HTTP_TIMEOUT", "100ms")
	filename := filepath.Join(t.TempDir(), "example.txt")
	require.NoError(t, os.WriteFile(filename, []byte("attachment content"), 0600))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(200 * time.Millisecond): //nolint:kennlint // deadlinetest: the upload must outlive the shorter ordinary request timeout.
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write([]byte(`{"attachment_id":"file-id"}`))
	}))
	t.Cleanup(srv.Close)
	hc, err := longRunningClientFor(t.Context(), srv.URL)
	require.NoError(t, err)
	id, err := uploadAttachmentCLI(t.Context(), hc, srv.URL, 1, "issue", "agent", filename, false)
	require.NoError(t, err)
	assert.Equal(t, "file-id", id)
}
