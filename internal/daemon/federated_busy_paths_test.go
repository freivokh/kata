package daemon_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.kenn.io/kata/internal/config"
)

func TestFederatedBusyPaths(t *testing.T) {
	for _, mode := range []string{"docbank", "deleted history", "search"} {
		t.Run(mode, func(t *testing.T) {
			hub, spoke, project, replica, issue, token := createClaimForwardingPair(t, "claim,push,pull")
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				busy := mode == "docbank" && strings.Contains(r.URL.Path, "/federation/docbank/") || mode == "deleted history" && strings.HasSuffix(r.URL.Path, "/lease") || mode == "search" && strings.HasSuffix(r.URL.Path, "/federation/search")
				if busy {
					w.Header().Set("Retry-After", "2")
					w.WriteHeader(429)
					_, _ = w.Write([]byte("Rate exceeded."))
					return
				}
				req := r.Clone(r.Context())
				req.RequestURI = ""
				req.URL.Scheme = "http"
				req.URL.Host = hub.URL[len("http://"):]
				req.Host = req.URL.Host
				resp, err := hub.HTTP.Do(req) //nolint:gosec // loopback fixture
				if err != nil {
					http.Error(w, err.Error(), 500)
					return
				}
				defer func() { _ = resp.Body.Close() }()
				for k, values := range resp.Header {
					for _, value := range values {
						w.Header().Add(k, value)
					}
				}
				w.WriteHeader(resp.StatusCode)
				_, _ = io.Copy(w, resp.Body)
			}))
			t.Cleanup(proxy.Close)
			setClaimBindingHubURL(t, spoke, replica.ID, proxy.URL)
			require.NoError(t, config.WriteFederationCredential(replica.UID, config.FederationCredential{HubURL: proxy.URL, HubProjectID: project.ID, Token: token, Actor: "tester", Capabilities: "claim,push,pull"}))
			path := ""
			switch mode {
			case "docbank":
				path = fmt.Sprintf("/api/v1/projects/%d/issues/%s/attachments/links", replica.ID, issue.ShortID)
			case "deleted history":
				_, _, _, err := spoke.DB.SoftDeleteIssue(t.Context(), issue.ID, "tester")
				require.NoError(t, err)
				path = fmt.Sprintf("/api/v1/projects/%d/events?issue=%s&actor=tester", replica.ID, issue.ShortID)
			case "search":
				path = fmt.Sprintf("/api/v1/projects/%d/search?q=claim&mode=semantic", replica.ID)
			}
			var resp *http.Response
			var raw []byte
			if mode == "docbank" {
				resp, raw = envDoRaw(t, spoke, http.MethodPost, path, map[string]string{"path": "/example.txt", "actor": "tester"}, nil)
			} else {
				resp, raw = envGetRaw(t, spoke, path)
			}
			assertAPIError(t, resp.StatusCode, raw, http.StatusTooManyRequests, "server_busy")
			require.Contains(t, string(raw), "127.0.0.1 is busy")
			require.Equal(t, "2", resp.Header.Get("Retry-After"))
			if mode == "search" {
				resp, raw = envGetRaw(t, spoke, fmt.Sprintf("/api/v1/projects/%d/search?q=claim", replica.ID))
				require.Equal(t, 200, resp.StatusCode, string(raw))
				require.Contains(t, string(raw), `"degraded":true`)
				require.Contains(t, string(raw), "claim target")
			}
		})
	}
}
