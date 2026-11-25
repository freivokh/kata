package daemon_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.kenn.io/kata/internal/config"
)

func TestFederationBridgeBusyResume(t *testing.T) {
	for _, stage := range []string{"instance", "enrollment", "disconnect"} {
		t.Run(stage, func(t *testing.T) {
			t.Setenv("KATA_HOME", t.TempDir())
			hub := newBridgeHubFixture(t)
			var armed atomic.Bool
			var refused atomic.Int32
			path := map[string]string{
				"instance":   "/api/v1/instance",
				"enrollment": "/api/v1/federation/enrollments",
				"disconnect": fmt.Sprintf("/api/v1/projects/%d/federation/relay:disconnect", hub.root.private.ID),
			}[stage]
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if armed.Load() && r.URL.Path == path && (refused.Add(1) == 1 || stage == "enrollment" && refused.Load() == 2) {
					w.Header().Set("Retry-After", "2")
					w.WriteHeader(429)
					_, _ = w.Write([]byte("Rate exceeded."))
					return
				}
				req := r.Clone(r.Context())
				req.RequestURI = ""
				req.URL.Scheme = "http"
				req.URL.Host = hub.root.server.Listener.Addr().String()
				req.Host = req.URL.Host
				req.Header.Del("Accept-Encoding")
				resp, err := hub.root.server.Client().Do(req) //nolint:gosec // loopback fixture
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
			local := newBridgeLocalDaemon(t, openReplicaServiceStore(t), hub.catalog(proxy.URL))
			endpoint := "/api/v1/federation/bridges"
			body := bridgeConnectBody(hub.root.private.Name, false)
			if stage == "disconnect" {
				code, _, raw := local.request(t, http.MethodPost, endpoint, "", body, bridgeOwnerHeaders)
				require.Equal(t, 200, code, string(raw))
				endpoint = "/api/v1/federation/bridges/shared-replica/disconnect"
				body = map[string]any{}
			}
			armed.Store(true)
			code, header, raw := local.request(t, http.MethodPost, endpoint, "", body, bridgeOwnerHeaders)
			require.Equal(t, 429, code, string(raw))
			require.Equal(t, "server_busy", bridgeErrorCode(t, raw))
			require.Contains(t, string(raw), "127.0.0.1 is busy")
			require.Equal(t, "2", header.Get("Retry-After"))
			credential, found, err := config.DefaultFederationCredentialStore().FederationCredential(t.Context(), hub.root.private.UID)
			require.NoError(t, err)
			if stage == "enrollment" {
				require.True(t, found)
				require.True(t, credential.RelayEnrollmentPending)
				code, _, raw = local.request(t, http.MethodPost, endpoint, "", body, bridgeOwnerHeaders)
				require.Equal(t, 429, code, string(raw))
				pending, found, err := config.DefaultFederationCredentialStore().FederationCredential(t.Context(), hub.root.private.UID)
				require.NoError(t, err)
				require.True(t, found)
				require.True(t, pending.RelayEnrollmentPending)
				require.Equal(t, credential.Token, pending.Token)
			}
			if stage == "disconnect" {
				require.True(t, found)
				require.True(t, credential.LeavePending)
			}
			code, _, raw = local.request(t, http.MethodPost, endpoint, "", body, bridgeOwnerHeaders)
			require.Equal(t, 200, code, string(raw))
			if stage == "enrollment" {
				after, found, err := config.DefaultFederationCredentialStore().FederationCredential(t.Context(), hub.root.private.UID)
				require.NoError(t, err)
				require.True(t, found)
				require.False(t, after.RelayEnrollmentPending)
				require.Equal(t, credential.Token, after.Token)
			}
			if stage == "disconnect" {
				_, found, err := config.DefaultFederationCredentialStore().FederationCredential(t.Context(), hub.root.private.UID)
				require.NoError(t, err)
				require.False(t, found)
				code, _, raw = local.request(t, http.MethodPost, "/api/v1/federation/bridges", "", bridgeConnectBody(hub.root.private.Name, false), bridgeOwnerHeaders)
				require.Equal(t, 200, code, strings.TrimSpace(string(raw)))
			}
		})
	}
}
