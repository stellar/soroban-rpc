package integrationtest

import (
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/integrationtest/infrastructure"
)

func TestArchiveUserAgent(t *testing.T) {
	ports := infrastructure.NewTestPorts(t)
	archiveHost := net.JoinHostPort("localhost", strconv.Itoa(int(ports.CoreArchivePort)))
	proxy := httputil.NewSingleHostReverseProxy(&url.URL{Scheme: "http", Host: archiveHost})
	userAgents := sync.Map{}
	historyArchiveProxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userAgents.Store(r.Header["User-Agent"][0], "")
		proxy.ServeHTTP(w, r)
	}))
	defer historyArchiveProxy.Close()

	cfg := &infrastructure.TestConfig{
		TestPorts:         &ports,
		HistoryArchiveURL: historyArchiveProxy.URL,
	}

	infrastructure.NewTest(t, cfg)

	_, ok := userAgents.Load("soroban-rpc/0.0.0")
	assert.True(t, ok, "rpc service should set user agent for history archives")

	_, ok = userAgents.Load("soroban-rpc/0.0.0/captivecore")
	assert.True(t, ok, "rpc captive core should set user agent for history archives")
}
