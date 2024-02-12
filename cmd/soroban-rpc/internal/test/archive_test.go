package test

import (
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestArchiveUserAgent(t *testing.T) {
	userAgents := sync.Map{}
	cfg := &TestConfig{
		historyArchiveProxyCallback: func(r *http.Request) {
			userAgents.Store(r.Header["User-Agent"][0], "")
		},
	}
	NewTest(t, cfg)

	_, ok := userAgents.Load("testing")
	assert.True(t, ok, "rpc service should set user agent for history archives")

	_, ok = userAgents.Load("testing/captivecore")
	assert.True(t, ok, "rpc captive core should set user agent for history archives")
}
