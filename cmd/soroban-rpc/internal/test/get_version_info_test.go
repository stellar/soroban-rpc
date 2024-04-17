package test

import (
	"context"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/jhttp"
	"github.com/stretchr/testify/assert"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/methods"
)

func TestGetVersionInfoSucceeds(t *testing.T) {
	test := NewTest(t, nil)

	ch := jhttp.NewChannel(test.sorobanRPCURL(), nil)
	client := jrpc2.NewClient(ch, nil)
	request := methods.GetVersionInfoRequest{}

	var result methods.GetVersionInfoResponse
	err := client.CallResult(context.Background(), "getVersionInfo", request, &result)
	assert.NoError(t, err)

	assert.NotEmpty(t, result.Version)
	assert.NotEmpty(t, result.BuildTimestamp)
	assert.NotEmpty(t, result.CommitHash)
	assert.NotEmpty(t, result.CaptiveCoreVersion)
}
