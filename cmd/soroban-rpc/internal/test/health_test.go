package test

import (
	"context"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/jhttp"
	"github.com/stretchr/testify/assert"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/ledgerbucketwindow"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/methods"
)

func TestHealth(t *testing.T) {
	test := NewTest(t, nil)

	ch := jhttp.NewChannel(test.sorobanRPCURL(), nil)
	client := jrpc2.NewClient(ch, nil)

	var result methods.HealthCheckResult
	if err := client.CallResult(context.Background(), "getHealth", nil, &result); err != nil {
		t.Fatalf("rpc call failed: %v", err)
	}
	assert.Equal(t, "healthy", result.Status)
	assert.Equal(t, uint32(ledgerbucketwindow.DefaultEventLedgerRetentionWindow), result.LedgerRetentionWindow)
	assert.Greater(t, result.OldestLedger, uint32(0))
	assert.Greater(t, result.LatestLedger, uint32(0))
	assert.GreaterOrEqual(t, result.LatestLedger, result.OldestLedger)
}
