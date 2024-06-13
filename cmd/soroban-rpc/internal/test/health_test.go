package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/ledgerbucketwindow"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/methods"
)

func TestHealth(t *testing.T) {
	test := NewTest(t, nil)

	client := test.GetRPCLient()

	var result methods.HealthCheckResult
	if err := client.CallResult(context.Background(), "getHealth", nil, &result); err != nil {
		t.Fatalf("rpc call failed: %v", err)
	}
	assert.Equal(t, "healthy", result.Status)
	assert.Equal(t, uint32(ledgerbucketwindow.OneDayOfLedgers), result.LedgerRetentionWindow)
	assert.Greater(t, result.OldestLedger, uint32(0))
	assert.Greater(t, result.LatestLedger, uint32(0))
	assert.GreaterOrEqual(t, result.LatestLedger, result.OldestLedger)
}
