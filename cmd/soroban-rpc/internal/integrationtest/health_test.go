package integrationtest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/config"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/integrationtest/infrastructure"
)

func TestHealth(t *testing.T) {
	test := infrastructure.NewTest(t, nil)
	result, err := test.GetRPCHealth()
	require.NoError(t, err)
	assert.Equal(t, "healthy", result.Status)
	assert.Equal(t, uint32(config.OneDayOfLedgers), result.LedgerRetentionWindow)
	assert.Greater(t, result.OldestLedger, uint32(0))
	assert.Greater(t, result.LatestLedger, uint32(0))
	assert.GreaterOrEqual(t, result.LatestLedger, result.OldestLedger)
}
