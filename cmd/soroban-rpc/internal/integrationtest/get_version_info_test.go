package integrationtest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/config"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/integrationtest/infrastructure"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/methods"
)

func init() {
	// Initialize variables to non-empty values
	config.CommitHash = "commitHash"
	config.BuildTimestamp = "buildTimestamp"
}

func TestGetVersionInfoSucceeds(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	var result methods.GetVersionInfoResponse
	err := test.GetRPCLient().CallResult(context.Background(), "getVersionInfo", nil, &result)
	assert.NoError(t, err)

	assert.Equal(t, "0.0.0", result.Version)
	assert.Equal(t, "buildTimestamp", result.BuildTimestamp)
	assert.Equal(t, "commitHash", result.CommitHash)
	assert.Equal(t, test.GetProtocolVersion(), result.ProtocolVersion)
	assert.NotEmpty(t, result.CaptiveCoreVersion)
}
