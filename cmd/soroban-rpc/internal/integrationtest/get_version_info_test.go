package integrationtest

import (
	"context"
	"fmt"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/config"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/integrationtest/infrastructure"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/methods"
)

func TestGetVersionInfoSucceeds(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	version, commitHash, buildTimeStamp := config.Version, config.CommitHash, config.BuildTimestamp

	populateVersionInfo(t)

	// reset to previous config values
	t.Cleanup(func() {
		config.Version = version
		config.CommitHash = commitHash
		config.BuildTimestamp = buildTimeStamp
	})

	client := test.GetRPCLient()

	var result methods.GetVersionInfoResponse
	err := client.CallResult(context.Background(), "getVersionInfo", nil, &result)
	assert.NoError(t, err)

	assert.Equal(t, config.Version, result.Version)
	assert.Equal(t, config.BuildTimestamp, result.BuildTimestamp)
	assert.Equal(t, config.CommitHash, result.CommitHash)
	assert.Equal(t, test.GetProtocolVersion(), result.ProtocolVersion)
	assert.NotEmpty(t, result.CaptiveCoreVersion)

}

// Runs git commands to fetch version information
func populateVersionInfo(t *testing.T) {

	execFunction := func(command string, args ...string) string {
		cmd := exec.Command(command, args...)
		t.Log("Running", cmd.Env, cmd.Args)
		out, innerErr := cmd.Output()
		if exitErr, ok := innerErr.(*exec.ExitError); ok {
			fmt.Printf("stdout:\n%s\n", string(out))
			fmt.Printf("stderr:\n%s\n", string(exitErr.Stderr))
		}
		require.NoError(t, innerErr)
		return string(out)
	}

	config.Version = execFunction("git", "describe", "--tags", "--always", "--abbrev=0", "--match='v[0-9]*.[0-9]*.[0-9]*'")
	config.CommitHash = execFunction("git", "rev-parse", "HEAD")
	config.BuildTimestamp = execFunction("date", "+%Y-%m-%dT%H:%M:%S")
}
