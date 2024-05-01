package test

import (
	"context"
	"fmt"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/config"
	"os/exec"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/jhttp"
	"github.com/stretchr/testify/assert"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/methods"
)

func TestGetVersionInfoSucceeds(t *testing.T) {
	test := NewTest(t, nil)

	populateVersionInfo(test)
	defer resetVersionInfo(test)

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
	assert.NotEmpty(t, result.ProtocolVersion)
}

// Runs git commands to fetch version information
func populateVersionInfo(test *Test) {

	execFunction := func(command string, args ...string) string {
		cmd := exec.Command(command, args...)
		test.t.Log("Running", cmd.Env, cmd.Args)
		out, innerErr := cmd.Output()
		if exitErr, ok := innerErr.(*exec.ExitError); ok {
			fmt.Printf("stdout:\n%s\n", string(out))
			fmt.Printf("stderr:\n%s\n", string(exitErr.Stderr))
		}

		if innerErr != nil {
			test.t.Fatalf("Command %s failed: %v", cmd.Env, innerErr)
		}
		return string(out)
	}

	config.Version = execFunction("git", "describe", "--tags", "--always", "--abbrev=0", "--match='v[0-9]*.[0-9]*.[0-9]*'")
	config.CommitHash = execFunction("git", "rev-parse", "HEAD")
	config.BuildTimestamp = execFunction("date", "+%Y-%m-%dT%H:%M:%S")
}

func resetVersionInfo(test *Test) {

	test.t.Log("Reset version information to default values")

	config.Version = "0.0.0"
	config.CommitHash = ""
	config.BuildTimestamp = ""
}
