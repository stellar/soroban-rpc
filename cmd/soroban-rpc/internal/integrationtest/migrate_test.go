package integrationtest

import (
	"context"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/integrationtest/infrastructure"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/methods"
)

// Test that every Soroban RPC version (within the current protocol) can migrate cleanly to the current version
// We cannot test prior protocol versions since the Transaction XDR used for the test could be incompatible
// TODO: find a way to test migrations between protocols
func TestMigrate(t *testing.T) {
	if infrastructure.GetCoreMaxSupportedProtocol() != infrastructure.MaxSupportedProtocolVersion {
		t.Skip("Only test this for the latest protocol: ", infrastructure.MaxSupportedProtocolVersion)
	}
	for _, originVersion := range getCurrentProtocolReleasedVersions(t) {
		if originVersion == "21.1.0" {
			// This version of the RPC container fails to even start with its captive core companion file
			// (it fails Invalid configuration: DEPRECATED_SQL_LEDGER_STATE not set.)
			continue
		}
		if originVersion == "21.3.0" {
			// This version of RPC wasn't published as a docker container
			continue
		}
		t.Run(originVersion, func(t *testing.T) {
			testMigrateFromVersion(t, originVersion)
		})
	}
}

func testMigrateFromVersion(t *testing.T, version string) {
	sqliteFile := filepath.Join(t.TempDir(), "soroban-rpc.db")
	test := infrastructure.NewTest(t, &infrastructure.TestConfig{
		UseReleasedRPCVersion: version,
		UseSQLitePath:         sqliteFile,
	})

	// Submit an event-logging transaction in the version to migrate from
	submitTransactionResponse, _ := test.UploadHelloWorldContract()

	// Replace RPC with the current version, but keeping the previous network and sql database (causing any data migrations)
	// We need to do some wiring to plug RPC into the prior network
	test.StopRPC()
	corePorts := test.GetPorts().TestCorePorts
	test = infrastructure.NewTest(t, &infrastructure.TestConfig{
		// We don't want to run Core again
		OnlyRPC: &infrastructure.TestOnlyRPCConfig{
			CorePorts: corePorts,
			DontWait:  false,
		},
		UseSQLitePath: sqliteFile,
		// We don't want to mark the test as parallel twice since it causes a panic
		NoParallel: true,
	})

	// make sure that the transaction submitted before and its events exist in current RPC
	var transactionsResult methods.GetTransactionsResponse
	getTransactions := methods.GetTransactionsRequest{
		StartLedger: submitTransactionResponse.Ledger,
		Pagination: &methods.TransactionsPaginationOptions{
			Limit: 1,
		},
	}
	err := test.GetRPCLient().CallResult(context.Background(), "getTransactions", getTransactions, &transactionsResult)
	require.NoError(t, err)
	require.Equal(t, 1, len(transactionsResult.Transactions))
	require.Equal(t, submitTransactionResponse.Ledger, transactionsResult.Transactions[0].Ledger)

	var eventsResult methods.GetEventsResponse
	getEventsRequest := methods.GetEventsRequest{
		StartLedger: submitTransactionResponse.Ledger,
		Pagination: &methods.PaginationOptions{
			Limit: 1,
		},
	}
	err = test.GetRPCLient().CallResult(context.Background(), "getEvents", getEventsRequest, &eventsResult)
	require.NoError(t, err)
	require.Equal(t, len(eventsResult.Events), 1)
	require.Equal(t, submitTransactionResponse.Ledger, uint32(eventsResult.Events[0].Ledger))
}

func getCurrentProtocolReleasedVersions(t *testing.T) []string {
	protocolStr := strconv.Itoa(infrastructure.MaxSupportedProtocolVersion)
	cmd := exec.Command("git", "tag")
	cmd.Dir = infrastructure.GetCurrentDirectory()
	out, err := cmd.Output()
	require.NoError(t, err)
	tags := strings.Split(string(out), "\n")
	filteredTags := make([]string, 0, len(tags))
	for _, tag := range tags {
		if strings.HasPrefix(tag, "v"+protocolStr) {
			filteredTags = append(filteredTags, tag[1:])
		}
	}
	return filteredTags
}
