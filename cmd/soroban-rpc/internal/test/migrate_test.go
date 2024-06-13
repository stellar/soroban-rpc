package test

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/jhttp"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/methods"
)

// Test that every Soroban RPC version (within the current protocol) can migrate cleanly to the current version
// We cannot test prior protocol versions since the Transaction XDR used for the test could be incompatible
// TODO: find a way to test migrations between protocols
func TestMigrate(t *testing.T) {
	t.Skip("see if it works when we skip this test")
	if GetCoreMaxSupportedProtocol() != MaxSupportedProtocolVersion {
		t.Skip("Only test this for the latest protocol: ", MaxSupportedProtocolVersion)
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
	it := NewTest(t, &TestConfig{
		UseReleasedRPCVersion: version,
		UseSQLitePath:         sqliteFile,
	})

	ch := jhttp.NewChannel(it.sorobanRPCURL(), nil)
	client := jrpc2.NewClient(ch, nil)

	// Submit an event-logging transaction in the version to migrate from
	kp := keypair.Root(StandaloneNetworkPassphrase)
	address := kp.Address()
	account := txnbuild.NewSimpleAccount(address, 0)

	contractBinary := getHelloWorldContract(t)
	params := preflightTransactionParams(t, client, txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{
			createInstallContractCodeOperation(account.AccountID, contractBinary),
		},
		BaseFee: txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	})
	tx, err := txnbuild.NewTransaction(params)
	assert.NoError(t, err)
	submitTransactionResponse := sendSuccessfulTransaction(t, client, kp, tx)

	// Run the current RPC version, but the previous network and sql database (causing a data migration if needed)
	it.StopRPC()
	it = NewTest(t, &TestConfig{UseSQLitePath: sqliteFile})

	// make sure that the transaction submitted before and its events exist in current RPC
	var transactionsResult methods.GetTransactionsResponse
	getTransactions := methods.GetTransactionsRequest{
		StartLedger: submitTransactionResponse.Ledger,
		Pagination: &methods.TransactionsPaginationOptions{
			Limit: 1,
		},
	}
	err = client.CallResult(context.Background(), "getTransactions", getTransactions, &transactionsResult)
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
	err = client.CallResult(context.Background(), "getEvents", getEventsRequest, &eventsResult)
	require.NoError(t, err)
	require.Equal(t, len(eventsResult.Events), 1)
	require.Equal(t, submitTransactionResponse.Ledger, uint32(eventsResult.Events[0].Ledger))
}

func getCurrentProtocolReleasedVersions(t *testing.T) []string {
	protocolStr := strconv.Itoa(MaxSupportedProtocolVersion)
	_, currentFilename, _, _ := runtime.Caller(0)
	currentDir := filepath.Dir(currentFilename)
	var out bytes.Buffer
	cmd := exec.Command("git", "tag")
	cmd.Dir = currentDir
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Run())
	tags := strings.Split(out.String(), "\n")
	filteredTags := make([]string, 0, len(tags))
	for _, tag := range tags {
		if strings.HasPrefix(tag, "v"+protocolStr) {
			filteredTags = append(filteredTags, tag[1:])
		}
	}
	return filteredTags
}
