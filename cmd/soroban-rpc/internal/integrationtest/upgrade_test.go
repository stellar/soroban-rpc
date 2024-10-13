package integrationtest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go/xdr"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/db"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/integrationtest/infrastructure"
)

func TestUpgradeFrom20To22(t *testing.T) {
	if infrastructure.GetCoreMaxSupportedProtocol() != 22 {
		t.Skip("Only test this for protocol 22")
	}
	test := infrastructure.NewTest(t, &infrastructure.TestConfig{
		ProtocolVersion: 21,
	})

	test.UploadHelloWorldContract()

	// Upgrade to protocol 21 and re-upload the contract, which should cause a
	// caching of the contract estimations
	test.UpgradeProtocol(22)

	// Wait for the ledger to advance, so that the simulation library passes the
	// right protocol number
	rpcDB := test.GetDaemon().GetDB()
	initialLedgerSequence, err := db.NewLedgerEntryReader(rpcDB).GetLatestLedgerSequence(context.Background())
	require.NoError(t, err)
	require.Eventually(t,
		func() bool {
			newLedgerSequence, err := db.NewLedgerEntryReader(rpcDB).GetLatestLedgerSequence(context.Background())
			require.NoError(t, err)
			return newLedgerSequence > initialLedgerSequence
		},
		time.Minute,
		time.Second,
	)

	_, contractID, _ := test.CreateHelloWorldContract()

	contractFnParameterSym := xdr.ScSymbol("world")
	test.InvokeHostFunc(
		contractID,
		"hello",
		xdr.ScVal{
			Type: xdr.ScValTypeScvSymbol,
			Sym:  &contractFnParameterSym,
		},
	)
}
