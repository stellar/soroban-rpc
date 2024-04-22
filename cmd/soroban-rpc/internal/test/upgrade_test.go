package test

import (
	"context"
	"testing"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/jhttp"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/db"
)

func TestUpgradeFrom20To21(t *testing.T) {
	if GetCoreMaxSupportedProtocol() != 21 {
		t.Skip("Only test this for protocol 21")
	}
	test := NewTest(t, &TestConfig{
		ProtocolVersion: 20,
	})

	ch := jhttp.NewChannel(test.sorobanRPCURL(), nil)
	client := jrpc2.NewClient(ch, nil)

	sourceAccount := keypair.Root(StandaloneNetworkPassphrase)
	address := sourceAccount.Address()
	account := txnbuild.NewSimpleAccount(address, 0)

	helloWorldContract := getHelloWorldContract(t)

	params := preflightTransactionParams(t, client, txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{
			createInstallContractCodeOperation(account.AccountID, helloWorldContract),
		},
		BaseFee: txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	})

	tx, err := txnbuild.NewTransaction(params)
	assert.NoError(t, err)
	sendSuccessfulTransaction(t, client, sourceAccount, tx)

	// Upgrade to protocol 21 and re-upload the contract, which should cause a caching of the contract
	// estimations
	test.UpgradeProtocol(21)
	// Wait for the protocol version to propagate, so that the simulation library passes the right protocol
	foundProtocol := uint32(0)
	for i := 0; i < 60; i++ {
		rpcDB := test.daemon.GetDB()
		time.Sleep(time.Second)
		lReader := db.NewLedgerReader(rpcDB)
		latestLedgerSequence, err := db.NewLedgerEntryReader(rpcDB).GetLatestLedgerSequence(context.Background())
		require.NoError(t, err)
		latestLedger, present, err := lReader.GetLedger(context.Background(), latestLedgerSequence)
		require.NoError(t, err)
		require.True(t, present)
		foundProtocol = uint32(latestLedger.V1.LedgerHeader.Header.LedgerVersion)
		if foundProtocol == 21 {
			break
		}
	}
	require.Equal(t, uint32(21), foundProtocol, "rpc didn't start ingesting protocol 21")

	params = preflightTransactionParams(t, client, txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{
			createInstallContractCodeOperation(account.AccountID, helloWorldContract),
		},
		BaseFee: txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	})

	tx, err = txnbuild.NewTransaction(params)
	assert.NoError(t, err)
	sendSuccessfulTransaction(t, client, sourceAccount, tx)

	params = preflightTransactionParams(t, client, txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{
			createCreateContractOperation(address, helloWorldContract),
		},
		BaseFee: txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	})

	tx, err = txnbuild.NewTransaction(params)
	assert.NoError(t, err)
	sendSuccessfulTransaction(t, client, sourceAccount, tx)

	contractID := getContractID(t, address, testSalt, StandaloneNetworkPassphrase)
	contractFnParameterSym := xdr.ScSymbol("world")
	authAddrArg := "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H"
	authAccountIDArg := xdr.MustAddress(authAddrArg)
	tx, err = txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{
			&txnbuild.CreateAccount{
				Destination:   authAddrArg,
				Amount:        "100000",
				SourceAccount: address,
			},
		},
		BaseFee: txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	})
	assert.NoError(t, err)
	sendSuccessfulTransaction(t, client, sourceAccount, tx)
	params = preflightTransactionParams(t, client, txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: false,
		Operations: []txnbuild.Operation{
			createInvokeHostOperation(
				address,
				contractID,
				"auth",
				xdr.ScVal{
					Type: xdr.ScValTypeScvAddress,
					Address: &xdr.ScAddress{
						Type:      xdr.ScAddressTypeScAddressTypeAccount,
						AccountId: &authAccountIDArg,
					},
				},
				xdr.ScVal{
					Type: xdr.ScValTypeScvSymbol,
					Sym:  &contractFnParameterSym,
				},
			),
		},
		BaseFee: txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	})
	tx, err = txnbuild.NewTransaction(params)
	assert.NoError(t, err)

	assert.NoError(t, err)
	sendSuccessfulTransaction(t, client, sourceAccount, tx)
}
