package test

import (
	"context"
	"testing"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/jhttp"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
	"github.com/stretchr/testify/assert"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/methods"
)

func buildTxParams(account txnbuild.SimpleAccount) txnbuild.TransactionParams {
	params := txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{
			&txnbuild.SetOptions{HomeDomain: txnbuild.NewHomeDomain("soroban.com")},
		},
		BaseFee:       txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewInfiniteTimeout()},
	}
	return params
}

func sendTransaction(t *testing.T, client *jrpc2.Client, kp *keypair.Full, transaction *txnbuild.Transaction) (string, error) {
	tx, err := transaction.Sign(StandaloneNetworkPassphrase, kp)
	assert.NoError(t, err)
	b64, err := tx.Base64()
	assert.NoError(t, err)

	request := methods.SendTransactionRequest{Transaction: b64}
	var result methods.SendTransactionResponse

	expectedHashHex, err := tx.HashHex(StandaloneNetworkPassphrase)
	assert.NoError(t, err)

	return expectedHashHex, client.CallResult(context.Background(), "sendTransaction", request, &result)
}

func sendTransactions(t *testing.T, client *jrpc2.Client) []uint32 {
	kp := keypair.Root(StandaloneNetworkPassphrase)
	address := kp.Address()

	var hashes []string
	for i := 0; i <= 2; i++ {
		account := txnbuild.NewSimpleAccount(address, int64(i))
		tx, err := txnbuild.NewTransaction(buildTxParams(account))
		assert.NoError(t, err)

		hash, err := sendTransaction(t, client, kp, tx)
		assert.NoError(t, err)
		hashes = append(hashes, hash)
		time.Sleep(1 * time.Second)
	}

	var ledgers []uint32
	for _, hash := range hashes {
		response := getTransaction(t, client, hash)
		assert.NotNil(t, response)
		ledgers = append(ledgers, response.Ledger)
	}
	return ledgers
}

func TestGetTransactions(t *testing.T) {
	test := NewTest(t, nil)
	ch := jhttp.NewChannel(test.sorobanRPCURL(), nil)
	client := jrpc2.NewClient(ch, nil)

	ledgers := sendTransactions(t, client)

	// Get transactions across multiple ledgers
	var result methods.GetTransactionsResponse
	request := methods.GetTransactionsRequest{
		StartLedger: ledgers[0],
		EndLedger:   ledgers[2],
	}
	err := client.CallResult(context.Background(), "getTransactions", request, &result)
	assert.NoError(t, err)
	assert.Equal(t, len(result.Transactions), 3)
	assert.Equal(t, result.Transactions[0].LedgerSequence, ledgers[0])
	assert.Equal(t, result.Transactions[1].LedgerSequence, ledgers[1])
	assert.Equal(t, result.Transactions[2].LedgerSequence, ledgers[2])

	// Get transactions from single ledger
	request = methods.GetTransactionsRequest{
		StartLedger: ledgers[0],
		EndLedger:   ledgers[0],
	}
	err = client.CallResult(context.Background(), "getTransactions", request, &result)
	assert.NoError(t, err)
	assert.Equal(t, len(result.Transactions), 1)
	assert.Equal(t, result.Transactions[0].LedgerSequence, ledgers[0])

	// Get transactions with limit
	request = methods.GetTransactionsRequest{
		StartLedger: ledgers[0],
		EndLedger:   ledgers[2],
		Pagination: &methods.TransactionsPaginationOptions{
			Limit: 1,
		},
	}
	err = client.CallResult(context.Background(), "getTransactions", request, &result)
	assert.NoError(t, err)
	assert.Equal(t, len(result.Transactions), 1)
	assert.Equal(t, result.Transactions[0].LedgerSequence, ledgers[0])

	// Get transactions using previous result's cursor
	cursor := result.Pagination.Cursor
	request = methods.GetTransactionsRequest{
		EndLedger: ledgers[2],
		Pagination: &methods.TransactionsPaginationOptions{
			Cursor: cursor,
			Limit:  5,
		},
	}
	err = client.CallResult(context.Background(), "getTransactions", request, &result)
	assert.NoError(t, err)
	assert.Equal(t, len(result.Transactions), 2)
	assert.Equal(t, result.Transactions[0].LedgerSequence, ledgers[1])
	assert.Equal(t, result.Transactions[1].LedgerSequence, ledgers[2])

}
