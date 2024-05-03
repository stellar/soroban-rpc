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

const (
	Cursor int64 = 55834574848
)

// buildTxParams constructs the parameters necessary for creating a transaction from the given account.
//
// account - the source account from which the transaction will originate. This account provides the starting sequence number.
//
// Returns a fully populated TransactionParams structure.
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

// sendTransaction submits a single transaction to the Stellar network via the given JSON-RPC client and returns
// the transaction hash.
//
// t - the testing framework handle for assertions.
// client - the JSON-RPC client used to send the transaction.
// kp - the Stellar keypair used to sign the transaction.
// transaction - the Stellar transaction to be sent.
//
// Returns the expected transaction hash as a hex string and any error encountered during the transaction submission.
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

// sendTransactions sends multiple transactions for testing purposes.
// It sends a total of three transactions, each from a new account sequence, and gathers the ledger
// numbers where these transactions were recorded.
//
// t - the testing framework handle for assertions.
// client - the JSON-RPC client used to send the transactions.
//
// Returns a slice of ledger numbers corresponding to where each transaction was recorded.
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
	}
	err := client.CallResult(context.Background(), "getTransactions", request, &result)
	assert.NoError(t, err)
	assert.Equal(t, len(result.Transactions), 3)
	assert.Equal(t, result.Transactions[0].Ledger.Sequence, ledgers[0])
	assert.Equal(t, result.Transactions[1].Ledger.Sequence, ledgers[1])
	assert.Equal(t, result.Transactions[2].Ledger.Sequence, ledgers[2])

	// Get transactions from single ledger
	//request = methods.GetTransactionsRequest{
	//	StartLedger: ledgers[0],
	//}
	//err = client.CallResult(context.Background(), "getTransactions", request, &result)
	//assert.NoError(t, err)
	//assert.Equal(t, len(result.Transactions), 3)
	//assert.Equal(t, result.Transactions[0].LedgerSequence, ledgers[0])
	//assert.Equal(t, result.Transactions[1].LedgerSequence, ledgers[1])
	//assert.Equal(t, result.Transactions[2].LedgerSequence, ledgers[2])

	// Get transactions with limit
	//request = methods.GetTransactionsRequest{
	//	StartLedger: ledgers[0],
	//	Pagination: &methods.TransactionsPaginationOptions{
	//		Limit: 1,
	//	},
	//}
	//err = client.CallResult(context.Background(), "getTransactions", request, &result)
	//assert.NoError(t, err)
	//assert.Equal(t, len(result.Transactions), 1)
	//assert.Equal(t, result.Transactions[0].LedgerSequence, ledgers[0])

	// Get transactions using previous result's cursor
	//cursor := toid.Parse(Cursor)
	//request = methods.GetTransactionsRequest{
	//	Pagination: &methods.TransactionsPaginationOptions{
	//		Cursor: &cursor,
	//		Limit:  5,
	//	},
	//}
	//err = client.CallResult(context.Background(), "getTransactions", request, &result)
	//assert.NoError(t, err)
	//assert.Equal(t, len(result.Transactions), 2)
	//assert.Equal(t, result.Transactions[0].LedgerSequence, ledgers[1])
	//assert.Equal(t, result.Transactions[1].LedgerSequence, ledgers[2])
}
