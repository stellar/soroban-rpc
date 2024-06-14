package infrastructure

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/jhttp"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/protocols/stellarcore"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/methods"
)

// Client is a jrpc2 client which tolerates errors
type Client struct {
	url  string
	cli  *jrpc2.Client
	opts *jrpc2.ClientOptions
}

func NewClient(url string, opts *jrpc2.ClientOptions) *Client {
	c := &Client{url: url, opts: opts}
	c.refreshClient()
	return c
}

func (c *Client) refreshClient() {
	if c.cli != nil {
		c.cli.Close()
	}
	ch := jhttp.NewChannel(c.url, nil)
	c.cli = jrpc2.NewClient(ch, c.opts)
}

func (c *Client) CallResult(ctx context.Context, method string, params, result any) error {
	err := c.cli.CallResult(ctx, method, params, result)
	if err != nil {
		// This is needed because of https://github.com/creachadair/jrpc2/issues/118
		c.refreshClient()
	}
	return err
}

func (c *Client) Close() error {
	return c.cli.Close()
}

func getTransaction(t *testing.T, client *Client, hash string) methods.GetTransactionResponse {
	var result methods.GetTransactionResponse
	for i := 0; i < 60; i++ {
		request := methods.GetTransactionRequest{Hash: hash}
		err := client.CallResult(context.Background(), "getTransaction", request, &result)
		assert.NoError(t, err)

		if result.Status == methods.TransactionStatusNotFound {
			time.Sleep(time.Second)
			continue
		}

		return result
	}
	t.Fatal("GetTransaction timed out")
	return result
}

func SendSuccessfulTransaction(t *testing.T, client *Client, kp *keypair.Full, transaction *txnbuild.Transaction) methods.GetTransactionResponse {
	tx, err := transaction.Sign(StandaloneNetworkPassphrase, kp)
	assert.NoError(t, err)
	b64, err := tx.Base64()
	assert.NoError(t, err)

	request := methods.SendTransactionRequest{Transaction: b64}
	var result methods.SendTransactionResponse
	assert.NoError(t, client.CallResult(context.Background(), "sendTransaction", request, &result))

	expectedHashHex, err := tx.HashHex(StandaloneNetworkPassphrase)
	assert.NoError(t, err)

	assert.Equal(t, expectedHashHex, result.Hash)
	if !assert.Equal(t, stellarcore.TXStatusPending, result.Status) {
		var txResult xdr.TransactionResult
		err := xdr.SafeUnmarshalBase64(result.ErrorResultXDR, &txResult)
		assert.NoError(t, err)
		t.Logf("error: %#v\n", txResult)
	}
	assert.NotZero(t, result.LatestLedger)
	assert.NotZero(t, result.LatestLedgerCloseTime)

	response := getTransaction(t, client, expectedHashHex)
	if !assert.Equal(t, methods.TransactionStatusSuccess, response.Status) {
		var txResult xdr.TransactionResult
		assert.NoError(t, xdr.SafeUnmarshalBase64(response.ResultXdr, &txResult))
		t.Logf("error: %#v\n", txResult)

		var txMeta xdr.TransactionMeta
		assert.NoError(t, xdr.SafeUnmarshalBase64(response.ResultMetaXdr, &txMeta))

		if txMeta.V == 3 && txMeta.V3.SorobanMeta != nil {
			if len(txMeta.V3.SorobanMeta.Events) > 0 {
				t.Log("Contract events:")
				for i, e := range txMeta.V3.SorobanMeta.Events {
					t.Logf("  %d: %s\n", i, e)
				}
			}

			if len(txMeta.V3.SorobanMeta.DiagnosticEvents) > 0 {
				t.Log("Diagnostic events:")
				for i, d := range txMeta.V3.SorobanMeta.DiagnosticEvents {
					t.Logf("  %d: %s\n", i, d)
				}
			}
		}
	}

	require.NotNil(t, response.ResultXdr)
	assert.Greater(t, response.Ledger, result.LatestLedger)
	assert.Greater(t, response.LedgerCloseTime, result.LatestLedgerCloseTime)
	assert.GreaterOrEqual(t, response.LatestLedger, response.Ledger)
	assert.GreaterOrEqual(t, response.LatestLedgerCloseTime, response.LedgerCloseTime)
	return response
}

func SimulateTransactionFromTxParams(t *testing.T, client *Client, params txnbuild.TransactionParams) methods.SimulateTransactionResponse {
	savedAutoIncrement := params.IncrementSequenceNum
	params.IncrementSequenceNum = false
	tx, err := txnbuild.NewTransaction(params)
	require.NoError(t, err)
	params.IncrementSequenceNum = savedAutoIncrement
	txB64, err := tx.Base64()
	require.NoError(t, err)
	request := methods.SimulateTransactionRequest{Transaction: txB64}
	var response methods.SimulateTransactionResponse
	err = client.CallResult(context.Background(), "simulateTransaction", request, &response)
	require.NoError(t, err)
	return response
}

func PreflightTransactionParamsLocally(t *testing.T, params txnbuild.TransactionParams, response methods.SimulateTransactionResponse) txnbuild.TransactionParams {
	if !assert.Empty(t, response.Error) {
		fmt.Println(response.Error)
	}
	var transactionData xdr.SorobanTransactionData
	err := xdr.SafeUnmarshalBase64(response.TransactionData, &transactionData)
	require.NoError(t, err)

	op := params.Operations[0]
	switch v := op.(type) {
	case *txnbuild.InvokeHostFunction:
		require.Len(t, response.Results, 1)
		v.Ext = xdr.TransactionExt{
			V:           1,
			SorobanData: &transactionData,
		}
		var auth []xdr.SorobanAuthorizationEntry
		for _, b64 := range response.Results[0].Auth {
			var a xdr.SorobanAuthorizationEntry
			err := xdr.SafeUnmarshalBase64(b64, &a)
			assert.NoError(t, err)
			auth = append(auth, a)
		}
		v.Auth = auth
	case *txnbuild.ExtendFootprintTtl:
		require.Len(t, response.Results, 0)
		v.Ext = xdr.TransactionExt{
			V:           1,
			SorobanData: &transactionData,
		}
	case *txnbuild.RestoreFootprint:
		require.Len(t, response.Results, 0)
		v.Ext = xdr.TransactionExt{
			V:           1,
			SorobanData: &transactionData,
		}
	default:
		t.Fatalf("Wrong operation type %v", op)
	}

	params.Operations = []txnbuild.Operation{op}

	params.BaseFee += response.MinResourceFee
	return params
}

func PreflightTransactionParams(t *testing.T, client *Client, params txnbuild.TransactionParams) txnbuild.TransactionParams {
	response := SimulateTransactionFromTxParams(t, client, params)
	// The preamble should be zero except for the special restore case
	assert.Nil(t, response.RestorePreamble)
	return PreflightTransactionParamsLocally(t, params, response)
}
