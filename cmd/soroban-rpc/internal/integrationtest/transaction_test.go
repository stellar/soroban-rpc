package integrationtest

import (
	"context"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/assert"

	"github.com/stellar/go/keypair"
	proto "github.com/stellar/go/protocols/stellarcore"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/integrationtest/infrastructure"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/methods"
)

func TestSendTransactionSucceedsWithoutResults(t *testing.T) {
	test := infrastructure.NewTest(t, nil)
	test.SendMasterOperation(
		&txnbuild.SetOptions{HomeDomain: txnbuild.NewHomeDomain("soroban.com")},
	)
}

func TestSendTransactionSucceedsWithResults(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	response, contractHash := test.UploadHelloWorldContract()

	// Check the result is what we expect
	var transactionResult xdr.TransactionResult
	assert.NoError(t, xdr.SafeUnmarshalBase64(response.ResultXDR, &transactionResult))
	opResults, ok := transactionResult.OperationResults()
	assert.True(t, ok)
	invokeHostFunctionResult, ok := opResults[0].MustTr().GetInvokeHostFunctionResult()
	assert.True(t, ok)
	assert.Equal(t, invokeHostFunctionResult.Code, xdr.InvokeHostFunctionResultCodeInvokeHostFunctionSuccess)
	contractHashBytes := xdr.ScBytes(contractHash[:])
	expectedScVal := xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &contractHashBytes}
	var transactionMeta xdr.TransactionMeta
	assert.NoError(t, xdr.SafeUnmarshalBase64(response.ResultMetaXDR, &transactionMeta))
	assert.True(t, expectedScVal.Equals(transactionMeta.V3.SorobanMeta.ReturnValue))
	var resultXdr xdr.TransactionResult
	assert.NoError(t, xdr.SafeUnmarshalBase64(response.ResultXDR, &resultXdr))
	expectedResult := xdr.TransactionResult{
		FeeCharged: resultXdr.FeeCharged,
		Result: xdr.TransactionResultResult{
			Code: xdr.TransactionResultCodeTxSuccess,
			Results: &[]xdr.OperationResult{
				{
					Code: xdr.OperationResultCodeOpInner,
					Tr: &xdr.OperationResultTr{
						Type: xdr.OperationTypeInvokeHostFunction,
						InvokeHostFunctionResult: &xdr.InvokeHostFunctionResult{
							Code:    xdr.InvokeHostFunctionResultCodeInvokeHostFunctionSuccess,
							Success: (*resultXdr.Result.Results)[0].Tr.InvokeHostFunctionResult.Success,
						},
					},
				},
			},
		},
	}

	assert.Equal(t, expectedResult, resultXdr)
}

func TestSendTransactionBadSequence(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	params := infrastructure.CreateTransactionParams(
		test.MasterAccount(),
		&txnbuild.SetOptions{HomeDomain: txnbuild.NewHomeDomain("soroban.com")},
	)
	params.IncrementSequenceNum = false
	tx, err := txnbuild.NewTransaction(params)
	assert.NoError(t, err)
	tx, err = tx.Sign(infrastructure.StandaloneNetworkPassphrase, test.MasterKey())
	assert.NoError(t, err)
	b64, err := tx.Base64()
	assert.NoError(t, err)

	request := methods.SendTransactionRequest{Transaction: b64}
	var result methods.SendTransactionResponse
	client := test.GetRPCLient()
	err = client.CallResult(context.Background(), "sendTransaction", request, &result)
	assert.NoError(t, err)

	assert.NotZero(t, result.LatestLedger)
	assert.NotZero(t, result.LatestLedgerCloseTime)
	expectedHashHex, err := tx.HashHex(infrastructure.StandaloneNetworkPassphrase)
	assert.NoError(t, err)
	assert.Equal(t, expectedHashHex, result.Hash)
	assert.Equal(t, proto.TXStatusError, result.Status)
	var errorResult xdr.TransactionResult
	assert.NoError(t, xdr.SafeUnmarshalBase64(result.ErrorResultXDR, &errorResult))
	assert.Equal(t, xdr.TransactionResultCodeTxBadSeq, errorResult.Result.Code)
}

func TestSendTransactionFailedInsufficientResourceFee(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	client := test.GetRPCLient()

	params := infrastructure.PreflightTransactionParams(t, client,
		infrastructure.CreateTransactionParams(
			test.MasterAccount(),
			infrastructure.CreateUploadHelloWorldOperation(test.MasterAccount().GetAccountID()),
		),
	)

	// make the transaction fail due to insufficient resource fees
	params.Operations[0].(*txnbuild.InvokeHostFunction).Ext.SorobanData.ResourceFee /= 2

	tx, err := txnbuild.NewTransaction(params)
	assert.NoError(t, err)

	assert.NoError(t, err)
	tx, err = tx.Sign(infrastructure.StandaloneNetworkPassphrase, test.MasterKey())
	assert.NoError(t, err)
	b64, err := tx.Base64()
	assert.NoError(t, err)

	request := methods.SendTransactionRequest{Transaction: b64}
	var result methods.SendTransactionResponse
	err = client.CallResult(context.Background(), "sendTransaction", request, &result)
	assert.NoError(t, err)

	assert.Equal(t, proto.TXStatusError, result.Status)
	var errorResult xdr.TransactionResult
	assert.NoError(t, xdr.SafeUnmarshalBase64(result.ErrorResultXDR, &errorResult))
	assert.Equal(t, xdr.TransactionResultCodeTxSorobanInvalid, errorResult.Result.Code)

	assert.NotEmpty(t, result.DiagnosticEventsXDR)
	var event xdr.DiagnosticEvent
	err = xdr.SafeUnmarshalBase64(result.DiagnosticEventsXDR[0], &event)
	assert.NoError(t, err)
}

func TestSendTransactionFailedInLedger(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	client := test.GetRPCLient()

	kp := keypair.Root(infrastructure.StandaloneNetworkPassphrase)
	tx, err := txnbuild.NewTransaction(
		infrastructure.CreateTransactionParams(
			test.MasterAccount(),
			&txnbuild.Payment{
				// Destination doesn't exist, making the transaction fail
				Destination:   "GA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJVSGZ",
				Amount:        "100000.0000000",
				Asset:         txnbuild.NativeAsset{},
				SourceAccount: "",
			},
		),
	)
	assert.NoError(t, err)
	tx, err = tx.Sign(infrastructure.StandaloneNetworkPassphrase, kp)
	assert.NoError(t, err)
	b64, err := tx.Base64()
	assert.NoError(t, err)

	request := methods.SendTransactionRequest{Transaction: b64}
	var result methods.SendTransactionResponse
	err = client.CallResult(context.Background(), "sendTransaction", request, &result)
	assert.NoError(t, err)

	expectedHashHex, err := tx.HashHex(infrastructure.StandaloneNetworkPassphrase)
	assert.NoError(t, err)

	assert.Equal(t, expectedHashHex, result.Hash)
	if !assert.Equal(t, proto.TXStatusPending, result.Status) {
		var txResult xdr.TransactionResult
		err := xdr.SafeUnmarshalBase64(result.ErrorResultXDR, &txResult)
		assert.NoError(t, err)
		t.Logf("error: %#v\n", txResult)
	}
	assert.NotZero(t, result.LatestLedger)
	assert.NotZero(t, result.LatestLedgerCloseTime)

	response := test.GetTransaction(expectedHashHex)
	assert.Equal(t, methods.TransactionStatusFailed, response.Status)
	var transactionResult xdr.TransactionResult
	assert.NoError(t, xdr.SafeUnmarshalBase64(response.ResultXDR, &transactionResult))
	assert.Equal(t, xdr.TransactionResultCodeTxFailed, transactionResult.Result.Code)
	assert.Greater(t, response.Ledger, result.LatestLedger)
	assert.Greater(t, response.LedgerCloseTime, result.LatestLedgerCloseTime)
	assert.GreaterOrEqual(t, response.LatestLedger, response.Ledger)
	assert.GreaterOrEqual(t, response.LatestLedgerCloseTime, response.LedgerCloseTime)
}

func TestSendTransactionFailedInvalidXDR(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	client := test.GetRPCLient()

	request := methods.SendTransactionRequest{Transaction: "abcdef"}
	var response methods.SendTransactionResponse
	jsonRPCErr := client.CallResult(context.Background(), "sendTransaction", request, &response).(*jrpc2.Error)
	assert.Equal(t, "invalid_xdr", jsonRPCErr.Message)
	assert.Equal(t, jrpc2.InvalidParams, jsonRPCErr.Code)
}
