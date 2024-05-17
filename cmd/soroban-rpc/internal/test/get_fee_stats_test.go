package test

import (
	"context"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/jhttp"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/methods"
)

func TestGetFeeStats(t *testing.T) {
	test := NewTest(t, nil)

	ch := jhttp.NewChannel(test.sorobanRPCURL(), nil)
	client := jrpc2.NewClient(ch, nil)

	sourceAccount := keypair.Root(StandaloneNetworkPassphrase)
	address := sourceAccount.Address()
	account := txnbuild.NewSimpleAccount(address, 0)

	// Submit soroban transaction
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
	sorobanTxResponse := sendSuccessfulTransaction(t, client, sourceAccount, tx)
	var sorobanTxResult xdr.TransactionResult
	require.NoError(t, xdr.SafeUnmarshalBase64(sorobanTxResponse.ResultXdr, &sorobanTxResult))
	sorobanTotalFee := sorobanTxResult.FeeCharged
	var sorobanTxMeta xdr.TransactionMeta
	require.NoError(t, xdr.SafeUnmarshalBase64(sorobanTxResponse.ResultMetaXdr, &sorobanTxMeta))
	sorobanFees := sorobanTxMeta.MustV3().SorobanMeta.Ext.MustV1()
	sorobanResourceFeeCharged := sorobanFees.TotalRefundableResourceFeeCharged + sorobanFees.TotalNonRefundableResourceFeeCharged
	sorobanInclusionFee := uint64(sorobanTotalFee - sorobanResourceFeeCharged)

	// Submit classic transaction
	params = txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{
			&txnbuild.BumpSequence{BumpTo: account.Sequence + 100},
		},
		BaseFee: txnbuild.MinBaseFee,
		Memo:    nil,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	}
	tx, err = txnbuild.NewTransaction(params)
	assert.NoError(t, err)
	classicTxResponse := sendSuccessfulTransaction(t, client, sourceAccount, tx)
	var classicTxResult xdr.TransactionResult
	require.NoError(t, xdr.SafeUnmarshalBase64(classicTxResponse.ResultXdr, &classicTxResult))
	classicFee := uint64(classicTxResult.FeeCharged)

	var result methods.GetFeeStatsResult
	if err := client.CallResult(context.Background(), "getFeeStats", nil, &result); err != nil {
		t.Fatalf("rpc call failed: %v", err)
	}
	expectedResult := methods.GetFeeStatsResult{
		SorobanInclusionFee: methods.FeeDistribution{
			Max:              sorobanInclusionFee,
			Min:              sorobanInclusionFee,
			Mode:             sorobanInclusionFee,
			P10:              sorobanInclusionFee,
			P20:              sorobanInclusionFee,
			P30:              sorobanInclusionFee,
			P40:              sorobanInclusionFee,
			P50:              sorobanInclusionFee,
			P60:              sorobanInclusionFee,
			P70:              sorobanInclusionFee,
			P80:              sorobanInclusionFee,
			P90:              sorobanInclusionFee,
			P95:              sorobanInclusionFee,
			P99:              sorobanInclusionFee,
			TransactionCount: 1,
			LedgerCount:      result.SorobanInclusionFee.LedgerCount,
		},
		InclusionFee: methods.FeeDistribution{
			Max:              classicFee,
			Min:              classicFee,
			Mode:             classicFee,
			P10:              classicFee,
			P20:              classicFee,
			P30:              classicFee,
			P40:              classicFee,
			P50:              classicFee,
			P60:              classicFee,
			P70:              classicFee,
			P80:              classicFee,
			P90:              classicFee,
			P95:              classicFee,
			P99:              classicFee,
			TransactionCount: 1,
			LedgerCount:      result.InclusionFee.LedgerCount,
		},
		LatestLedger: result.LatestLedger,
	}
	assert.Equal(t, expectedResult, result)

	// check ledgers separately
	assert.Greater(t, result.InclusionFee.LedgerCount, uint32(0))
	assert.Greater(t, result.SorobanInclusionFee.LedgerCount, uint32(0))
	assert.Greater(t, result.LatestLedger, uint32(0))
}
