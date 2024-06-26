package db

import (
	"context"
	"encoding/hex"
	"math/rand"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stellar/go/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/daemon/interfaces"
)

const (
	txMetaV                 = 3
	ledgerCloseTimeConstant = 100
	feeCharged              = 100
)

func TestTransactionNotFound(t *testing.T) {
	db := NewTestDB(t)
	log.SetLevel(logrus.TraceLevel)

	reader := NewTransactionReader(log.DefaultLogger, db, passphrase)
	_, _, err := reader.GetTransaction(context.TODO(), xdr.Hash{})
	require.ErrorIs(t, err, ErrNoTransaction)
}

func TestTransactionFound(t *testing.T) {
	db := NewTestDB(t)
	ctx := context.TODO()
	logger := log.DefaultLogger
	log.SetLevel(logrus.TraceLevel)

	writer := NewReadWriter(logger, db, interfaces.MakeNoOpDeamon(), 10, 10, passphrase)
	write, err := writer.NewTx(ctx)
	require.NoError(t, err)

	lcms := []xdr.LedgerCloseMeta{
		txMeta(1234, true),
		txMeta(1235, true),
		txMeta(1236, true),
		txMeta(1237, true),
	}

	ledgerW, txW := write.LedgerWriter(), write.TransactionWriter()
	for _, lcm := range lcms {
		require.NoError(t, ledgerW.InsertLedger(lcm), "ingestion failed for ledger %+v", lcm.V1)
		require.NoError(t, txW.InsertTransactions(lcm), "ingestion failed for ledger %+v", lcm.V1)
	}
	require.NoError(t, write.Commit(lcms[len(lcms)-1].LedgerSequence()))

	// check 404 case
	reader := NewTransactionReader(logger, db, passphrase)
	_, _, err = reader.GetTransaction(ctx, xdr.Hash{})
	require.ErrorIs(t, err, ErrNoTransaction)

	// check all 200 cases
	for _, lcm := range lcms {
		h := lcm.TransactionHash(0)
		tx, lRange, err := reader.GetTransaction(ctx, h)
		require.NoError(t, err, "failed to find txhash %s in db", hex.EncodeToString(h[:]))
		assert.EqualValues(t, 1234, lRange.FirstLedger.Sequence)
		assert.EqualValues(t, 1237, lRange.LastLedger.Sequence)
		assert.EqualValues(t, 1, tx.ApplicationOrder)

		expectedEnvelope, err := lcm.TransactionEnvelopes()[0].MarshalBinary()
		require.NoError(t, err)
		assert.Equal(t, expectedEnvelope, tx.Envelope)
	}
}

func TestGetLedgerRange_NonEmptyDB(t *testing.T) {
	db := NewTestDB(t)
	ctx := context.TODO()
	logger := log.DefaultLogger
	log.SetLevel(logrus.TraceLevel)

	writer := NewReadWriter(logger, db, interfaces.MakeNoOpDeamon(), 10, 10, passphrase)
	write, err := writer.NewTx(ctx)
	require.NoError(t, err)

	lcms := []xdr.LedgerCloseMeta{
		txMeta(1234, true),
		txMeta(1235, true),
		txMeta(1236, true),
		txMeta(1237, true),
	}

	ledgerW, txW := write.LedgerWriter(), write.TransactionWriter()
	for _, lcm := range lcms {
		require.NoError(t, ledgerW.InsertLedger(lcm), "ingestion failed for ledger %+v", lcm.V1)
		require.NoError(t, txW.InsertTransactions(lcm), "ingestion failed for ledger %+v", lcm.V1)
	}
	require.NoError(t, write.Commit(lcms[len(lcms)-1].LedgerSequence()))

	reader := NewTransactionReader(logger, db, passphrase)
	ledgerRange, err := reader.GetLedgerRange(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(1234), ledgerRange.FirstLedger.Sequence)
	assert.Equal(t, ledgerCloseTime(1234), ledgerRange.FirstLedger.CloseTime)
	assert.Equal(t, uint32(1237), ledgerRange.LastLedger.Sequence)
	assert.Equal(t, ledgerCloseTime(1237), ledgerRange.LastLedger.CloseTime)
}

func TestGetLedgerRange_EmptyDB(t *testing.T) {
	db := NewTestDB(t)
	ctx := context.TODO()
	logger := log.DefaultLogger
	log.SetLevel(logrus.TraceLevel)

	reader := NewTransactionReader(logger, db, passphrase)
	ledgerRange, err := reader.GetLedgerRange(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), ledgerRange.FirstLedger.Sequence)
	assert.Equal(t, int64(0), ledgerRange.FirstLedger.CloseTime)
	assert.Equal(t, uint32(0), ledgerRange.LastLedger.Sequence)
	assert.Equal(t, int64(0), ledgerRange.LastLedger.CloseTime)
}

func BenchmarkTransactionFetch(b *testing.B) {
	db := NewTestDB(b)
	ctx := context.TODO()
	logger := log.DefaultLogger

	writer := NewReadWriter(logger, db, interfaces.MakeNoOpDeamon(), 100, 1_000_000, passphrase)
	write, err := writer.NewTx(ctx)
	require.NoError(b, err)

	// ingest 100k tx rows
	lcms := make([]xdr.LedgerCloseMeta, 0, 100_000)
	for i := range cap(lcms) {
		lcms = append(lcms, txMeta(uint32(1234+i), i%2 == 0))
	}

	ledgerW, txW := write.LedgerWriter(), write.TransactionWriter()
	for _, lcm := range lcms {
		require.NoError(b, ledgerW.InsertLedger(lcm))
		require.NoError(b, txW.InsertTransactions(lcm))
	}
	require.NoError(b, write.Commit(lcms[len(lcms)-1].LedgerSequence()))
	reader := NewTransactionReader(logger, db, passphrase)

	randoms := make([]int, b.N)
	for i := range b.N {
		randoms[i] = rand.Intn(len(lcms))
	}

	b.ResetTimer()
	for i := range b.N {
		r := randoms[i]
		tx, _, err := reader.GetTransaction(ctx, lcms[r].TransactionHash(0))
		require.NoError(b, err)
		assert.Equal(b, r%2 == 0, tx.Successful)
	}
}

func BenchmarkGetLedgerRange(b *testing.B) {
	db := NewTestDB(b)
	logger := log.DefaultLogger
	writer := NewReadWriter(logger, db, interfaces.MakeNoOpDeamon(), 100, 1_000_000, passphrase)
	write, err := writer.NewTx(context.TODO())
	require.NoError(b, err)

	// create 100k tx rows
	lcms := make([]xdr.LedgerCloseMeta, 0, 100_000)
	for i := range cap(lcms) {
		lcms = append(lcms, txMeta(uint32(1234+i), i%2 == 0))
	}

	ledgerW, txW := write.LedgerWriter(), write.TransactionWriter()
	for _, lcm := range lcms {
		require.NoError(b, ledgerW.InsertLedger(lcm))
		require.NoError(b, txW.InsertTransactions(lcm))
	}
	require.NoError(b, write.Commit(lcms[len(lcms)-1].LedgerSequence()))
	reader := NewTransactionReader(logger, db, passphrase)

	b.ResetTimer()
	for range b.N {
		ledgerRange, err := reader.GetLedgerRange(context.TODO())
		require.NoError(b, err)
		assert.Equal(b, ledgerRange.FirstLedger.Sequence, lcms[0].LedgerSequence())
		assert.Equal(b, ledgerRange.LastLedger.Sequence, lcms[len(lcms)-1].LedgerSequence())
	}
}

//
// Structure creation methods below.
//

func txHash(acctSeq uint32) xdr.Hash {
	envelope := txEnvelope(acctSeq)
	hash, err := network.HashTransactionInEnvelope(envelope, passphrase)
	if err != nil {
		panic(err)
	}
	return hash
}

func txEnvelope(acctSeq uint32) xdr.TransactionEnvelope {
	envelope, err := xdr.NewTransactionEnvelope(xdr.EnvelopeTypeEnvelopeTypeTx, xdr.TransactionV1Envelope{
		Tx: xdr.Transaction{
			Fee:           feeCharged,
			SeqNum:        xdr.SequenceNumber(acctSeq),
			SourceAccount: xdr.MustMuxedAddress("MA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJVAAAAAAAAAAAAAJLK"),
		},
	})
	if err != nil {
		panic(err)
	}
	return envelope
}

func transactionResult(successful bool) xdr.TransactionResult {
	code := xdr.TransactionResultCodeTxBadSeq
	if successful {
		code = xdr.TransactionResultCodeTxSuccess
	}
	opResults := []xdr.OperationResult{}
	return xdr.TransactionResult{
		FeeCharged: feeCharged,
		Result: xdr.TransactionResultResult{
			Code:    code,
			Results: &opResults,
		},
	}
}

func txMeta(acctSeq uint32, successful bool) xdr.LedgerCloseMeta {
	envelope := txEnvelope(acctSeq)
	txProcessing := []xdr.TransactionResultMeta{
		{
			TxApplyProcessing: xdr.TransactionMeta{
				V:          txMetaV,
				Operations: &[]xdr.OperationMeta{},
				V3:         &xdr.TransactionMetaV3{},
			},
			Result: xdr.TransactionResultPair{
				TransactionHash: txHash(acctSeq),
				Result:          transactionResult(successful),
			},
		},
	}
	components := []xdr.TxSetComponent{
		{
			Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
			TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
				BaseFee: nil,
				Txs:     []xdr.TransactionEnvelope{envelope},
			},
		},
	}

	return xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					ScpValue: xdr.StellarValue{
						CloseTime: xdr.TimePoint(ledgerCloseTime(acctSeq)),
					},
					LedgerSeq: xdr.Uint32(acctSeq),
				},
			},
			TxProcessing: txProcessing,
			TxSet: xdr.GeneralizedTransactionSet{
				V: 1,
				V1TxSet: &xdr.TransactionSetV1{
					PreviousLedgerHash: xdr.Hash{1},
					Phases: []xdr.TransactionPhase{{
						V:            0,
						V0Components: &components,
					}},
				},
			},
		},
	}
}

func ledgerCloseTime(ledgerSequence uint32) int64 {
	return int64(ledgerSequence)*25 + ledgerCloseTimeConstant
}
