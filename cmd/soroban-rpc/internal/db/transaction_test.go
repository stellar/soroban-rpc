package db

import (
	"context"
	"encoding/hex"
	"io"
	"math/rand"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactionNotFound(t *testing.T) {
	db := NewTestDB(t)
	log := log.DefaultLogger
	log.SetLevel(logrus.TraceLevel)

	reader := NewTransactionReader(log, db, passphrase)
	_, _, err := reader.GetTransaction(context.TODO(), xdr.Hash{})
	require.Error(t, err, io.EOF)
}

func TestTransactionFound(t *testing.T) {
	db := NewTestDB(t)
	ctx := context.TODO()
	log := log.DefaultLogger
	log.SetLevel(logrus.TraceLevel)

	reader := NewTransactionReader(log, db, passphrase)
	writer := NewReadWriter(log, db, 10, 10, passphrase)
	write, err := writer.NewTx(ctx)
	require.NoError(t, err)

	lcms := []xdr.LedgerCloseMeta{
		txMeta(1234, true),
		txMeta(1235, true),
		txMeta(1236, true),
		txMeta(1237, true),
	}

	for _, lcm := range lcms {
		require.NoError(t, write.TransactionWriter().InsertTransactions(lcm),
			"ingestion failed for ledger %+v", lcm.V1)
		b := lcm.TransactionHash(0)
		t.Logf("hash: %v", hex.EncodeToString(b[:]))
	}
	require.NoError(t, write.Commit(lcms[len(lcms)-1].LedgerSequence()))

	// check 404 case
	_, _, err = reader.GetTransaction(ctx, xdr.Hash{})
	require.Error(t, err, io.EOF)

	// check all 200 cases
	for _, lcm := range lcms {
		h := lcm.TransactionHash(0)
		_, lRange, err := reader.GetTransaction(ctx, h)
		assert.NoError(t, err, "failed to find txhash %s in db", hex.EncodeToString(h[:]))
		assert.EqualValues(t, 1234, lRange.FirstLedger.Sequence)
		assert.EqualValues(t, 1237, lRange.LastLedger.Sequence)
		// assert.Equal(t, tx.ApplicationOrder, 0)
		// assert.Equal(t, tx.Envelope, []byte{})
	}
}

func BenchmarkTransactionFetch(b *testing.B) {
	db := NewTestDB(b)
	ctx := context.TODO()
	log := log.DefaultLogger

	reader := NewTransactionReader(log, db, passphrase)
	writer := NewReadWriter(log, db, 10, 10, passphrase)
	write, err := writer.NewTx(ctx)
	require.NoError(b, err)

	// ingest 100k tx rows
	lcms := make([]xdr.LedgerCloseMeta, 0, 100_000)
	for i := uint32(0); i < uint32(cap(lcms)); i++ {
		lcms = append(lcms, txMeta(1234+i, i%2 == 0))
	}

	for _, lcm := range lcms {
		require.NoError(b, write.TransactionWriter().InsertTransactions(lcm),
			"ingestion failed for ledger %+v", lcm)
	}
	require.NoError(b, write.Commit(lcms[len(lcms)-1].LedgerSequence()))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := rand.Intn(len(lcms))
		reader.GetTransaction(ctx, lcms[r].TransactionHash(0))
		// tx, _, err := reader.GetTransaction(ctx, lcms[r].TransactionHash(0))
		// require.NoError(b, err)
		// assert.Equal(b, i%2 == 0, tx.Successful)
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
			Fee:           1,
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
		FeeCharged: 100,
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
				V:          3,
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
						CloseTime: xdr.TimePoint(ledgerCloseTime(acctSeq + 100)),
					},
					LedgerSeq: xdr.Uint32(acctSeq + 100),
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
	return int64(ledgerSequence)*25 + 100
}
