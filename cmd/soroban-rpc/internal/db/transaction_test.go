package db

import (
	"context"
	"encoding/hex"
	"math/rand"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/daemon/interfaces"
)

func TestTransactionNotFound(t *testing.T) {
	db := NewTestDB(t)
	ctx := context.TODO()
	log.SetLevel(logrus.TraceLevel)

	reader := NewTransactionReader(log.DefaultLogger, db, passphrase)

	// Assert the ledger range
	ledgerRange, err := reader.GetLedgerRange(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), ledgerRange.FirstLedger.Sequence)
	assert.Equal(t, int64(0), ledgerRange.FirstLedger.CloseTime)
	assert.Equal(t, uint32(0), ledgerRange.LastLedger.Sequence)
	assert.Equal(t, int64(0), ledgerRange.LastLedger.CloseTime)

	_, _, err = reader.GetTransaction(context.TODO(), xdr.Hash{})
	require.ErrorIs(t, err, ErrNoTransaction)
}

func TestTransactionFound(t *testing.T) {
	db := NewTestDB(t)
	ctx := context.TODO()
	log := log.DefaultLogger
	log.SetLevel(logrus.TraceLevel)

	writer := NewReadWriter(log, db, interfaces.MakeNoOpDeamon(), 10, 10, passphrase)
	write, err := writer.NewTx(ctx)
	require.NoError(t, err)

	lcms := []xdr.LedgerCloseMeta{
		CreateTxMeta(1234, true),
		CreateTxMeta(1235, true),
		CreateTxMeta(1236, true),
		CreateTxMeta(1237, true),
	}

	ledgerW, txW := write.LedgerWriter(), write.TransactionWriter()
	for _, lcm := range lcms {
		require.NoError(t, ledgerW.InsertLedger(lcm), "ingestion failed for ledger %+v", lcm.V1)
		require.NoError(t, txW.InsertTransactions(lcm), "ingestion failed for ledger %+v", lcm.V1)
	}
	require.NoError(t, write.Commit(lcms[len(lcms)-1].LedgerSequence()))

	// Assert the ledger range
	reader := NewTransactionReader(log, db, passphrase)
	ledgerRange, err := reader.GetLedgerRange(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(1234), ledgerRange.FirstLedger.Sequence)
	assert.Equal(t, LedgerCloseTime(1234), ledgerRange.FirstLedger.CloseTime)
	assert.Equal(t, uint32(1237), ledgerRange.LastLedger.Sequence)
	assert.Equal(t, LedgerCloseTime(1237), ledgerRange.LastLedger.CloseTime)

	// check 404 case
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

func BenchmarkTransactionFetch(b *testing.B) {
	db := NewTestDB(b)
	ctx := context.TODO()
	log := log.DefaultLogger

	writer := NewReadWriter(log, db, interfaces.MakeNoOpDeamon(), 100, 1_000_000, passphrase)
	write, err := writer.NewTx(ctx)
	require.NoError(b, err)

	// ingest 100k tx rows
	lcms := make([]xdr.LedgerCloseMeta, 0, 100_000)
	for i := uint32(0); i < uint32(cap(lcms)); i++ {
		lcms = append(lcms, CreateTxMeta(1234+i, i%2 == 0))
	}

	ledgerW, txW := write.LedgerWriter(), write.TransactionWriter()
	for _, lcm := range lcms {
		require.NoError(b, ledgerW.InsertLedger(lcm))
		require.NoError(b, txW.InsertTransactions(lcm))
	}
	require.NoError(b, write.Commit(lcms[len(lcms)-1].LedgerSequence()))
	reader := NewTransactionReader(log, db, passphrase)

	randoms := make([]int, b.N)
	for i := 0; i < b.N; i++ {
		randoms[i] = rand.Intn(len(lcms))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := randoms[i]
		tx, _, err := reader.GetTransaction(ctx, lcms[r].TransactionHash(0))
		require.NoError(b, err)
		assert.Equal(b, r%2 == 0, tx.Successful)
	}
}

//func BenchmarkGetLedgerRange(b *testing.B) {
//	logger := log.DefaultLogger
//
//	// create 100k tx rows
//	lcms := make([]xdr.LedgerCloseMeta, 0, 100_000)
//	for i := uint32(0); i < uint32(cap(lcms)); i++ {
//		lcms = append(lcms, CreateTxMeta(1234+i, i%2 == 0))
//	}
//
//	randoms := make([]int, b.N)
//	for i := 0; i < b.N; i++ {
//		randoms[i] = rand.Intn(len(lcms))
//	}
//
//	for i := 2; i < b.N; i++ {
//		db := NewTestDB(b)
//		writer := NewReadWriter(logger, db, interfaces.MakeNoOpDeamon(), 100, 1_000_000, passphrase)
//		write, err := writer.NewTx(context.TODO())
//		require.NoError(b, err)
//
//		ledgerW, txW := write.LedgerWriter(), write.TransactionWriter()
//		for j := 0; j < i; j++ {
//			require.NoError(b, ledgerW.InsertLedger(lcms[j]))
//			require.NoError(b, txW.InsertTransactions(lcms[j]))
//		}
//		require.NoError(b, write.Commit(lcms[len(lcms)-1].LedgerSequence()))
//		reader := NewTransactionReader(logger, db, passphrase)
//
//		b.ResetTimer()
//		ledgerRange, err := reader.GetLedgerRange(context.TODO())
//		require.NoError(b, err)
//		assert.Equal(b, ledgerRange.FirstLedger.Sequence, lcms[0].LedgerSequence())
//		assert.Equal(b, ledgerRange.LastLedger.Sequence, lcms[i-1].LedgerSequence())
//	}
//}
