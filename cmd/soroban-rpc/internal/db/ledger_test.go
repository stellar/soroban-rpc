package db

import (
	"context"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go/network"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/daemon/interfaces"
)

var (
	passphrase = network.FutureNetworkPassphrase
	logger     = log.DefaultLogger
)

func createLedger(ledgerSequence uint32) xdr.LedgerCloseMeta {
	return xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Hash: xdr.Hash{},
				Header: xdr.LedgerHeader{
					LedgerSeq: xdr.Uint32(ledgerSequence),
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{},
			},
		},
	}
}

func assertLedgerRange(t *testing.T, reader LedgerReader, start, end uint32) {
	var allLedgers []xdr.LedgerCloseMeta
	err := reader.StreamAllLedgers(context.Background(), func(txmeta xdr.LedgerCloseMeta) error {
		allLedgers = append(allLedgers, txmeta)
		return nil
	})
	assert.NoError(t, err)
	for i := start - 1; i <= end+1; i++ {
		ledger, exists, err := reader.GetLedger(context.Background(), i)
		assert.NoError(t, err)
		if i < start || i > end {
			assert.False(t, exists)
			continue
		}
		assert.True(t, exists)
		ledgerBinary, err := ledger.MarshalBinary()
		assert.NoError(t, err)
		expected := createLedger(i)
		expectedBinary, err := expected.MarshalBinary()
		assert.NoError(t, err)
		assert.Equal(t, expectedBinary, ledgerBinary)

		ledgerBinary, err = allLedgers[0].MarshalBinary()
		assert.NoError(t, err)
		assert.Equal(t, expectedBinary, ledgerBinary)
		allLedgers = allLedgers[1:]
	}
	assert.Empty(t, allLedgers)
}

func TestLedgers(t *testing.T) {
	db := NewTestDB(t)
	daemon := interfaces.MakeNoOpDeamon()

	reader := NewLedgerReader(db)
	_, exists, err := reader.GetLedger(context.Background(), 1)
	assert.NoError(t, err)
	assert.False(t, exists)

	for i := 1; i <= 10; i++ {
		ledgerSequence := uint32(i)
		tx, err := NewReadWriter(logger, db, daemon, 150, 15, passphrase).NewTx(context.Background())
		assert.NoError(t, err)
		assert.NoError(t, tx.LedgerWriter().InsertLedger(createLedger(ledgerSequence)))
		assert.NoError(t, tx.Commit(ledgerSequence))
		// rolling back after a commit is a no-op
		assert.NoError(t, tx.Rollback())
	}

	assertLedgerRange(t, reader, 1, 10)

	ledgerSequence := uint32(11)
	tx, err := NewReadWriter(logger, db, daemon, 150, 15, passphrase).NewTx(context.Background())
	assert.NoError(t, err)
	assert.NoError(t, tx.LedgerWriter().InsertLedger(createLedger(ledgerSequence)))
	assert.NoError(t, tx.Commit(ledgerSequence))

	assertLedgerRange(t, reader, 1, 11)

	ledgerSequence = uint32(12)
	tx, err = NewReadWriter(logger, db, daemon, 150, 5, passphrase).NewTx(context.Background())
	assert.NoError(t, err)
	assert.NoError(t, tx.LedgerWriter().InsertLedger(createLedger(ledgerSequence)))
	assert.NoError(t, tx.Commit(ledgerSequence))

	assertLedgerRange(t, reader, 8, 12)
}

func NewTestDB(tb testing.TB) *DB {
	tmp := tb.TempDir()
	dbPath := path.Join(tmp, "db.sqlite")
	db, err := OpenSQLiteDB(dbPath)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		assert.NoError(tb, db.Close())
	})
	return db
}
