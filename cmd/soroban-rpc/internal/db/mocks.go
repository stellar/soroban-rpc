package db

import (
	"context"
	"io"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/ledgerbucketwindow"
)

type MockTransactionHandler struct {
	passphrase string

	ledgerRange     ledgerbucketwindow.LedgerRange
	txs             map[string]ingest.LedgerTransaction
	txHashToMeta    map[string]*xdr.LedgerCloseMeta
	ledgerSeqToMeta map[uint32]*xdr.LedgerCloseMeta
}

func NewMockTransactionStore(passphrase string) *MockTransactionHandler {
	return &MockTransactionHandler{
		passphrase:      passphrase,
		txs:             make(map[string]ingest.LedgerTransaction),
		txHashToMeta:    make(map[string]*xdr.LedgerCloseMeta),
		ledgerSeqToMeta: make(map[uint32]*xdr.LedgerCloseMeta),
	}
}

func (txn *MockTransactionHandler) InsertTransactions(lcm xdr.LedgerCloseMeta) error {
	txn.ledgerSeqToMeta[lcm.LedgerSequence()] = &lcm

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(txn.passphrase, lcm)
	if err != nil {
		return err
	}

	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		h := tx.Result.TransactionHash.HexString()
		txn.txs[h] = tx
		txn.txHashToMeta[h] = &lcm
	}

	if lcmSeq := lcm.LedgerSequence(); lcmSeq < txn.ledgerRange.FirstLedger.Sequence ||
		txn.ledgerRange.FirstLedger.Sequence == 0 {
		txn.ledgerRange.FirstLedger.Sequence = lcmSeq
		txn.ledgerRange.FirstLedger.CloseTime = lcm.LedgerCloseTime()
	}

	if lcmSeq := lcm.LedgerSequence(); lcmSeq > txn.ledgerRange.LastLedger.Sequence {
		txn.ledgerRange.LastLedger.Sequence = lcmSeq
		txn.ledgerRange.LastLedger.CloseTime = lcm.LedgerCloseTime()
	}

	return nil
}

// GetLedgerRange pulls the min/max ledger sequence numbers from the database.
func (txn *MockTransactionHandler) GetLedgerRange(_ context.Context) (ledgerbucketwindow.LedgerRange, error) {
	return txn.ledgerRange, nil
}

func (txn *MockTransactionHandler) GetTransaction(_ context.Context, hash xdr.Hash) (
	Transaction, ledgerbucketwindow.LedgerRange, error,
) {
	if tx, ok := txn.txs[hash.HexString()]; !ok {
		return Transaction{}, txn.ledgerRange, ErrNoTransaction
	} else {
		itx, err := ParseTransaction(*txn.txHashToMeta[hash.HexString()], tx)
		return itx, txn.ledgerRange, err
	}
}

func (txn *MockTransactionHandler) RegisterMetrics(_, _ prometheus.Observer) {}

type MockLedgerReader struct {
	txn MockTransactionHandler
}

func NewMockLedgerReader(txn *MockTransactionHandler) *MockLedgerReader {
	return &MockLedgerReader{
		txn: *txn,
	}
}

func (m *MockLedgerReader) GetLedger(_ context.Context, sequence uint32) (xdr.LedgerCloseMeta, bool, error) {
	lcm, ok := m.txn.ledgerSeqToMeta[sequence]
	if !ok {
		return xdr.LedgerCloseMeta{}, false, nil
	}
	return *lcm, true, nil
}

func (m *MockLedgerReader) StreamAllLedgers(_ context.Context, _ StreamLedgerFn) error {
	return nil
}

func (m *MockLedgerReader) StreamLedgerRange(_ context.Context, _ uint32, _ uint32, f StreamLedgerFn) error {
	return nil
}

var (
	_ TransactionReader = &MockTransactionHandler{}
	_ TransactionWriter = &MockTransactionHandler{}
	_ LedgerReader      = &MockLedgerReader{}
)
