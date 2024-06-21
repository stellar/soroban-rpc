package db

import (
	"context"
	"io"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/events"

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

type MockEventHandler struct {
	passphrase      string
	ledgerRange     ledgerbucketwindow.LedgerRange
	contractIDToTx  map[string]ingest.LedgerTransaction
	eventTypeToTx   map[int]ingest.LedgerTransaction
	eventIDToTx     map[string]ingest.LedgerTransaction
	ledgerSeqToMeta map[uint32]*xdr.LedgerCloseMeta
}

func NewMockEventStore(passphrase string) *MockEventHandler {
	return &MockEventHandler{
		passphrase:      passphrase,
		contractIDToTx:  make(map[string]ingest.LedgerTransaction),
		eventTypeToTx:   make(map[int]ingest.LedgerTransaction),
		eventIDToTx:     make(map[string]ingest.LedgerTransaction),
		ledgerSeqToMeta: make(map[uint32]*xdr.LedgerCloseMeta),
	}
}

func (eventHandler *MockEventHandler) GetEvents(
	ctx context.Context,
	cursorRange events.CursorRange,
	contractIDs []string,
	f ScanFunction,
) error {
	return nil
}

func (eventHandler *MockEventHandler) GetLedgerRange(ctx context.Context) (ledgerbucketwindow.LedgerRange, error) {
	return ledgerbucketwindow.LedgerRange{}, nil
}

func (eventHandler *MockEventHandler) IngestEvents(lcm xdr.LedgerCloseMeta) error {
	eventHandler.ledgerSeqToMeta[lcm.LedgerSequence()] = &lcm

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(eventHandler.passphrase, lcm)
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

		txEvents, err := tx.GetDiagnosticEvents()
		if err != nil {
			return err
		}

		for index, e := range txEvents {
			var contractID []byte
			if e.Event.ContractId != nil {
				contractID = e.Event.ContractId[:]
				eventHandler.contractIDToTx[string(contractID)] = tx
			}
			id := events.Cursor{Ledger: lcm.LedgerSequence(), Tx: tx.Index, Op: 0, Event: uint32(index)}.String()
			eventHandler.eventTypeToTx[int(e.Event.Type)] = tx
			eventHandler.eventIDToTx[id] = tx
		}
	}

	if lcmSeq := lcm.LedgerSequence(); lcmSeq < eventHandler.ledgerRange.FirstLedger.Sequence ||
		eventHandler.ledgerRange.FirstLedger.Sequence == 0 {
		eventHandler.ledgerRange.FirstLedger.Sequence = lcmSeq
		eventHandler.ledgerRange.FirstLedger.CloseTime = lcm.LedgerCloseTime()
	}

	if lcmSeq := lcm.LedgerSequence(); lcmSeq > eventHandler.ledgerRange.LastLedger.Sequence {
		eventHandler.ledgerRange.LastLedger.Sequence = lcmSeq
		eventHandler.ledgerRange.LastLedger.CloseTime = lcm.LedgerCloseTime()
	}

	return nil
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
func (txn *MockTransactionHandler) GetLedgerRange(ctx context.Context) (ledgerbucketwindow.LedgerRange, error) {
	return txn.ledgerRange, nil
}

func (txn *MockTransactionHandler) GetTransaction(ctx context.Context, hash xdr.Hash) (
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

func (m *MockLedgerReader) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, bool, error) {
	lcm, ok := m.txn.ledgerSeqToMeta[sequence]
	if !ok {
		return xdr.LedgerCloseMeta{}, false, nil
	}
	return *lcm, true, nil
}

func (m *MockLedgerReader) StreamAllLedgers(ctx context.Context, f StreamLedgerFn) error {
	return nil
}

var (
	_ TransactionReader = &MockTransactionHandler{}
	_ TransactionWriter = &MockTransactionHandler{}
	_ LedgerReader      = &MockLedgerReader{}
)
