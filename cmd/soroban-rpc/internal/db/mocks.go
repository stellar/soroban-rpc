package db

import (
	"context"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/events"
	"io"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/ledgerbucketwindow"
)

type mockTransactionHandler struct {
	passphrase string

	ledgerRange     ledgerbucketwindow.LedgerRange
	txs             map[string]ingest.LedgerTransaction
	txHashToMeta    map[string]*xdr.LedgerCloseMeta
	ledgerSeqToMeta map[uint32]*xdr.LedgerCloseMeta
}

func NewMockTransactionStore(passphrase string) *mockTransactionHandler {
	return &mockTransactionHandler{
		passphrase:      passphrase,
		txs:             make(map[string]ingest.LedgerTransaction),
		txHashToMeta:    make(map[string]*xdr.LedgerCloseMeta),
		ledgerSeqToMeta: make(map[uint32]*xdr.LedgerCloseMeta),
	}
}

type mockEventHandler struct {
	passphrase       string
	ledgerRange      ledgerbucketwindow.LedgerRange
	contractIdToMeta map[string]ingest.LedgerTransaction
	eventTypeToTx    map[int]ingest.LedgerTransaction
	eventIdToTx      map[string]ingest.LedgerTransaction
	ledgerSeqToMeta  map[uint32]*xdr.LedgerCloseMeta
}

func NewMockEventStore(passphrase string) *mockEventHandler {
	return &mockEventHandler{
		passphrase:       passphrase,
		contractIdToMeta: make(map[string]ingest.LedgerTransaction),
		eventTypeToTx:    make(map[int]ingest.LedgerTransaction),
		eventIdToTx:      make(map[string]ingest.LedgerTransaction),
		ledgerSeqToMeta:  make(map[uint32]*xdr.LedgerCloseMeta),
	}
}

func (eventHandler *mockEventHandler) GetEvents(ctx context.Context, cursorRange events.CursorRange, contractIds []string, f ScanFunction) error {
	if contractIds != nil {
		for _, contractId := range contractIds {
			ledgerTx, ok := eventHandler.contractIdToMeta[contractId]
			if ok {
				diagEvents, diagErr := ledgerTx.GetDiagnosticEvents()
				if diagErr != nil {
				}

				for _, event := range diagEvents {
					if !f(event, events.Cursor{0, 0, 0, 0}, 0, &ledgerTx.Result.TransactionHash) {
						return nil
					}
				}
			}
		}
	}
	return nil
}

func (eventHandler *mockEventHandler) IngestEvents(lcm xdr.LedgerCloseMeta) error {
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
			var contractId []byte
			if e.Event.ContractId != nil {
				contractId = e.Event.ContractId[:]
				eventHandler.contractIdToMeta[string(contractId)] = tx
			}
			id := events.Cursor{Ledger: lcm.LedgerSequence(), Tx: tx.Index, Op: 0, Event: uint32(index)}.String()
			eventHandler.eventTypeToTx[int(e.Event.Type)] = tx
			eventHandler.eventIdToTx[id] = tx
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

func (txn *mockTransactionHandler) InsertTransactions(lcm xdr.LedgerCloseMeta) error {
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
func (txn *mockTransactionHandler) GetLedgerRange(ctx context.Context) (ledgerbucketwindow.LedgerRange, error) {
	return txn.ledgerRange, nil
}

func (txn *mockTransactionHandler) GetTransaction(ctx context.Context, hash xdr.Hash) (
	Transaction, ledgerbucketwindow.LedgerRange, error,
) {
	if tx, ok := txn.txs[hash.HexString()]; !ok {
		return Transaction{}, txn.ledgerRange, ErrNoTransaction
	} else {
		itx, err := ParseTransaction(*txn.txHashToMeta[hash.HexString()], tx)
		return itx, txn.ledgerRange, err
	}
}

func (txn *mockTransactionHandler) RegisterMetrics(_, _ prometheus.Observer) {}

type mockLedgerReader struct {
	txn mockTransactionHandler
}

func NewMockLedgerReader(txn *mockTransactionHandler) *mockLedgerReader {
	return &mockLedgerReader{
		txn: *txn,
	}
}

func (m *mockLedgerReader) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, bool, error) {
	lcm, ok := m.txn.ledgerSeqToMeta[sequence]
	if !ok {
		return xdr.LedgerCloseMeta{}, false, nil
	}
	return *lcm, true, nil
}

func (m *mockLedgerReader) StreamAllLedgers(ctx context.Context, f StreamLedgerFn) error {
	return nil
}

type mockEventReader struct {
}

func NewMockEventReader() {
}

func (m *mockEventReader) GetEvents(ctx context.Context, startLedgerSequence int, eventTypes []int, contractIds []string, f ScanFunction) error {
	return nil
}

var (
	_ TransactionReader = &mockTransactionHandler{}
	_ TransactionWriter = &mockTransactionHandler{}
	_ LedgerReader      = &mockLedgerReader{}
)
