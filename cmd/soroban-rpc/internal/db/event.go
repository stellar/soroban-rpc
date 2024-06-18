package db

import (
	"context"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/support/db"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/events"
	"io"
)

const eventTableName = "events"

// EventWriter is used during ingestion of events from LCM to DB
type EventWriter interface {
	InsertEvents(lcm xdr.LedgerCloseMeta) error
}

// EventReader has all the public methods to fetch events from DB
type EventReader interface {
	GetEvents(ctx context.Context, startCursor events.Cursor, contractIds []string, f ScanFunction) error
}

type eventHandler struct {
	log                       *log.Entry
	db                        db.SessionInterface
	stmtCache                 *sq.StmtCache
	passphrase                string
	ingestMetric, countMetric prometheus.Observer
}

func NewEventReader(log *log.Entry, db db.SessionInterface, passphrase string) EventReader {
	return &eventHandler{log: log, db: db, passphrase: passphrase}
}

func (eventHandler *eventHandler) InsertEvents(lcm xdr.LedgerCloseMeta) error {
	txCount := lcm.CountTransactions()

	if eventHandler.stmtCache == nil {
		return errors.New("EventWriter incorrectly initialized without stmtCache")
	} else if txCount == 0 {
		return nil
	}

	var txReader *ingest.LedgerTransactionReader
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(eventHandler.passphrase, lcm)
	if err != nil {
		return errors.Wrapf(err,
			"failed to open transaction reader for ledger %d",
			lcm.LedgerSequence())
	}
	defer func() {
		closeErr := txReader.Close()
		if err == nil {
			err = closeErr
		}
	}()

	for {
		var tx ingest.LedgerTransaction
		tx, err = txReader.Read()
		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			return err
		}

		if !tx.Result.Successful() {
			continue
		}

		txEvents, err := tx.GetDiagnosticEvents()
		if err != nil {
			return err
		}

		if len(txEvents) == 0 {
			continue
		}

		query := sq.Insert(eventTableName).
			Columns("id", "ledger_sequence", "application_order", "contract_id", "event_type")

		for index, e := range txEvents {
			var contractId []byte
			if e.Event.ContractId != nil {
				contractId = e.Event.ContractId[:]
			}
			id := events.Cursor{Ledger: lcm.LedgerSequence(), Tx: tx.Index, Op: 0, Event: uint32(index)}.String()
			query = query.Values(id, lcm.LedgerSequence(), tx.Index, contractId, int(e.Event.Type))
		}

		_, err = query.RunWith(eventHandler.stmtCache).Exec()
		if err != nil {
			return err
		}
	}

	return nil
}

type ScanFunction func(xdr.DiagnosticEvent, events.Cursor, int64, *xdr.Hash) bool

// trimEvents removes all Events which fall outside the ledger retention window.
func (eventHandler *eventHandler) trimEvents(latestLedgerSeq uint32, retentionWindow uint32) error {
	if latestLedgerSeq+1 <= retentionWindow {
		return nil
	}

	cutoff := latestLedgerSeq + 1 - retentionWindow
	_, err := sq.StatementBuilder.
		RunWith(eventHandler.stmtCache).
		Delete(eventTableName).
		Where(sq.Lt{"ledger_sequence": cutoff}).
		Exec()
	return err
}

func (eventHandler *eventHandler) GetEvents(ctx context.Context, startCursor events.Cursor, contractIds []string, f ScanFunction) error {

	var rows []struct {
		EventCursorId string              `db:"id"`
		TxIndex       int                 `db:"application_order"`
		Lcm           xdr.LedgerCloseMeta `db:"meta"`
	}

	rowQ := sq.
		Select("e.id", "e.application_order", "lcm.meta").
		From(fmt.Sprintf("%s e", eventTableName)).
		Join(fmt.Sprintf("%s lcm ON (e.ledger_sequence = lcm.sequence)", ledgerCloseMetaTableName)).
		Where(sq.GtOrEq{"e.id": startCursor.String()})

	if len(contractIds) > 0 {
		rowQ = rowQ.Where(sq.Eq{"e.contract_id": contractIds})
	}

	if err := eventHandler.db.Select(ctx, &rows, rowQ); err != nil {
		return errors.Wrapf(err, "db read failed for startLedgerSequence= %d contractIds= %v", startCursor.Ledger, contractIds)
	} else if len(rows) < 1 {
		return errors.New("No LCM found with requested event filters")
	}

	for _, row := range rows {
		eventCursorId, txIndex, lcm := row.EventCursorId, row.TxIndex, row.Lcm
		reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(eventHandler.passphrase, lcm)
		reader.Seek(txIndex - 1)

		if err != nil {
			return errors.Wrapf(err, "failed to index to tx %d in ledger %d", txIndex, lcm.LedgerSequence())
		}

		ledgerCloseTime := lcm.LedgerCloseTime()
		ledgerTx, err := reader.Read()
		transactionHash := ledgerTx.Result.TransactionHash
		diagEvents, diagErr := ledgerTx.GetDiagnosticEvents()

		if diagErr != nil {
			return errors.Wrapf(err, "db read failed for Event Id %s", eventCursorId)
		}

		// Find events based on filter passed in function f
		for eventIndex, event := range diagEvents {
			cur := events.Cursor{lcm.LedgerSequence(), uint32(txIndex), 0, uint32(eventIndex)}
			if f != nil && !f(event, cur, ledgerCloseTime, &transactionHash) {
				return nil
			}
		}
	}

	return nil
}
