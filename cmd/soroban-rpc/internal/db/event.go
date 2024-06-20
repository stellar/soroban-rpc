package db

import (
	"context"
	"fmt"
	"io"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/support/db"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/events"
)

const eventTableName = "events"

// EventWriter is used during ingestion of events from LCM to DB
type EventWriter interface {
	InsertEvents(lcm xdr.LedgerCloseMeta) error
}

// EventReader has all the public methods to fetch events from DB
type EventReader interface {
	GetEvents(ctx context.Context, cursorRange events.CursorRange, contractIds []string, f ScanFunction) error
	// GetLedgerRange(ctx context.Context) error
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
		return fmt.Errorf("EventWriter incorrectly initialized without stmtCache")
	} else if txCount == 0 {
		return nil
	}

	var txReader *ingest.LedgerTransactionReader
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(eventHandler.passphrase, lcm)
	if err != nil {
		return fmt.Errorf(
			"failed to open transaction reader for ledger %d: %w ",
			lcm.LedgerSequence(), err)
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

// GetEvents applies f on all the events occurring in the given range with specified contract IDs if provided.
// The events are returned in sorted ascending Cursor order.
// If f returns false, the scan terminates early (f will not be applied on
// remaining events in the range).
func (eventHandler *eventHandler) GetEvents(ctx context.Context, cursorRange events.CursorRange, contractIds []string, f ScanFunction) error {
	start := time.Now()

	var rows []struct {
		EventCursorId string              `db:"id"`
		TxIndex       int                 `db:"application_order"`
		Lcm           xdr.LedgerCloseMeta `db:"meta"`
	}

	rowQ := sq.
		Select("e.id", "e.application_order", "lcm.meta").
		From(eventTableName + " e").
		Join(ledgerCloseMetaTableName + "lcm ON (e.ledger_sequence = lcm.sequence)").
		Where(sq.GtOrEq{"e.id": cursorRange.Start.String()}).
		Where(sq.Lt{"e.id": cursorRange.End.String()}).
		OrderBy("e.id ASC")

	if len(contractIds) > 0 {
		rowQ = rowQ.Where(sq.Eq{"e.contract_id": contractIds})
	}

	if err := eventHandler.db.Select(ctx, &rows, rowQ); err != nil {
		return fmt.Errorf("db read failed for start ledger cursor= %v contractIds= %v: %w", cursorRange.Start.String(), contractIds, err)
	} else if len(rows) < 1 {
		return errors.New("No LCM found with requested event filters")
	}

	for _, row := range rows {
		eventCursorId, txIndex, lcm := row.EventCursorId, row.TxIndex, row.Lcm
		reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(eventHandler.passphrase, lcm)
		if err != nil {
			return fmt.Errorf("failed to index to tx %d in ledger %d: %w", txIndex, lcm.LedgerSequence(), err)
		}

		err = reader.Seek(txIndex - 1)
		if err != nil {
			return fmt.Errorf("failed to index to tx %d in ledger %d: %w", txIndex, lcm.LedgerSequence(), err)
		}

		ledgerCloseTime := lcm.LedgerCloseTime()
		ledgerTx, err := reader.Read()
		transactionHash := ledgerTx.Result.TransactionHash
		diagEvents, diagErr := ledgerTx.GetDiagnosticEvents()

		if diagErr != nil {
			return fmt.Errorf("db read failed for Event Id %s: %w", eventCursorId, err)
		}

		// Find events based on filter passed in function f
		for eventIndex, event := range diagEvents {
			cur := events.Cursor{Ledger: lcm.LedgerSequence(), Tx: uint32(txIndex), Event: uint32(eventIndex)}
			if f != nil && !f(event, cur, ledgerCloseTime, &transactionHash) {
				return nil
			}
		}
	}

	eventHandler.log.
		WithField("startLedgerSequence", cursorRange.Start.Ledger).
		WithField("endLedgerSequence", cursorRange.End.Ledger).
		WithField("duration", time.Since(start)).
		Debugf("Fetched and decoded all the events with filters - contractIds: %v ", contractIds)

	return nil
}
