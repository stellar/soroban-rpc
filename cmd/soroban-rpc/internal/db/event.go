package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/support/db"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
)

const (
	eventTableName = "events"
	firstLedger    = uint32(2)
)

// EventWriter is used during ingestion of events from LCM to DB
type EventWriter interface {
	InsertEvents(lcm xdr.LedgerCloseMeta) error
}

// EventReader has all the public methods to fetch events from DB
type EventReader interface {
	GetEvents(
		ctx context.Context,
		cursorRange CursorRange,
		contractIDs [][]byte,
		topics [][]string,
		eventTypes []int,
		f ScanFunction,
	) error
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

		transactionHash := tx.Result.TransactionHash[:]

		txEvents, err := tx.GetDiagnosticEvents()
		if err != nil {
			return err
		}

		if len(txEvents) == 0 {
			continue
		}

		query := sq.Insert(eventTableName).
			Columns(
				"id",
				"contract_id",
				"event_type",
				"event_data",
				"ledger_close_time",
				"transaction_hash",
				"topic1",
				"topic2",
				"topic3",
				"topic4",
			)

		for index, e := range txEvents {

			var contractID []byte
			if e.Event.ContractId != nil {
				contractID = e.Event.ContractId[:]
			}

			id := Cursor{Ledger: lcm.LedgerSequence(), Tx: tx.Index, Op: 0, Event: uint32(index)}.String()
			eventBlob, err := xdr.NewEncodingBuffer().MarshalBinary(&e)
			if err != nil {
				return err
			}

			v0, ok := e.Event.Body.GetV0()
			if !ok {
				return errors.New("unknown event version")
			}

			// Encode the topics
			maxTopicCount := 4
			topicList := make([]string, maxTopicCount)
			for index, segment := range v0.Topics {
				seg, err := xdr.MarshalBase64(segment)
				if err != nil {
					return err
				}
				topicList[index] = seg
			}

			query = query.Values(
				id,
				contractID,
				int(e.Event.Type),
				eventBlob,
				lcm.LedgerCloseTime(),
				transactionHash,
				topicList[0],
				topicList[1],
				topicList[2],
				topicList[3],
			)
		}

		_, err = query.RunWith(eventHandler.stmtCache).Exec()
		if err != nil {
			return err
		}
	}

	return nil
}

type ScanFunction func(
	event xdr.DiagnosticEvent,
	cursor Cursor,
	ledgerCloseTimestamp int64,
	txHash *xdr.Hash,
) bool

// trimEvents removes all Events which fall outside the ledger retention window.
func (eventHandler *eventHandler) trimEvents(latestLedgerSeq uint32, retentionWindow uint32) error {
	if latestLedgerSeq+1 <= retentionWindow {
		return nil
	}
	cutoff := latestLedgerSeq + 1 - retentionWindow
	id := Cursor{Ledger: cutoff}.String()

	_, err := sq.StatementBuilder.
		RunWith(eventHandler.stmtCache).
		Delete(eventTableName).
		Where(sq.Lt{"id": id}).
		Exec()
	return err
}

// GetEvents applies f on all the events occurring in the given range with specified contract IDs if provided.
// The events are returned in sorted ascending Cursor order.
// If f returns false, the scan terminates early (f will not be applied on
// remaining events in the range).
func (eventHandler *eventHandler) GetEvents(
	ctx context.Context,
	cursorRange CursorRange,
	contractIDs [][]byte,
	topics [][]string,
	eventTypes []int,
	f ScanFunction,
) error {
	start := time.Now()

	rowQ := sq.
		Select(" id", "event_data", "transaction_hash", "ledger_close_time").
		From(eventTableName).
		Where(sq.GtOrEq{"id": cursorRange.Start.String()}).
		Where(sq.Lt{"id": cursorRange.End.String()}).
		OrderBy("id ASC")

	if len(contractIDs) > 0 {
		rowQ = rowQ.Where(sq.Eq{"contract_id": contractIDs})
	}
	if len(eventTypes) > 0 {
		rowQ = rowQ.Where(sq.Eq{"event_type": eventTypes})
	}

	if len(topics) > 0 {
		var orConditions sq.Or
		for i, topic := range topics {
			if topic == nil {
				continue
			}
			orConditions = append(orConditions, sq.Eq{fmt.Sprintf("topic%d", i+1): topic})
		}
		if len(orConditions) > 0 {
			rowQ = rowQ.Where(orConditions)
		}
	}

	rows, err := eventHandler.db.Query(ctx, rowQ)
	if err != nil {
		return fmt.Errorf(
			"db read failed for start ledger cursor= %v end ledger cursor= %v "+
				"contractIDs= %v eventTypes= %v topics= %v   : %w",
			cursorRange.Start.String(),
			cursorRange.End.String(),
			contractIDs,
			eventTypes,
			topics,
			err)
	}

	defer rows.Close()

	foundRows := false
	for rows.Next() {
		foundRows = true
		var row struct {
			eventCursorID   string `db:"id"`
			eventData       []byte `db:"event_data"`
			transactionHash []byte `db:"transaction_hash"`
			ledgerCloseTime int64  `db:"ledger_close_time"`
		}

		err = rows.Scan(&row.eventCursorID, &row.eventData, &row.transactionHash, &row.ledgerCloseTime)
		if err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		id, eventData, ledgerCloseTime := row.eventCursorID, row.eventData, row.ledgerCloseTime
		transactionHash := row.transactionHash
		cur, err := ParseCursor(id)
		if err != nil {
			return fmt.Errorf("failed to parse cursor: %w", err)
		}

		var eventXDR xdr.DiagnosticEvent
		err = xdr.SafeUnmarshal(eventData, &eventXDR)
		if err != nil {
			return fmt.Errorf("failed to decode event: %w", err)
		}
		txHash := xdr.Hash(transactionHash)
		if !f(eventXDR, cur, ledgerCloseTime, &txHash) {
			return nil
		}
	}
	if !foundRows {
		eventHandler.log.
			WithField("duration", time.Since(start)).
			WithField("start", cursorRange.Start.String()).
			WithField("end", cursorRange.End.String()).
			WithField("contracts", contractIDs).
			WithField("eventTypes", eventTypes).
			WithField("Topics", topics).
			Debugf(
				"No events found for ledger range: duration= %v start ledger cursor= %v - end ledger cursor= %v contractIDs= %v",
				time.Since(start),
				cursorRange.Start.String(),
				cursorRange.End.String(),
				contractIDs,
			)
	}

	eventHandler.log.
		WithField("startLedgerSequence", cursorRange.Start.Ledger).
		WithField("endLedgerSequence", cursorRange.End.Ledger).
		WithField("duration", time.Since(start)).
		Debugf("Fetched and decoded all the events with filters - contractIDs: %v ", contractIDs)

	return nil
}

type eventTableMigration struct {
	firstLedger uint32
	lastLedger  uint32
	writer      EventWriter
}

func (e *eventTableMigration) ApplicableRange() *LedgerSeqRange {
	return &LedgerSeqRange{
		FirstLedgerSeq: e.firstLedger,
		LastLedgerSeq:  e.lastLedger,
	}
}

func (e *eventTableMigration) Apply(_ context.Context, meta xdr.LedgerCloseMeta) error {
	return e.writer.InsertEvents(meta)
}

func newEventTableMigration(
	_ context.Context,
	logger *log.Entry,
	passphrase string,
	ledgerSeqRange *LedgerSeqRange,
) migrationApplierFactory {
	return migrationApplierFactoryF(func(db *DB) (MigrationApplier, error) {
		migration := eventTableMigration{
			firstLedger: ledgerSeqRange.FirstLedgerSeq,
			lastLedger:  ledgerSeqRange.LastLedgerSeq,
			writer: &eventHandler{
				log:        logger,
				db:         db,
				stmtCache:  sq.NewStmtCache(db.GetTx()),
				passphrase: passphrase,
			},
		}
		return &migration, nil
	})
}
