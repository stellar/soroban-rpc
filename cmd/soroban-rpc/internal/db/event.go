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

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/ledgerbucketwindow"
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
	GetEvents(ctx context.Context, cursorRange CursorRange, contractIDs [][]byte, f ScanFunction) error
	GetLedgerRange(ctx context.Context) (ledgerbucketwindow.LedgerRange, error)
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
			var contractID []byte
			if e.Event.ContractId != nil {
				contractID = e.Event.ContractId[:]
			}
			id := Cursor{Ledger: lcm.LedgerSequence(), Tx: tx.Index, Op: 0, Event: uint32(index)}.String()
			query = query.Values(id, lcm.LedgerSequence(), tx.Index, contractID, int(e.Event.Type))
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
func (eventHandler *eventHandler) GetEvents(
	ctx context.Context,
	cursorRange CursorRange,
	contractIDs [][]byte,
	f ScanFunction,
) error {
	start := time.Now()

	var rows []struct {
		EventCursorID string              `db:"id"`
		TxIndex       int                 `db:"application_order"`
		Lcm           xdr.LedgerCloseMeta `db:"meta"`
	}

	rowQ := sq.
		Select("e.id", "e.application_order", "lcm.meta").
		From(eventTableName + " e").
		Join(ledgerCloseMetaTableName + " lcm ON (e.ledger_sequence = lcm.sequence)").
		Where(sq.GtOrEq{"e.id": cursorRange.Start.String()}).
		Where(sq.Lt{"e.id": cursorRange.End.String()}).
		OrderBy("e.id ASC")

	if len(contractIDs) > 0 {
		rowQ = rowQ.Where(sq.Eq{"e.contract_id": contractIDs})
	}

	if err := eventHandler.db.Select(ctx, &rows, rowQ); err != nil {
		return fmt.Errorf(
			"db read failed for start ledger cursor= %v contractIDs= %v: %w",
			cursorRange.Start.String(),
			contractIDs,
			err)
	} else if len(rows) < 1 {
		eventHandler.log.Debugf("No events found for start ledger cursor= %v contractIDs= %v",
			cursorRange.Start.String(),
			contractIDs,
		)
		return nil
	}

	for _, row := range rows {
		eventCursorID, txIndex, lcm := row.EventCursorID, row.TxIndex, row.Lcm
		reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(eventHandler.passphrase, lcm)
		if err != nil {
			return fmt.Errorf("failed to create ledger reader from LCM: %w", err)
		}

		err = reader.Seek(txIndex - 1)
		if err != nil {
			return fmt.Errorf("failed to index to tx %d in ledger %d: %w", txIndex, lcm.LedgerSequence(), err)
		}

		ledgerCloseTime := lcm.LedgerCloseTime()
		ledgerTx, err := reader.Read()
		if err != nil {
			return fmt.Errorf("failed reading tx: %w", err)
		}
		transactionHash := ledgerTx.Result.TransactionHash
		diagEvents, diagErr := ledgerTx.GetDiagnosticEvents()

		if diagErr != nil {
			return fmt.Errorf("db read failed for Event Id %s: %w", eventCursorID, err)
		}

		// Find events based on filter passed in function f
		for eventIndex, event := range diagEvents {
			cur := Cursor{Ledger: lcm.LedgerSequence(), Tx: uint32(txIndex), Event: uint32(eventIndex)}
			if !f(event, cur, ledgerCloseTime, &transactionHash) {
				return nil
			}
		}
	}

	eventHandler.log.
		WithField("startLedgerSequence", cursorRange.Start.Ledger).
		WithField("endLedgerSequence", cursorRange.End.Ledger).
		WithField("duration", time.Since(start)).
		Debugf("Fetched and decoded all the events with filters - contractIDs: %v ", contractIDs)

	return nil
}

// GetLedgerRange returns the min/max ledger sequence numbers from the events table

func (eventHandler *eventHandler) GetLedgerRange(ctx context.Context) (ledgerbucketwindow.LedgerRange, error) {
	// TODO: Once we unify all the retention windows use common GetLedgerRange from LedgerReader
	var ledgerRange ledgerbucketwindow.LedgerRange

	//
	// We use subqueries alongside a UNION ALL stitch in order to select the min
	// and max from the ledger table in a single query and get around sqlite's
	// limitations with parentheses (see https://stackoverflow.com/a/22609948).
	//
	// Queries to get the minimum and maximum ledger sequence from the transactions table
	minLedgerSeqQ := sq.
		Select("m1.ledger_sequence").
		FromSelect(
			sq.
				Select("ledger_sequence").
				From(eventTableName).
				OrderBy("ledger_sequence ASC").
				Limit(1),
			"m1",
		)
	maxLedgerSeqQ, args, err := sq.
		Select("m2.ledger_sequence").
		FromSelect(
			sq.
				Select("ledger_sequence").
				From(eventTableName).
				OrderBy("ledger_sequence DESC").
				Limit(1),
			"m2",
		).ToSql()
	if err != nil {
		return ledgerRange, fmt.Errorf("couldn't build ledger range query: %w", err)
	}

	// Combine the min and max ledger sequence queries using UNION ALL
	eventMinMaxLedgersQ, _, err := minLedgerSeqQ.Suffix("UNION ALL "+maxLedgerSeqQ, args...).ToSql()
	if err != nil {
		return ledgerRange, fmt.Errorf("couldn't build ledger range query: %w", err)
	}

	// Final query to join ledger_close_meta table and the sequence numbers we got from eventMinMaxLedgersQ
	finalSQL := sq.
		Select("lcm.meta").
		From(ledgerCloseMetaTableName + " as lcm").
		JoinClause(fmt.Sprintf("JOIN (%s) as seqs ON lcm.sequence == seqs.ledger_sequence", eventMinMaxLedgersQ))

	var lcms []xdr.LedgerCloseMeta
	if err = eventHandler.db.Select(ctx, &lcms, finalSQL); err != nil {
		return ledgerRange, fmt.Errorf("couldn't build ledger range query: %w", err)
	} else if len(lcms) < 2 {
		// There is almost certainly a row, but we want to avoid a race condition
		// with ingestion as well as support test cases from an empty DB, so we need
		// to sanity check that there is in fact a result. Note that no ledgers in
		// the database isn't an error, it's just an empty range.
		return ledgerRange, nil
	}

	lcm1, lcm2 := lcms[0], lcms[1]
	ledgerRange.FirstLedger.Sequence = lcm1.LedgerSequence()
	ledgerRange.FirstLedger.CloseTime = lcm1.LedgerCloseTime()
	ledgerRange.LastLedger.Sequence = lcm2.LedgerSequence()
	ledgerRange.LastLedger.CloseTime = lcm2.LedgerCloseTime()

	eventHandler.log.Debugf("Database ledger range: [%d, %d]",
		ledgerRange.FirstLedger.Sequence, ledgerRange.LastLedger.Sequence)
	return ledgerRange, nil
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
	logger *log.Entry,
	retentionWindow uint32,
	passphrase string,
) migrationApplierFactory {
	return migrationApplierFactoryF(func(db *DB, latestLedger uint32) (MigrationApplier, error) {
		firstLedgerToMigrate := firstLedger
		writer := &eventHandler{
			log:        logger,
			db:         db,
			stmtCache:  sq.NewStmtCache(db.GetTx()),
			passphrase: passphrase,
		}
		if latestLedger > retentionWindow {
			firstLedgerToMigrate = latestLedger - retentionWindow
		}

		migration := eventTableMigration{
			firstLedger: firstLedgerToMigrate,
			lastLedger:  latestLedger,
			writer:      writer,
		}
		return &migration, nil
	})
}
