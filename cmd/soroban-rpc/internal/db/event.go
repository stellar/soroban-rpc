package db

import (
	"context"
	"encoding/json"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/support/db"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/toid"
	"github.com/stellar/go/xdr"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/events"
	"io"
	"strconv"
	"strings"
)

const eventTableName = "events"

// EventWriter is used during ingestion of events from LCM to DB
type EventWriter interface {
	InsertEvents(lcm xdr.LedgerCloseMeta) error
}

// EventReader has all the public methods to fetch events from DB
type EventReader interface {
	GetEvents(ctx context.Context, startLedgerSequence int, eventType int, contractIds []string, f ScanFunction) ([]xdr.DiagnosticEvent, error)
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

// Cursor represents the position of a Soroban event.
// Soroban events are sorted in ascending order by
// ledger sequence, transaction index, operation index,
// and event index.
type Cursor struct {
	// Ledger is the sequence of the ledger which emitted the event.
	Ledger uint32
	// Tx is the index of the transaction within the ledger which emitted the event.
	Tx uint32
	// Op is the index of the operation within the transaction which emitted the event.
	// Note: Currently, there is no use for it (events are transaction-wide and not operation-specific)
	//       but we keep it in order to make the API future-proof.
	Op uint32
	// Event is the index of the event within in the operation which emitted the event.
	Event uint32
}

// String returns a string representation of this cursor
func (c Cursor) String() string {
	return fmt.Sprintf(
		"%019d-%010d",
		toid.New(int32(c.Ledger), int32(c.Tx), int32(c.Op)).ToInt64(),
		c.Event,
	)
}

// MarshalJSON marshals the cursor into JSON
func (c Cursor) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.String())
}

// UnmarshalJSON unmarshalls a cursor from the given JSON
func (c *Cursor) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	if parsed, err := ParseCursor(s); err != nil {
		return err
	} else {
		*c = parsed
	}
	return nil
}

// ParseCursor parses the given string and returns the corresponding cursor
func ParseCursor(input string) (Cursor, error) {
	parts := strings.SplitN(input, "-", 2)
	if len(parts) != 2 {
		return Cursor{}, fmt.Errorf("invalid event id %s", input)
	}

	// Parse the first part (toid)
	idInt, err := strconv.ParseInt(parts[0], 10, 64) //lint:ignore gomnd
	if err != nil {
		return Cursor{}, fmt.Errorf("invalid event id %s: %w", input, err)
	}
	parsed := toid.Parse(idInt)

	// Parse the second part (event order)
	eventOrder, err := strconv.ParseUint(parts[1], 10, 32) //lint:ignore gomnd
	if err != nil {
		return Cursor{}, fmt.Errorf("invalid event id %s: %w", input, err)
	}

	return Cursor{
		Ledger: uint32(parsed.LedgerSequence),
		Tx:     uint32(parsed.TransactionOrder),
		Op:     uint32(parsed.OperationOrder),
		Event:  uint32(eventOrder),
	}, nil
}

type ScanFunction func(xdr.DiagnosticEvent, Cursor, int64, *xdr.Hash) bool

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

func (eventHandler *eventHandler) GetEvents(ctx context.Context, startLedgerSequence int, eventType int, contractIds []string, f ScanFunction) ([]xdr.DiagnosticEvent, error) {

	var rows []struct {
		TxIndex int                 `db:"application_order"`
		Lcm     xdr.LedgerCloseMeta `db:"meta"`
	}

	rowQ := sq.
		Select("e.application_order", "lcm.meta").
		From(fmt.Sprintf("%s e", eventTableName)).
		Join(fmt.Sprintf("%s lcm ON (e.ledger_sequence = lcm.sequence)", ledgerCloseMetaTableName)).
		Where(sq.GtOrEq{"e.ledger_sequence": startLedgerSequence})

	if len(contractIds) > 0 {
		rowQ = rowQ.Where(sq.Eq{"e.contract_id": contractIds})
	}

	if eventType != -1 {
		rowQ = rowQ.Where(sq.Eq{"e.event_type": eventType})
	}

	if err := eventHandler.db.Select(ctx, &rows, rowQ); err != nil {
		return []xdr.DiagnosticEvent{},
			errors.Wrapf(err, "db read failed for startLedgerSequence= %d contractIds= %v eventType= %d ", startLedgerSequence, contractIds, eventType)
	} else if len(rows) < 1 {
		return []xdr.DiagnosticEvent{}, errors.New("No LCM found with requested event filters")
	}

	txIndex, lcm := rows[0].TxIndex, rows[0].Lcm
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(eventHandler.passphrase, lcm)
	reader.Seek(txIndex - 1)

	if err != nil {
		return []xdr.DiagnosticEvent{},
			errors.Wrapf(err, "failed to index to tx %d in ledger %d",
				txIndex, lcm.LedgerSequence())
	}

	ledgerTx, err := reader.Read()
	events, diagErr := ledgerTx.GetDiagnosticEvents()
	if diagErr != nil {
		return []xdr.DiagnosticEvent{}, errors.Wrapf(err, "db read failed for startLedgerSequence %d", startLedgerSequence)
	}

	return events, nil
}
