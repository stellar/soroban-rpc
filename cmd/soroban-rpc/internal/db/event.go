package db

import (
	sq "github.com/Masterminds/squirrel"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/support/db"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/events"
	"io"
)

const eventTableName = "events"

type EventWriter interface {
	InsertEvents(lcm xdr.LedgerCloseMeta) error
}

type EventReader interface {
	GetEvents(lcm xdr.LedgerCloseMeta) error
}

type eventHandler struct {
	log                       *log.Entry
	db                        db.SessionInterface
	stmtCache                 *sq.StmtCache
	passphrase                string
	ingestMetric, countMetric prometheus.Observer
}

func (eventHandler *eventHandler) InsertEvents(lcm xdr.LedgerCloseMeta) (err error) {

	var txReader *ingest.LedgerTransactionReader
	txReader, err = ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(eventHandler.passphrase, lcm)
	if err != nil {
		return
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
			return
		}

		if !tx.Result.Successful() {
			continue
		}

		txEvents, err := tx.GetDiagnosticEvents()
		if err != nil {
			return err
		}

		query := sq.Insert(eventTableName).
			Columns("id", "ledger_sequence", "application_order", "contract_id", "event_type")

		for index, e := range txEvents {
			id := events.Cursor{Ledger: lcm.LedgerSequence(), Tx: tx.Index, Op: 0, Event: uint32(index)}.String()
			query = query.Values(id, lcm.LedgerSequence(), tx.Index, e.Event.ContractId[:], int(e.Event.Type))
		}

		_, err = query.RunWith(eventHandler.stmtCache).Exec()
		if err != nil {
			return err
		}
	}

	return nil
}
