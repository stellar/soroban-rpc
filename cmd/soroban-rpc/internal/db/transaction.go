package db

import (
	"context"
	"database/sql"
	"encoding/hex"
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

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/ledgerbucketwindow"
)

const (
	transactionTableName = "transactions"
)

type Transaction struct {
	Result           []byte   // XDR encoded xdr.TransactionResult
	Meta             []byte   // XDR encoded xdr.TransactionMeta
	Envelope         []byte   // XDR encoded xdr.TransactionEnvelope
	Events           [][]byte // XDR encoded xdr.DiagnosticEvent
	FeeBump          bool
	ApplicationOrder int32
	Successful       bool
	Ledger           ledgerbucketwindow.LedgerInfo
}

// TransactionWriter is used during ingestion
type TransactionWriter interface {
	InsertTransactions(lcm xdr.LedgerCloseMeta) error
	RegisterMetrics(ingest, insert, count prometheus.Observer)
}

// TransactionReader is used to serve requests and just returns a cloned,
// read-only DB session to perform actual DB reads on.
type TransactionReader interface {
	NewTx(ctx context.Context) (TransactionReaderTx, error)
	GetLedgerRange(ctx context.Context) ledgerbucketwindow.LedgerRange
}

// TransactionReaderTx provides all of the public ways to read from the DB.
// Note that `Done()` *MUST* be called to clean things up.
type TransactionReaderTx interface {
	GetTransaction(log *log.Entry, hash xdr.Hash) (Transaction, bool, ledgerbucketwindow.LedgerRange)
	GetLedgerRange() ledgerbucketwindow.LedgerRange
	Done() error
}

type transactionHandler struct {
	db         db.SessionInterface
	stmtCache  *sq.StmtCache
	passphrase string

	ingestMetric, insertMetric, countMetric prometheus.Observer
}

type transactionReaderTx struct {
	ctx        context.Context
	db         db.SessionInterface
	passphrase string
}

func NewTransactionReader(db db.SessionInterface, passphrase string) TransactionReader {
	return &transactionHandler{db: db, passphrase: passphrase}
}

func NewTransactionWriter(db db.SessionInterface, stmtCache *sq.StmtCache, passphrase string) TransactionWriter {
	return &transactionHandler{db: db, stmtCache: stmtCache, passphrase: passphrase}
}

func (txn *transactionHandler) InsertTransactions(lcm xdr.LedgerCloseMeta) error {
	start := time.Now()
	txCount := lcm.CountTransactions()
	L := log.
		WithField("ledger_seq", lcm.LedgerSequence()).
		WithField("tx_count", txCount)

	if txn.stmtCache == nil {
		return errors.New("TransactionWriter incorrectly initialized without stmtCache")
	}

	if txCount == 0 {
		L.Warnf("No transactions present in ledger: %+v", lcm)
		return nil
	}

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(txn.passphrase, lcm)
	if err != nil {
		return errors.Wrapf(err,
			"failed to open transaction reader for ledger %d",
			lcm.LedgerSequence())
	}

	transactions := make(map[xdr.Hash]ingest.LedgerTransaction, txCount)
	for i := 0; i < txCount; i++ {
		tx, err := reader.Read()
		if err != nil {
			return errors.Wrapf(err, "failed reading tx %d", i)
		}

		// For fee-bump transactions, we store lookup entries for both the outer
		// and inner hashes.
		if tx.Envelope.IsFeeBump() {
			transactions[tx.Result.InnerHash()] = tx
		}
		transactions[tx.Result.TransactionHash] = tx
	}

	mid := time.Now()
	L.WithField("passphrase", txn.passphrase).
		Debugf("Ingesting %d transaction lookups from ledger", len(transactions))

	query := sq.Insert(transactionTableName).
		Columns("hash", "ledger_sequence", "application_order", "is_soroban")
	for hash, tx := range transactions {
		hexHash := hex.EncodeToString(hash[:])
		is_soroban := (tx.UnsafeMeta.V == 3 && tx.UnsafeMeta.V3.SorobanMeta != nil)
		query = query.Values(hexHash, lcm.LedgerSequence(), tx.Index, is_soroban)
	}
	_, err = query.RunWith(txn.stmtCache).Exec()

	L.WithError(err).
		WithField("duration", time.Since(start)).
		WithField("sql_duration", time.Since(mid)).
		Infof("Ingested %d transaction lookups", len(transactions))

	if txn.ingestMetric != nil {
		txn.ingestMetric.Observe(time.Since(start).Seconds())
		txn.insertMetric.Observe(time.Since(mid).Seconds())
		txn.countMetric.Observe(float64(txCount))
	}

	return err
}

func (txn *transactionHandler) RegisterMetrics(ingest, insert, count prometheus.Observer) {
	txn.ingestMetric = ingest
	txn.insertMetric = insert
	txn.countMetric = count
}

// trimTransactions removes all transactions which fall outside the ledger retention window.
func (txn *transactionHandler) trimTransactions(latestLedgerSeq uint32, retentionWindow uint32) error {
	if latestLedgerSeq+1 <= retentionWindow {
		return nil
	}

	cutoff := latestLedgerSeq + 1 - retentionWindow
	_, err := sq.StatementBuilder.
		RunWith(txn.stmtCache).
		Delete(transactionTableName).
		Where(sq.Lt{"ledger_sequence": cutoff}).
		Exec()
	return err
}

// NewTx creates a read-only SQL transaction on a cloned database session. You
// MUST call `.Done()` on the resulting reader if there are no errors.
func (txn *transactionHandler) NewTx(ctx context.Context) (TransactionReaderTx, error) {
	sesh := txn.db.Clone()
	if err := sesh.BeginTx(ctx, &sql.TxOptions{ReadOnly: true}); err != nil {
		return nil, err
	}

	return &transactionReaderTx{ctx: ctx, db: sesh, passphrase: txn.passphrase}, nil
}

// GetLedgerRange will fetch from the database in an isolated tx.
func (txn *transactionHandler) GetLedgerRange(ctx context.Context) ledgerbucketwindow.LedgerRange {
	reader, err := txn.NewTx(ctx)
	defer reader.Done()
	if err != nil {
		return ledgerbucketwindow.LedgerRange{}
	}

	lRange := reader.GetLedgerRange()
	return lRange
}

func (txn *transactionReaderTx) Done() error {
	return txn.db.Rollback()
}

// GetLedgerRange pulls the min/max ledger sequence numbers from the database.
func (txn *transactionReaderTx) GetLedgerRange() ledgerbucketwindow.LedgerRange {
	var ledgerRange ledgerbucketwindow.LedgerRange
	log.Debugf("Retrieving ledger range from database: %+v", txn)

	sqlTx := txn.db.GetTx()
	newestQ := sq.Select("meta").
		From(ledgerCloseMetaTableName).
		OrderBy("sequence DESC").
		Limit(1).
		RunWith(sqlTx)
	rows, err := newestQ.Query()
	if err != nil {
		log.Errorf("Error when querying for latest ledger: %v", err)
		return ledgerRange
	}

	// There is almost certainly a row, but we want to avoid a race condition
	// with ingestion as well as support test cases from an empty DB, so we need
	// to sanity check that there is in fact a result.
	if !rows.Next() {
		if ierr := rows.Err(); ierr != nil {
			log.Errorf("Error when querying for latest ledger: %v", ierr)
		}
		return ledgerRange
	}

	var row1, row2 []byte
	var lcm1, lcm2 xdr.LedgerCloseMeta
	if err := rows.Scan(&row1); err != nil {
		log.Errorf("Error when scanning newest ledger: %v", err)
		return ledgerRange
	}
	if err := lcm1.UnmarshalBinary(row1); err != nil {
		log.Errorf("Error when unmarshaling newest ledger: %v", err)
		return ledgerRange
	}
	// Best effort: if the second fails later, we at least set the first.
	ledgerRange.FirstLedger.Sequence = lcm1.LedgerSequence()
	ledgerRange.FirstLedger.CloseTime = lcm1.LedgerCloseTime()
	ledgerRange.LastLedger.Sequence = lcm1.LedgerSequence()
	ledgerRange.LastLedger.CloseTime = lcm1.LedgerCloseTime()

	oldestQ := sq.Select("meta").
		From(ledgerCloseMetaTableName).
		OrderBy("sequence ASC").
		Limit(1).
		RunWith(sqlTx).
		QueryRow()

	if err := oldestQ.Scan(&row2); err != nil {
		log.Errorf("Error when scanning oldest ledger: %v", err)
		return ledgerRange
	}
	if err := lcm2.UnmarshalBinary(row2); err != nil {
		log.Errorf("Error when unmarshaling oldest ledger: %v", err)
		return ledgerRange
	}
	ledgerRange.FirstLedger.Sequence = lcm2.LedgerSequence()
	ledgerRange.FirstLedger.CloseTime = lcm2.LedgerCloseTime()

	log.Debugf("Database ledger range: [%d, %d]",
		ledgerRange.FirstLedger.Sequence, ledgerRange.LastLedger.Sequence)
	return ledgerRange
}

// GetTransaction conforms to the interface in
// methods/get_transaction.go#NewGetTransactionHandler so that it can be used
// directly against the RPC handler.
//
// Errors (i.e. the bool being false) should only occur if the XDR is out of
// date or is otherwise incorrect/corrupted or the tx really isn't in the DB.
func (txn *transactionReaderTx) GetTransaction(log *log.Entry, hash xdr.Hash) (
	Transaction, bool, ledgerbucketwindow.LedgerRange,
) {
	start := time.Now()
	tx := Transaction{}
	ledgerRange := txn.GetLedgerRange()
	hexHash := hex.EncodeToString(hash[:])
	lcm, ingestTx, err := txn.getTransactionByHash(hexHash)
	if err != nil {
		if err != io.EOF { // mark for "not in db"
			log.WithField("error", err).
				WithField("txhash", hexHash).
				Errorf("Failed to fetch transaction from database")
		}
		return tx, false, ledgerRange
	}

	//
	// On-the-fly ingestion: extract all of the fields, return best effort.
	//
	tx.FeeBump = ingestTx.Envelope.IsFeeBump()
	tx.ApplicationOrder = int32(ingestTx.Index)
	tx.Successful = ingestTx.Result.Successful()
	tx.Ledger = ledgerbucketwindow.LedgerInfo{
		Sequence:  lcm.LedgerSequence(),
		CloseTime: lcm.LedgerCloseTime(),
	}

	if tx.Result, err = ingestTx.Result.Result.MarshalBinary(); err != nil {
		log.WithError(err).Errorf("Failed to encode transaction Result")
		return tx, false, ledgerRange
	}
	if tx.Meta, err = ingestTx.UnsafeMeta.MarshalBinary(); err != nil {
		log.WithError(err).Errorf("Failed to encode transaction UnsafeMeta")
		return tx, false, ledgerRange
	}
	if tx.Envelope, err = ingestTx.Envelope.MarshalBinary(); err != nil {
		log.WithError(err).Errorf("Failed to encode transaction Envelope")
		return tx, false, ledgerRange
	}
	if events, diagErr := ingestTx.GetDiagnosticEvents(); diagErr == nil {
		tx.Events = make([][]byte, 0, len(events))
		for i, event := range events {
			bytes, ierr := event.MarshalBinary()
			if ierr != nil {
				log.WithError(ierr).
					Errorf("Failed to encode transaction DiagnosticEvent %d", i)
				return tx, false, ledgerRange
			}
			tx.Events = append(tx.Events, bytes)
		}
	} else {
		log.WithError(diagErr).
			Errorf("Failed to encode transaction DiagnosticEvents")
		return tx, false, ledgerRange
	}

	log.WithField("txhash", hexHash).
		WithField("duration", time.Since(start)).
		Debugf("Fetched and encoded transaction from ledger %d",
			lcm.LedgerSequence())

	return tx, true, ledgerRange
}

// getTransactionByHash actually performs the DB ops to cross-reference a
// transaction hash with a particular set of ledger close meta and parses out
// the relevant transaction efficiently by leveraging the `application_order` db
// field.
//
// Note: Caller must do input sanitization on the hash.
func (txn *transactionReaderTx) getTransactionByHash(hash string) (
	xdr.LedgerCloseMeta, ingest.LedgerTransaction, error,
) {
	rowQ := sq.Select("t.application_order", "lcm.meta").
		From(fmt.Sprintf("%s t", transactionTableName)).
		Join(fmt.Sprintf("%s lcm ON (t.ledger_sequence = lcm.sequence)", ledgerCloseMetaTableName)).
		Where(sq.Eq{"t.hash": hash}).
		Limit(1).
		RunWith(txn.db.GetTx())

	rows, err := rowQ.Query()
	if err != nil {
		return xdr.LedgerCloseMeta{}, ingest.LedgerTransaction{},
			errors.Wrapf(err, "failed row read for txhash %s", hash)
	}
	if !rows.Next() {
		if ierr := rows.Err(); ierr != nil {
			return xdr.LedgerCloseMeta{}, ingest.LedgerTransaction{},
				errors.Wrapf(ierr, "failed to fetch txhash %s", hash)
		}

		// We use EOF to mark "this tx doesn't exist," since that isn't a real error.
		return xdr.LedgerCloseMeta{}, ingest.LedgerTransaction{}, io.EOF
	}

	var (
		txIndex int
		meta    []byte
	)
	if err := rows.Scan(&txIndex, &meta); err != nil {
		return xdr.LedgerCloseMeta{}, ingest.LedgerTransaction{},
			errors.Wrapf(err, "db read from %s failed for txhash %s", transactionTableName, hash)
	}

	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(meta); err != nil {
		return lcm, ingest.LedgerTransaction{},
			errors.Wrapf(err, "ledger meta decode failed for txhash %s", hash)
	}

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(txn.passphrase, lcm)
	reader.Seek(txIndex - 1)
	if err != nil {
		return lcm, ingest.LedgerTransaction{},
			errors.Wrapf(err, "failed to index to tx %d in ledger %d (txhash=%s)",
				txIndex, lcm.LedgerSequence(), hash)
	}

	ledgerTx, err := reader.Read()
	return lcm, ledgerTx, err
}
