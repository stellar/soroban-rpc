package db

import (
	"context"
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

// TransactionWriter is used during ingestion to write LCM.
type TransactionWriter interface {
	InsertTransactions(lcm xdr.LedgerCloseMeta) error
	RegisterMetrics(ingest, insert, count prometheus.Observer)
}

// TransactionReader provides all of the public ways to read from the DB.
type TransactionReader interface {
	GetTransaction(ctx context.Context, hash xdr.Hash) (Transaction, ledgerbucketwindow.LedgerRange, error)
	GetLedgerRange(ctx context.Context) (ledgerbucketwindow.LedgerRange, error)
}

type transactionHandler struct {
	log        *log.Entry
	db         db.SessionInterface
	stmtCache  *sq.StmtCache
	passphrase string

	ingestMetric, insertMetric, countMetric prometheus.Observer
}

func NewTransactionReader(log *log.Entry, db db.SessionInterface, passphrase string) TransactionReader {
	return &transactionHandler{log: log, db: db, passphrase: passphrase}
}

func (txn *transactionHandler) InsertTransactions(lcm xdr.LedgerCloseMeta) error {
	start, mid := time.Now(), time.Now()
	txCount := lcm.CountTransactions()
	L := txn.log.
		WithField("ledger_seq", lcm.LedgerSequence()).
		WithField("tx_count", txCount)

	defer func() {
		if txn.ingestMetric != nil {
			txn.ingestMetric.Observe(time.Since(start).Seconds())
			txn.insertMetric.Observe(time.Since(mid).Seconds())
			txn.countMetric.Observe(float64(txCount))
		}
	}()

	if txn.stmtCache == nil {
		return errors.New("TransactionWriter incorrectly initialized without stmtCache")
	} else if txCount == 0 {
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

	mid = time.Now()
	query := sq.Insert(transactionTableName).
		Columns("hash", "ledger_sequence", "application_order")
	for hash, tx := range transactions {
		query = query.Values(hash[:], lcm.LedgerSequence(), tx.Index)
	}
	_, err = query.RunWith(txn.stmtCache).Exec()

	L.WithError(err).
		WithField("duration", time.Since(start)).
		WithField("sql_duration", time.Since(mid)).
		Infof("Ingested %d transaction lookups", len(transactions))

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

// GetLedgerRange pulls the min/max ledger sequence numbers from the database.
func (txn *transactionHandler) GetLedgerRange(ctx context.Context) (ledgerbucketwindow.LedgerRange, error) {
	var ledgerRange ledgerbucketwindow.LedgerRange

	//
	// We use subqueries alongside a UNION ALL stitch in order to select the min
	// and max from the ledger table in a single query and get around sqlite's
	// limitations with parentheses (see https://stackoverflow.com/a/22609948).
	//
	newestQ := sq.
		Select("m1.meta").
		FromSelect(
			sq.
				Select("meta").
				From(ledgerCloseMetaTableName).
				OrderBy("sequence ASC").
				Limit(1),
			"m1",
		)
	oldestQ := sq.
		Select("m2.meta").
		FromSelect(
			sq.
				Select("meta").
				From(ledgerCloseMetaTableName).
				OrderBy("sequence DESC").
				Limit(1),
			"m2",
		)

	sql, args, err := oldestQ.ToSql()
	if err != nil {
		return ledgerRange, errors.Wrap(err, "couldn't build ledger range query")
	}

	rows, err := txn.db.Query(ctx, newestQ.Suffix(" UNION ALL "+sql, args...))
	if err != nil {
		return ledgerRange, errors.Wrap(err, "couldn't query ledger range")
	}

	var lcm1, lcm2 xdr.LedgerCloseMeta
	// There is almost certainly a row, but we want to avoid a race condition
	// with ingestion as well as support test cases from an empty DB, so we need
	// to sanity check that there is in fact a result. Note that no ledgers in
	// the database isn't an error, it's just an empty range.
	if !rows.Next() {
		return ledgerRange, errors.Wrap(rows.Err(), "couldn't query ledger range") // nil if no row err
	} else if err := rows.Scan(&lcm1); err != nil {
		return ledgerRange, errors.Wrap(err, "couldn't read ledger")
	}

	// Best effort: if the second fails later, we at least set the first.
	ledgerRange.FirstLedger.Sequence = lcm1.LedgerSequence()
	ledgerRange.FirstLedger.CloseTime = lcm1.LedgerCloseTime()
	ledgerRange.LastLedger.Sequence = lcm1.LedgerSequence()
	ledgerRange.LastLedger.CloseTime = lcm1.LedgerCloseTime()

	if !rows.Next() {
		return ledgerRange, errors.Wrap(rows.Err(), "couldn't query ledger range") // nil if no row err
	} else if err := rows.Scan(&lcm2); err != nil {
		return ledgerRange, errors.Wrap(err, "couldn't read ledger")
	}

	ledgerRange.LastLedger.Sequence = lcm2.LedgerSequence()
	ledgerRange.LastLedger.CloseTime = lcm2.LedgerCloseTime()

	txn.log.Debugf("Database ledger range: [%d, %d]",
		ledgerRange.FirstLedger.Sequence, ledgerRange.LastLedger.Sequence)
	return ledgerRange, nil
}

// GetTransaction conforms to the interface in
// methods/get_transaction.go#NewGetTransactionHandler so that it can be used
// directly against the RPC handler.
//
// Errors occur if there are issues with the DB connection or the XDR is
// corrupted somehow. If the transaction is not found, io.EOF is returned.
func (txn *transactionHandler) GetTransaction(ctx context.Context, hash xdr.Hash) (
	Transaction, ledgerbucketwindow.LedgerRange, error,
) {
	start := time.Now()
	tx := Transaction{}

	ledgerRange, err := txn.GetLedgerRange(ctx)
	if err != nil && err != ErrEmptyDB {
		return tx, ledgerRange, err
	}

	lcm, ingestTx, err := txn.getTransactionByHash(ctx, hash)
	if err != nil {
		return tx, ledgerRange, err
	}
	tx, err = ParseTransaction(lcm, ingestTx)
	if err != nil {
		return tx, ledgerRange, err
	}

	txn.log.
		WithField("txhash", hex.EncodeToString(hash[:])).
		WithField("duration", time.Since(start)).
		Debugf("Fetched and encoded transaction from ledger %d", lcm.LedgerSequence())

	return tx, ledgerRange, nil
}

// getTransactionByHash actually performs the DB ops to cross-reference a
// transaction hash with a particular set of ledger close meta and parses out
// the relevant transaction efficiently by leveraging the `application_order` db
// field.
//
// Note: Caller must do input sanitization on the hash.
func (txn *transactionHandler) getTransactionByHash(ctx context.Context, hash xdr.Hash) (
	xdr.LedgerCloseMeta, ingest.LedgerTransaction, error,
) {
	rowQ := sq.Select("t.application_order", "lcm.meta").
		From(fmt.Sprintf("%s t", transactionTableName)).
		Join(fmt.Sprintf("%s lcm ON (t.ledger_sequence = lcm.sequence)", ledgerCloseMetaTableName)).
		Where(sq.Eq{"t.hash": []byte(hash[:])}).
		Limit(1)

	rows, err := txn.db.Query(ctx, rowQ)
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
		lcm     xdr.LedgerCloseMeta
	)
	if err := rows.Scan(&txIndex, &lcm); err != nil {
		return xdr.LedgerCloseMeta{}, ingest.LedgerTransaction{},
			errors.Wrapf(err, "db read failed for txhash %s", hash)
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

func ParseTransaction(lcm xdr.LedgerCloseMeta, ingestTx ingest.LedgerTransaction) (Transaction, error) {
	var tx Transaction
	var err error

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
		return tx, errors.Wrap(err, "couldn't encode transaction Result")
	}
	if tx.Meta, err = ingestTx.UnsafeMeta.MarshalBinary(); err != nil {
		return tx, errors.Wrap(err, "couldn't encode transaction UnsafeMeta")
	}
	if tx.Envelope, err = ingestTx.Envelope.MarshalBinary(); err != nil {
		return tx, errors.Wrap(err, "couldn't encode transaction Envelope")
	}
	if events, diagErr := ingestTx.GetDiagnosticEvents(); diagErr == nil {
		tx.Events = make([][]byte, 0, len(events))
		for i, event := range events {
			bytes, ierr := event.MarshalBinary()
			if ierr != nil {
				return tx, errors.Wrapf(ierr, "couldn't encode transaction DiagnosticEvent %d", i)
			}
			tx.Events = append(tx.Events, bytes)
		}
	} else {
		return tx, errors.Wrap(diagErr, "couldn't encode transaction DiagnosticEvents")
	}

	return tx, nil
}
