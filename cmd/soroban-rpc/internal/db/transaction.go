package db

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	sq "github.com/Masterminds/squirrel"
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
	db          db.SessionInterface
	stmtCache   *sq.StmtCache
	passphrase  string
	ledgerRange ledgerRangeCache
}

type transactionReaderTx struct {
	ctx        context.Context
	db         db.SessionInterface
	passphrase string
}

func NewTransactionReader(db db.SessionInterface, passphrase string) TransactionReader {
	return &transactionHandler{db: db, passphrase: passphrase}
}

func (txn *transactionHandler) InsertTransactions(lcm xdr.LedgerCloseMeta) error {
	start := time.Now()
	txn.ledgerRange.Update(&lcm)
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
		WithField("total_duration", time.Since(start)).
		WithField("sql_duration", time.Since(mid)).
		Infof("Ingested %d transaction lookups", len(transactions))
	return err
}

func (txn *transactionHandler) NewTx(ctx context.Context) (TransactionReaderTx, error) {
	sesh := txn.db.Clone()
	if err := sesh.BeginTx(ctx, &sql.TxOptions{ReadOnly: true}); err != nil {
		return nil, err
	}

	return &transactionReaderTx{ctx: ctx, db: sesh, passphrase: txn.passphrase}, nil
}

// GetLedgerRange will either return its cached ledger range or will actually
// fetch it from the database and update the internal cache for next time.
func (txn *transactionHandler) GetLedgerRange(ctx context.Context) ledgerbucketwindow.LedgerRange {
	if txn.ledgerRange.IsLoaded() {
		return txn.ledgerRange.Read()
	}

	reader, err := txn.NewTx(ctx)
	defer reader.Done()
	if err != nil {
		return ledgerbucketwindow.LedgerRange{}
	}

	lRange := reader.GetLedgerRange()
	txn.ledgerRange.Set(lRange)
	return lRange
}

func (txn *transactionReaderTx) Done() error {
	return txn.db.Rollback()
}

// TODO: Make this return an error (need to fix in interface)
func (txn *transactionReaderTx) GetLedgerRange() ledgerbucketwindow.LedgerRange {
	var ledgerRange ledgerbucketwindow.LedgerRange
	log.Debugf("Retrieving ledger range from database: %+v", txn)

	// TODO: Evaluate whether or not this would be faster:
	//  SELECT MIN(sequence) AS minSeq, MAX(sequence) AS maxSeq FROM ledger_close_meta;
	//  SELECT meta FROM ledger_close_meta WHERE sequence = minSeq OR sequence = maxSeq;

	sqlTx := txn.db.GetTx()
	newestQ := sq.Select("meta").
		From(ledgerCloseMetaTableName).
		OrderBy("sequence DESC").
		Limit(1).
		RunWith(sqlTx).
		QueryRow()

	oldestQ := sq.Select("meta").
		From(ledgerCloseMetaTableName).
		OrderBy("sequence ASC").
		Limit(1).
		RunWith(sqlTx).
		QueryRow()

	var row1, row2 []byte
	if err := newestQ.Scan(&row1); err != nil {
		log.Warnf("Error when scanning newest ledger: %v", err)
		return ledgerRange
	}
	if err := oldestQ.Scan(&row2); err != nil {
		log.Warnf("Error when scanning oldest ledger: %v", err)
		return ledgerRange
	}

	var lcm1, lcm2 xdr.LedgerCloseMeta
	if err := lcm1.UnmarshalBinary(row1); err != nil {
		log.Warnf("Error when unmarshaling newest ledger: %v", err)
		return ledgerRange
	}
	if err := lcm2.UnmarshalBinary(row2); err != nil {
		log.Warnf("Error when unmarshaling oldest ledger: %v", err)
		return ledgerRange
	}

	ledgerRange = ledgerbucketwindow.LedgerRange{
		FirstLedger: ledgerbucketwindow.LedgerInfo{
			Sequence:  lcm1.LedgerSequence(),
			CloseTime: lcm1.LedgerCloseTime(),
		},
		LastLedger: ledgerbucketwindow.LedgerInfo{
			Sequence:  lcm2.LedgerSequence(),
			CloseTime: lcm2.LedgerCloseTime(),
		},
	}

	log.Debugf("Database ledger range: [%d, %d]",
		ledgerRange.FirstLedger.Sequence, ledgerRange.LastLedger.Sequence)

	return ledgerRange
}

// GetTransaction conforms to the interface in
// methods/get_transaction.go#NewGetTransactionHandler so that it can be used
// directly against the RPC handler.
//
// Errors (i.e. the bool being false) should only occur if the XDR is out of
// date or is otherwise incorrect/corrupted or the tx isn't in the DB.
func (txn *transactionReaderTx) GetTransaction(log *log.Entry, hash xdr.Hash) (
	Transaction, bool, ledgerbucketwindow.LedgerRange,
) {
	start := time.Now()
	tx := Transaction{}
	ledgerRange := txn.GetLedgerRange()
	hexHash := hex.EncodeToString(hash[:])
	lcm, ingestTx, err := txn.getTransactionByHash(hexHash)
	if err != nil {
		log.WithField("error", err).
			WithField("txhash", hexHash).
			Errorf("Failed to fetch transaction from database")
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

	if tx.Result, err = ingestTx.Result.MarshalBinary(); err != nil {
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
		RunWith(txn.db.GetTx()).
		QueryRow()

	var (
		txIndex int
		meta    []byte
	)
	if err := rowQ.Scan(&txIndex, &meta); err != nil {
		return xdr.LedgerCloseMeta{}, ingest.LedgerTransaction{},
			errors.Wrapf(err, "failed row read from %s for txhash=%s", transactionTableName, hash)
	}

	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(meta); err != nil {
		return lcm, ingest.LedgerTransaction{},
			errors.Wrapf(err, "failed to decode ledger meta for txhash=%s", hash)
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

// ledgerRangeCache is a small abstraction to make it easier to store the known
// ledger range in memory rather than fetching it from the database every time.
type ledgerRangeCache struct {
	sync.RWMutex
	ledgerRange ledgerbucketwindow.LedgerRange
}

func (lr *ledgerRangeCache) IsLoaded() bool {
	// There's no need for a reader lock because we don't care what the value is
	// as long as it's non-zero for both.
	return lr.ledgerRange.FirstLedger.Sequence != 0 &&
		lr.ledgerRange.LastLedger.Sequence != 0
}

func (lr *ledgerRangeCache) Set(ledgerRange ledgerbucketwindow.LedgerRange) {
	lr.Lock()
	defer lr.Unlock()
	lr.ledgerRange = ledgerRange
}

// Update preserves the condition that the upper ledger bound only increases.
func (lr *ledgerRangeCache) Update(lcm *xdr.LedgerCloseMeta) {
	lr.Lock()
	defer lr.Unlock()
	if lcm.LedgerSequence() > lr.ledgerRange.LastLedger.Sequence {
		lr.ledgerRange.LastLedger.Sequence = lcm.LedgerSequence()
		lr.ledgerRange.LastLedger.CloseTime = lcm.LedgerCloseTime()
	}
}

func (lr *ledgerRangeCache) Read() ledgerbucketwindow.LedgerRange {
	lr.RLock()
	defer lr.RUnlock()
	return lr.ledgerRange
}
