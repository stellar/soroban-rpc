package db

import (
	"context"
	"encoding/hex"
	"fmt"
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

type TransactionWriter interface {
	InsertTransactions(lcm xdr.LedgerCloseMeta) error
}

type TransactionReader interface {
	GetTransactionByHash(ctx context.Context, hash string) (
		xdr.LedgerCloseMeta, ingest.LedgerTransaction, error)
	GetTransaction(ctx context.Context, log *log.Entry, hash xdr.Hash) (
		Transaction, bool, ledgerbucketwindow.LedgerRange)
	GetLedgerRange(ctx context.Context) ledgerbucketwindow.LedgerRange
}

type transactionHandler struct {
	stmtCache  *sq.StmtCache
	db         db.SessionInterface
	passphrase string
}

func NewTransactionReader(db db.SessionInterface, passphrase string) TransactionReader {
	return &transactionHandler{db: db, passphrase: passphrase}
}

func (txn *transactionHandler) InsertTransactions(lcm xdr.LedgerCloseMeta) error {
	start := time.Now()
	txCount := lcm.CountTransactions()
	L := log.
		WithField("ledger_seq", lcm.LedgerSequence()).
		WithField("tx_count", txCount)

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

// TODO: Make this return an error (need to fix in interface)
func (txn *transactionHandler) GetLedgerRange(ctx context.Context) ledgerbucketwindow.LedgerRange {
	log.Debugf("Retrieving ledger range from database")

	ledgerRange := ledgerbucketwindow.LedgerRange{
		FirstLedger: ledgerbucketwindow.LedgerInfo{Sequence: 0, CloseTime: 0},
		LastLedger:  ledgerbucketwindow.LedgerInfo{Sequence: 0, CloseTime: 0},
	}

	// seqRangeQ := sq.
	// 	Select("MIN(sequence)", "MAX(sequence)").
	// 	From(ledgerCloseMetaTableName).
	// 	RunWith(txn.stmtCache).
	// 	QueryRow()

	// var seqMin, seqMax int
	// seqRangeQ.Scan(&seqMin, &seqMax)

	newestSql := sq.Select("meta").
		From(ledgerCloseMetaTableName).
		OrderBy("sequence DESC").
		Limit(1)
	newestQ, err := txn.db.Query(ctx, newestSql)
	defer newestQ.Close()
	if err != nil || !newestQ.Next() {
		log.WithField("error", err).
			WithField("qError", newestQ.Err()).
			Warn("Error fetching newest ledger")
		return ledgerRange
	}

	oldestSql := sq.Select("meta").
		From(ledgerCloseMetaTableName).
		OrderBy("sequence ASC").
		Limit(1)
	oldestQ, err := txn.db.Query(ctx, oldestSql)
	defer oldestQ.Close()
	if err != nil || !oldestQ.Next() {
		log.WithField("error", err).
			WithField("qError", oldestQ.Err()).
			Warn("Error fetching oldest ledger")
		return ledgerRange
	}

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
			CloseTime: int64(lcm1.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime),
		},
		LastLedger: ledgerbucketwindow.LedgerInfo{
			Sequence:  lcm2.LedgerSequence(),
			CloseTime: int64(lcm2.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime),
		},
	}

	log.Debugf("Database ledger range: [%d, %d]",
		ledgerRange.FirstLedger.Sequence, ledgerRange.LastLedger.Sequence)

	return ledgerRange
}

func (txn *transactionHandler) GetTransactionByHash(ctx context.Context, hash string) (
	xdr.LedgerCloseMeta, ingest.LedgerTransaction, error,
) {
	// input sanitization
	if _, err := hex.DecodeString(hash); err != nil || len(hash) != 64 {
		return xdr.LedgerCloseMeta{}, ingest.LedgerTransaction{},
			fmt.Errorf("invalid transaction hash: %s", hash)
	}

	rowSql := sq.Select("t.application_order", "lcm.meta").
		From(fmt.Sprintf("%s t", transactionTableName)).
		Join(fmt.Sprintf("%s lcm ON (t.ledger_sequence = lcm.sequence)", ledgerCloseMetaTableName)).
		Where(sq.Eq{"t.hash": hash}).
		Limit(1)

	rowQ, err := txn.db.Query(ctx, rowSql)
	defer rowQ.Close()
	if err != nil || !rowQ.Next() {
		return xdr.LedgerCloseMeta{}, ingest.LedgerTransaction{},
			fmt.Errorf("error fetching transaction: %v, %v", err, rowQ.Err())
	}

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

// GetTransaction conforms to the interface in
// methods/get_transaction.go#NewGetTransactionHandler so that it can be used
// directly against the RPC handler.
//
// Errors (i.e. the bool being false) should only occur if the XDR is out of
// date or is otherwise incorrect/corrupted or the tx isn't in the DB.
func (txn *transactionHandler) GetTransaction(ctx context.Context, log *log.Entry, hash xdr.Hash) (
	Transaction, bool, ledgerbucketwindow.LedgerRange,
) {
	start := time.Now()
	tx := Transaction{}
	ledgerRange := txn.GetLedgerRange(ctx)
	hexHash := hex.EncodeToString(hash[:])
	lcm, ingestTx, err := txn.GetTransactionByHash(ctx, hexHash)
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
		CloseTime: int64(lcm.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime),
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
