package db

import (
	"encoding/hex"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/support/db"
	"github.com/stellar/go/xdr"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/ledgerbucketwindow"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/transactions"
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

type TransactionHandler struct {
	stmtCache  *sq.StmtCache
	db         db.SessionInterface
	passphrase string
}

func NewTransactionHandler(db db.SessionInterface, passphrase string) *TransactionHandler {
	return &TransactionHandler{db: db, passphrase: passphrase}
}

func (txn *TransactionHandler) InsertTransactions(lcm xdr.LedgerCloseMeta) error {
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(txn.passphrase, lcm)
	if err != nil {
		return err
	}
	txCount := lcm.CountTransactions()
	transactions := make(map[xdr.Hash]ingest.LedgerTransaction, txCount)

	for i := 0; i < txCount; i++ {
		tx, err := reader.Read()
		if err != nil {
			return err
		}

		if tx.Envelope.IsFeeBump() {
			transactions[tx.Result.InnerHash()] = tx
		}
		transactions[tx.Result.TransactionHash] = tx
	}

	query := sq.
		Insert(transactionTableName).
		RunWith(txn.stmtCache).
		Columns("hash", "ledger_sequence", "application_order")
	for hash, tx := range transactions {
		query = query.Values(hash, lcm.LedgerSequence(), tx.Index)
	}

	_, err = query.Exec()
	return err
}

func (txn *TransactionHandler) GetLedgerRange() ledgerbucketwindow.LedgerRange {
	var ledgerRange ledgerbucketwindow.LedgerRange
	newestQ := sq.
		Select("meta").
		From(ledgerCloseMetaTableName).
		OrderBy("sequence DESC").
		Limit(1).
		RunWith(txn.stmtCache).
		QueryRow()
	oldestQ := sq.
		Select("meta").
		From(ledgerCloseMetaTableName).
		OrderBy("sequence ASC").
		Limit(1).
		RunWith(txn.stmtCache).
		QueryRow()

	var row1, row2 []byte
	if err := newestQ.Scan(&row1); err != nil {
		return ledgerRange
	}
	if err := oldestQ.Scan(&row2); err != nil {
		return ledgerRange
	}

	var lcm1, lcm2 xdr.LedgerCloseMeta
	if err := lcm1.UnmarshalBinary(row1); err != nil {
		return ledgerRange
	}
	if err := lcm2.UnmarshalBinary(row2); err != nil {
		return ledgerRange
	}

	return ledgerbucketwindow.LedgerRange{
		FirstLedger: ledgerbucketwindow.LedgerInfo{
			Sequence:  lcm1.LedgerSequence(),
			CloseTime: int64(lcm1.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime),
		},
		LastLedger: ledgerbucketwindow.LedgerInfo{
			Sequence:  lcm2.LedgerSequence(),
			CloseTime: int64(lcm2.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime),
		},
	}
}

func (txn *TransactionHandler) GetTransactionByHash(hash string) (
	xdr.LedgerCloseMeta, ingest.LedgerTransaction, error,
) {
	rawHash, err := hex.DecodeString(hash)
	if err != nil {
		return xdr.LedgerCloseMeta{}, ingest.LedgerTransaction{}, err
	}

	rows := sq.
		Select("t.application_order", "lcm.meta").
		From(fmt.Sprintf("%s t", transactionTableName)).
		Join(fmt.Sprintf("%s lcm ON (t.ledger_sequence = lcm.sequence)", ledgerCloseMetaTableName)).
		Where(sq.Eq{"hash": rawHash}).
		Limit(1).
		RunWith(txn.stmtCache).
		QueryRow()

	var row struct {
		txIndex int
		meta    []byte
	}
	if err := rows.Scan(&row); err != nil {
		return xdr.LedgerCloseMeta{}, ingest.LedgerTransaction{}, err
	}

	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(row.meta); err != nil {
		return lcm, ingest.LedgerTransaction{}, err
	}

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(txn.passphrase, lcm)
	reader.Seek(row.txIndex - 1)
	if err != nil {
		return lcm, ingest.LedgerTransaction{}, err
	}

	ledgerTx, err := reader.Read()
	return lcm, ledgerTx, err
}

// GetTransaction conforms to the interface in
// methods/get_transaction.go#NewGetTransactionHandler so that it can be used
// directly against the RPC handler.
func (txn *TransactionHandler) GetTransaction(hash xdr.Hash) (
	transactions.Transaction, bool, ledgerbucketwindow.LedgerRange,
) {
	tx := transactions.Transaction{}

	ledgerRange := txn.GetLedgerRange()
	lcm, ingestTx, err := txn.GetTransactionByHash(hex.EncodeToString(hash[:]))
	if err != nil {
		return tx, false, ledgerRange
	}

	//
	// On-the-fly ingestion: extract all of the fields
	//

	if tx.Result, err = ingestTx.Result.MarshalBinary(); err != nil {
		return tx, false, ledgerRange
	}
	if tx.Meta, err = ingestTx.UnsafeMeta.MarshalBinary(); err != nil {
		return tx, false, ledgerRange
	}
	if tx.Envelope, err = ingestTx.Envelope.MarshalBinary(); err != nil {
		return tx, false, ledgerRange
	}
	if events, errr := ingestTx.GetDiagnosticEvents(); errr != nil {
		tx.Events = make([][]byte, 0, len(events))
		for _, event := range events {
			bytes, ierr := event.MarshalBinary()
			if ierr != nil {
				return tx, false, ledgerRange
			}
			tx.Events = append(tx.Events, bytes)
		}
	}
	tx.FeeBump = ingestTx.Envelope.IsFeeBump()
	tx.ApplicationOrder = int32(ingestTx.Index)
	tx.Successful = ingestTx.Result.Successful()
	tx.Ledger = ledgerbucketwindow.LedgerInfo{
		Sequence:  lcm.LedgerSequence(),
		CloseTime: int64(lcm.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime),
	}

	return tx, true, ledgerRange
}
