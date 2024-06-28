package db

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"

	"github.com/stellar/go/xdr"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/ledgerbucketwindow"
)

const (
	ledgerCloseMetaTableName = "ledger_close_meta"
)

type StreamLedgerFn func(xdr.LedgerCloseMeta) error

type LedgerReader interface {
	GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, bool, error)
	StreamAllLedgers(ctx context.Context, f StreamLedgerFn) error
	GetLedgerRange(ctx context.Context) (ledgerbucketwindow.LedgerRange, error)
}

type LedgerWriter interface {
	InsertLedger(ledger xdr.LedgerCloseMeta) error
}

type ledgerReader struct {
	db *DB
}

func NewLedgerReader(db *DB) LedgerReader {
	return ledgerReader{db: db}
}

// StreamAllLedgers runs f over all the ledgers in the database (until f errors or signals it's done).
func (r ledgerReader) StreamAllLedgers(ctx context.Context, f StreamLedgerFn) error {
	sql := sq.Select("meta").From(ledgerCloseMetaTableName).OrderBy("sequence asc")
	q, err := r.db.Query(ctx, sql)
	if err != nil {
		return err
	}
	defer q.Close()
	for q.Next() {
		var closeMeta xdr.LedgerCloseMeta
		if err = q.Scan(&closeMeta); err != nil {
			return err
		}
		if err = f(closeMeta); err != nil {
			return err
		}
	}
	return q.Err()
}

// GetLedger fetches a single ledger from the db.
func (r ledgerReader) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, bool, error) {
	sql := sq.Select("meta").From(ledgerCloseMetaTableName).Where(sq.Eq{"sequence": sequence})
	var results []xdr.LedgerCloseMeta
	if err := r.db.Select(ctx, &results, sql); err != nil {
		return xdr.LedgerCloseMeta{}, false, err
	}
	switch len(results) {
	case 0:
		return xdr.LedgerCloseMeta{}, false, nil
	case 1:
		return results[0], true, nil
	default:
		return xdr.LedgerCloseMeta{}, false, fmt.Errorf("multiple lcm entries (%d) for sequence %d in table %q",
			len(results), sequence, ledgerCloseMetaTableName)
	}
}

// GetLedgerRange pulls the min/max ledger sequence numbers from the meta table.
func (r ledgerReader) GetLedgerRange(ctx context.Context) (ledgerbucketwindow.LedgerRange, error) {
	var ledgerRange ledgerbucketwindow.LedgerRange

	query := sq.Select("lcm.meta").
		From(ledgerCloseMetaTableName + " as lcm").
		Where(sq.Or{
			sq.Expr("lcm.sequence = (?)", sq.Select("MIN(sequence)").From(ledgerCloseMetaTableName)),
			sq.Expr("lcm.sequence = (?)", sq.Select("MAX(sequence)").From(ledgerCloseMetaTableName)),
		})

	var lcms []xdr.LedgerCloseMeta
	if err := r.db.Select(ctx, &lcms, query); err != nil {
		return ledgerRange, fmt.Errorf("couldn't query ledger range: %w", err)
	} else if len(lcms) < 2 { //nolint:mnd
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
	return ledgerRange, nil
}

type ledgerWriter struct {
	stmtCache *sq.StmtCache
}

// trimLedgers removes all ledgers which fall outside the retention window.
func (l ledgerWriter) trimLedgers(latestLedgerSeq uint32, retentionWindow uint32) error {
	if latestLedgerSeq+1 <= retentionWindow {
		return nil
	}
	cutoff := latestLedgerSeq + 1 - retentionWindow
	_, err := sq.StatementBuilder.
		RunWith(l.stmtCache).
		Delete(ledgerCloseMetaTableName).
		Where(sq.Lt{"sequence": cutoff}).
		Exec()
	return err
}

// InsertLedger inserts a ledger in the db.
func (l ledgerWriter) InsertLedger(ledger xdr.LedgerCloseMeta) error {
	_, err := sq.StatementBuilder.RunWith(l.stmtCache).
		Insert(ledgerCloseMetaTableName).
		Values(ledger.LedgerSequence(), ledger).
		Exec()
	return err
}
