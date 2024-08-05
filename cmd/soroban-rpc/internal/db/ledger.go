package db

import (
	"context"
	"database/sql"
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
	NewTx(ctx context.Context) (LedgerReaderTx, error)
}

type LedgerWriter interface {
	InsertLedger(ledger xdr.LedgerCloseMeta) error
}

type ledgerReader struct {
	db *DB
}

type LedgerReaderTx interface {
	GetLedgerRange(ctx context.Context) (ledgerbucketwindow.LedgerRange, error)
}

type ledgerReaderTx struct {
	db                         *DB
	latestLedgerSeqCache       uint32
	latestLedgerCloseTimeCache int64
}

// GetLedgerRange pulls the min/max ledger sequence numbers from the meta table.
func (r *ledgerReaderTx) GetLedgerRange(ctx context.Context) (ledgerbucketwindow.LedgerRange, error) {
	// Make use of the cached latest ledger seq and close time to query only the oldest ledger details.
	if r.latestLedgerSeqCache != 0 {
		query := sq.Select("meta").
			From(ledgerCloseMetaTableName).
			Where(
				sq.Expr("sequence = (?)", sq.Select("MIN(sequence)").From(ledgerCloseMetaTableName)),
			)
		var lcm []xdr.LedgerCloseMeta
		if err := r.db.Select(ctx, &lcm, query); err != nil {
			return ledgerbucketwindow.LedgerRange{}, fmt.Errorf("couldn't query ledger range: %w", err)
		}

		return ledgerbucketwindow.LedgerRange{
			FirstLedger: ledgerbucketwindow.LedgerInfo{
				Sequence:  lcm[0].LedgerSequence(),
				CloseTime: lcm[0].LedgerCloseTime(),
			},
			LastLedger: ledgerbucketwindow.LedgerInfo{
				Sequence:  r.latestLedgerSeqCache,
				CloseTime: r.latestLedgerCloseTimeCache,
			},
		}, nil

	}

	query := sq.Select("lcm.meta").
		From(ledgerCloseMetaTableName + " as lcm").
		Where(sq.Or{
			sq.Expr("lcm.sequence = (?)", sq.Select("MIN(sequence)").From(ledgerCloseMetaTableName)),
			sq.Expr("lcm.sequence = (?)", sq.Select("MAX(sequence)").From(ledgerCloseMetaTableName)),
		}).OrderBy("lcm.sequence ASC")

	var lcms []xdr.LedgerCloseMeta
	if err := r.db.Select(ctx, &lcms, query); err != nil {
		return ledgerbucketwindow.LedgerRange{}, fmt.Errorf("couldn't query ledger range: %w", err)
	}

	// Empty DB
	if len(lcms) == 0 {
		return ledgerbucketwindow.LedgerRange{}, ErrEmptyDB
	}

	r.latestLedgerSeqCache = lcms[len(lcms)-1].LedgerSequence()
	r.latestLedgerCloseTimeCache = lcms[len(lcms)-1].LedgerCloseTime()

	return ledgerbucketwindow.LedgerRange{
		FirstLedger: ledgerbucketwindow.LedgerInfo{
			Sequence:  lcms[0].LedgerSequence(),
			CloseTime: lcms[0].LedgerCloseTime(),
		},
		LastLedger: ledgerbucketwindow.LedgerInfo{
			Sequence:  lcms[len(lcms)-1].LedgerSequence(),
			CloseTime: lcms[len(lcms)-1].LedgerCloseTime(),
		},
	}, nil
}

func NewLedgerReader(db *DB) LedgerReader {
	return ledgerReader{db: db}
}

func (r ledgerReader) NewTx(ctx context.Context) (LedgerReaderTx, error) {
	txSession := r.db.Clone()
	if err := txSession.BeginTx(ctx, &sql.TxOptions{ReadOnly: true}); err != nil {
		return nil, err
	}
	r.db.cache.RLock()
	defer r.db.cache.RUnlock()
	return &ledgerReaderTx{
		db:                         r.db,
		latestLedgerSeqCache:       r.db.cache.latestLedgerSeq,
		latestLedgerCloseTimeCache: r.db.cache.latestLedgerCloseTime,
	}, nil
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
