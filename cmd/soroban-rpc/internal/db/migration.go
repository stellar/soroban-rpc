package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
)

const (
	TransactionsMigrationName = "TransactionsTable"
	EventsMigrationName       = "EventsTable"
)

type LedgerSeqRange struct {
	FirstLedgerSeq uint32
	LastLedgerSeq  uint32
}

func (mlr *LedgerSeqRange) IsLedgerIncluded(ledgerSeq uint32) bool {
	if mlr == nil {
		return false
	}
	return ledgerSeq >= mlr.FirstLedgerSeq && ledgerSeq <= mlr.LastLedgerSeq
}

func (mlr *LedgerSeqRange) Merge(other *LedgerSeqRange) *LedgerSeqRange {
	if mlr == nil {
		return other
	}
	if other == nil {
		return mlr
	}
	// TODO: using min/max can result in a much larger range than needed,
	//       as an optimization, we should probably use a sequence of ranges instead.
	return &LedgerSeqRange{
		FirstLedgerSeq: min(mlr.FirstLedgerSeq, other.FirstLedgerSeq),
		LastLedgerSeq:  max(mlr.LastLedgerSeq, other.LastLedgerSeq),
	}
}

type MigrationApplier interface {
	// ApplicableRange returns the closed ledger sequence interval,
	// where Apply() should be called. A null result indicates the empty range
	ApplicableRange() *LedgerSeqRange
	// Apply applies the migration on a ledger. It should never be applied
	// in ledgers outside the ApplicableRange()
	Apply(ctx context.Context, meta xdr.LedgerCloseMeta) error
}

type migrationApplierFactory interface {
	New(db *DB) (MigrationApplier, error)
}

type migrationApplierFactoryF func(db *DB) (MigrationApplier, error)

func (m migrationApplierFactoryF) New(db *DB) (MigrationApplier, error) {
	return m(db)
}

type Migration interface {
	MigrationApplier
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type MigrationFactory struct {
	Factory       migrationApplierFactory
	DB            *DB
	Logger        *log.Entry
	MigrationName string
}

// guardedMigration is a db data migration whose application is guarded by a boolean in the meta table
// (after the migration is applied the boolean is set to true, so that the migration is not applied again)
type guardedMigration struct {
	guardMetaKey    string
	db              *DB
	migration       MigrationApplier
	alreadyMigrated bool
	logger          *log.Entry
	applyLogged     bool
}

func NewGuardedDataMigration(
	ctx context.Context, uniqueMigrationName string, logger *log.Entry, factory migrationApplierFactory, db *DB,
) (Migration, error) {
	migrationDB := &DB{
		cache:            db.cache,
		SessionInterface: db.SessionInterface.Clone(),
	}
	if err := migrationDB.Begin(ctx); err != nil {
		return nil, err
	}
	metaKey := "Migration" + uniqueMigrationName + "Done"
	previouslyMigrated, err := getMetaBool(ctx, migrationDB, metaKey)
	if err != nil && !errors.Is(err, ErrEmptyDB) {
		err = errors.Join(err, migrationDB.Rollback())
		return nil, err
	}
	applier, err := factory.New(migrationDB)
	if err != nil {
		err = errors.Join(err, migrationDB.Rollback())
		return nil, err
	}
	guardedMigration := &guardedMigration{
		guardMetaKey:    metaKey,
		db:              migrationDB,
		migration:       applier,
		alreadyMigrated: previouslyMigrated,
		logger:          logger,
	}
	return guardedMigration, nil
}

func (g *guardedMigration) Apply(ctx context.Context, meta xdr.LedgerCloseMeta) error {
	if g.alreadyMigrated {
		// This shouldn't happen since we would be out of the applicable range
		// but, just in case.
		return nil
	}
	if !g.applyLogged {
		g.logger.WithField("ledger", meta.LedgerSequence()).Info("applying migration")
		g.applyLogged = true
	}
	return g.migration.Apply(ctx, meta)
}

func (g *guardedMigration) ApplicableRange() *LedgerSeqRange {
	if g.alreadyMigrated {
		return nil
	}
	return g.migration.ApplicableRange()
}

func (g *guardedMigration) Commit(ctx context.Context) error {
	if g.alreadyMigrated {
		return g.Rollback(ctx)
	}

	err := setMetaBool(ctx, g.db, g.guardMetaKey, true)
	if err != nil {
		return errors.Join(err, g.Rollback(ctx))
	}
	return g.db.Commit()
}

func (g *guardedMigration) Rollback(_ context.Context) error {
	return g.db.Rollback()
}

func GetMigrationLedgerRange(ctx context.Context, db *DB, retentionWindow uint32) (*LedgerSeqRange, error) {
	firstLedgerToMigrate := firstLedger
	latestLedger, err := NewLedgerEntryReader(db).GetLatestLedgerSequence(ctx)
	if err != nil && !errors.Is(err, ErrEmptyDB) {
		return nil, fmt.Errorf("failed to get latest ledger sequence: %w", err)
	}
	if latestLedger > retentionWindow {
		firstLedgerToMigrate = latestLedger - retentionWindow
	}
	return &LedgerSeqRange{
		FirstLedgerSeq: firstLedgerToMigrate,
		LastLedgerSeq:  latestLedger,
	}, nil
}

func BuildMigrations(ctx context.Context, logger *log.Entry, db *DB, networkPassphrase string,
	ledgerSeqRange *LedgerSeqRange,
) ([]MigrationFactory, error) {
	var migrations []MigrationFactory

	transactionFactory := newTransactionTableMigration(
		ctx,
		logger.WithField("migration", TransactionsMigrationName),
		networkPassphrase,
		ledgerSeqRange,
	)
	migrations = append(migrations, MigrationFactory{
		Factory:       transactionFactory,
		DB:            db,
		Logger:        logger,
		MigrationName: TransactionsMigrationName,
	})

	eventFactory := newEventTableMigration(
		logger.WithField("migration", EventsMigrationName),
		networkPassphrase,
		ledgerSeqRange,
	)
	migrations = append(migrations, MigrationFactory{
		Factory:       eventFactory,
		DB:            db,
		Logger:        logger,
		MigrationName: EventsMigrationName,
	})

	return migrations, nil
}
