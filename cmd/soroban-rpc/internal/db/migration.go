package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/config"
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
	New(db *DB, latestLedger uint32) (MigrationApplier, error)
}

type migrationApplierFactoryF func(db *DB, latestLedger uint32) (MigrationApplier, error)

func (m migrationApplierFactoryF) New(db *DB, latestLedger uint32) (MigrationApplier, error) {
	return m(db, latestLedger)
}

type Migration interface {
	MigrationApplier
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type multiMigration []Migration

func (mm multiMigration) ApplicableRange() *LedgerSeqRange {
	var result *LedgerSeqRange
	for _, m := range mm {
		result = m.ApplicableRange().Merge(result)
	}
	return result
}

func (mm multiMigration) Apply(ctx context.Context, meta xdr.LedgerCloseMeta) error {
	var err error
	for _, m := range mm {
		ledgerSeq := meta.LedgerSequence()
		if !m.ApplicableRange().IsLedgerIncluded(ledgerSeq) {
			// The range of a sub-migration can be smaller than the global range.
			continue
		}
		if localErr := m.Apply(ctx, meta); localErr != nil {
			err = errors.Join(err, localErr)
		}
	}
	return err
}

func (mm multiMigration) Commit(ctx context.Context) error {
	var err error
	for _, m := range mm {
		if localErr := m.Commit(ctx); localErr != nil {
			err = errors.Join(err, localErr)
		}
	}
	return err
}

func (mm multiMigration) Rollback(ctx context.Context) error {
	var err error
	for _, m := range mm {
		if localErr := m.Rollback(ctx); localErr != nil {
			err = errors.Join(err, localErr)
		}
	}
	return err
}

// guardedMigration is a db data migration whose application is guarded by a boolean in the meta table
// (after the migration is applied the boolean is set to true, so that the migration is not applied again)
type guardedMigration struct {
	guardMetaKey    string
	db              *DB
	migration       MigrationApplier
	alreadyMigrated bool
}

func newGuardedDataMigration(ctx context.Context, uniqueMigrationName string, factory migrationApplierFactory, db *DB) (Migration, error) {
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
	latestLedger, err := NewLedgerEntryReader(db).GetLatestLedgerSequence(ctx)
	if err != nil && err != ErrEmptyDB {
		err = errors.Join(err, migrationDB.Rollback())
		return nil, fmt.Errorf("failed to get latest ledger sequence: %w", err)
	}
	applier, err := factory.New(migrationDB, latestLedger)
	if err != nil {
		err = errors.Join(err, migrationDB.Rollback())
		return nil, err
	}
	guardedMigration := &guardedMigration{
		guardMetaKey:    metaKey,
		db:              migrationDB,
		migration:       applier,
		alreadyMigrated: previouslyMigrated,
	}
	return guardedMigration, nil
}

func (g *guardedMigration) Apply(ctx context.Context, meta xdr.LedgerCloseMeta) error {
	if g.alreadyMigrated {
		// This shouldn't happen since we would be out of the applicable range
		// but, just in case.
		return nil
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
		return nil
	}
	err := setMetaBool(ctx, g.db, g.guardMetaKey, true)
	if err != nil {
		return errors.Join(err, g.Rollback(ctx))
	}
	return g.db.Commit()
}

func (g *guardedMigration) Rollback(ctx context.Context) error {
	return g.db.Rollback()
}

func BuildMigrations(ctx context.Context, logger *log.Entry, db *DB, cfg *config.Config) (Migration, error) {
	var migrations []Migration

	migrationName := "TransactionsTable"
	factory := newTransactionTableMigration(
		ctx,
		logger.WithField("migration", migrationName),
		cfg.TransactionLedgerRetentionWindow,
		cfg.NetworkPassphrase,
	)

	m1, err := newGuardedDataMigration(ctx, migrationName, factory, db)
	if err != nil {
		return nil, fmt.Errorf("creating guarded transaction migration: %w", err)
	}
	migrations = append(migrations, m1)

	eventMigrationName := "EventsTable"
	eventFactory := newEventTableMigration(
		logger.WithField("migration", eventMigrationName),
		cfg.EventLedgerRetentionWindow,
		cfg.NetworkPassphrase,
	)
	m2, err := newGuardedDataMigration(ctx, eventMigrationName, eventFactory, db)
	if err != nil {
		return nil, fmt.Errorf("creating guarded transaction migration: %w", err)
	}
	migrations = append(migrations, m2)

	return multiMigration(migrations), nil
}
