package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/config"
)

type MigrationApplier interface {
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

type multiMigration []Migration

func (m multiMigration) Apply(ctx context.Context, meta xdr.LedgerCloseMeta) error {
	var err error
	for _, data := range m {
		if localErr := data.Apply(ctx, meta); localErr != nil {
			err = errors.Join(err, localErr)
		}
	}
	return err
}

func (m multiMigration) Commit(ctx context.Context) error {
	var err error
	for _, data := range m {
		if localErr := data.Commit(ctx); localErr != nil {
			err = errors.Join(err, localErr)
		}
	}
	return err
}

func (m multiMigration) Rollback(ctx context.Context) error {
	var err error
	for _, data := range m {
		if localErr := data.Rollback(ctx); localErr != nil {
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
	migrationDB := db.Clone()
	if err := migrationDB.Begin(ctx); err != nil {
		return nil, err
	}
	metaKey := "Migration" + uniqueMigrationName + "Done"
	previouslyMigrated, err := getMetaBool(ctx, migrationDB.SessionInterface, metaKey)
	if err != nil && !errors.Is(err, ErrEmptyDB) {
		migrationDB.Rollback()
		return nil, err
	}
	applier, err := factory.New(migrationDB)
	if err != nil {
		migrationDB.Rollback()
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
		return nil
	}
	return g.migration.Apply(ctx, meta)
}

func (g *guardedMigration) Commit(ctx context.Context) error {
	if g.alreadyMigrated {
		return nil
	}
	err := setMetaBool(ctx, g.db.SessionInterface, g.guardMetaKey)
	if err != nil {
		return errors.Join(err, g.Rollback(ctx))
	}
	return g.db.Commit()
}

func (g *guardedMigration) Rollback(ctx context.Context) error {
	return g.db.Rollback()
}

func BuildMigrations(ctx context.Context, logger *log.Entry, db *DB, cfg *config.Config) (Migration, error) {
	migrationName := "TransactionsTable"
	factory := newTransactionTableMigration(ctx, logger.WithField("migration", migrationName), cfg.TransactionLedgerRetentionWindow, cfg.NetworkPassphrase)
	m, err := newGuardedDataMigration(ctx, migrationName, factory, db)
	if err != nil {
		return nil, fmt.Errorf("creating guarded transaction migration: %w", err)
	}
	// Add other migrations here
	return multiMigration{m}, nil
}
