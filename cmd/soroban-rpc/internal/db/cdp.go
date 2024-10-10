package db

import (
	"context"

	"github.com/stellar/go/support/datastore"
)

func MakeDatastore(ctx context.Context) (datastore.DataStore, error) {
	return datastore.NewGCSDataStore(
		ctx,
		"sdf-ledger-close-meta/ledgers/pubnet/",
		datastore.DataStoreSchema{
			LedgersPerFile:    1,
			FilesPerPartition: 64000,
		})
}
