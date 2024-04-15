package transactions

import (
	"github.com/stellar/go/xdr"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/ledgerbucketwindow"
)

// TransactionStore lets you ingest (write) and query (read) transactions from
// an abstract backend storage (i.e. via in-memory or sqlite).
type TransactionStore interface {
	IngestTransactions(ledgerCloseMeta xdr.LedgerCloseMeta) error
	GetLedgerRange() ledgerbucketwindow.LedgerRange
	GetTransaction(hash xdr.Hash) (Transaction, bool, ledgerbucketwindow.LedgerRange)
}
