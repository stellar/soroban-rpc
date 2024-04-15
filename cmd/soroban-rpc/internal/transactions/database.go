package transactions

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go/xdr"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/daemon/interfaces"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/ledgerbucketwindow"
)

// DatabaseStore is an on-disk (sqlite) store of Stellar transactions.
type DatabaseStore struct {
	// passphrase is an immutable string containing the Stellar network
	// passphrase and accessing it does not need to be protected by the lock
	passphrase string

	lock                      sync.RWMutex
	transactions              map[xdr.Hash]transaction
	transactionsByLedger      *ledgerbucketwindow.LedgerBucketWindow[[]xdr.Hash]
	transactionDurationMetric *prometheus.SummaryVec
	transactionCountMetric    prometheus.Summary
}

func NewDatabaseStore(daemon interfaces.Daemon, networkPassphrase string, retentionWindow uint32) TransactionStore {
	return NewMemoryStore(daemon, networkPassphrase, retentionWindow)
}

// func (m *DatabaseStore) IngestTransactions(ledgerCloseMeta xdr.LedgerCloseMeta) error {
// 	// startTime := time.Now()
// 	return nil
// }

// // GetLedgerRange returns the first and latest ledger available in the store.
// func (m *DatabaseStore) GetLedgerRange() ledgerbucketwindow.LedgerRange {
// 	m.lock.RLock()
// 	defer m.lock.RUnlock()
// 	return m.transactionsByLedger.GetLedgerRange()
// }

// // GetTransaction obtains a transaction from the store and whether it's present and the current store range
// func (m *DatabaseStore) GetTransaction(hash xdr.Hash) (Transaction, bool, ledgerbucketwindow.LedgerRange) {
// 	return Transaction{}, false, ledgerbucketwindow.LedgerRange{}
// }
