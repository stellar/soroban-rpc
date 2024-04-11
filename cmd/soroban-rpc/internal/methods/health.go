package methods

import (
	"context"
	"fmt"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/events"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/transactions"
)

type HealthCheckResult struct {
	Status       string `json:"status"`
	LatestLedger uint32 `json:"latestLedger"`
	FirstLedger  uint32 `json:"firstLedger"`
}

// NewHealthCheck returns a health check json rpc handler
func NewHealthCheck(txStore *transactions.MemoryStore, evStore *events.MemoryStore, maxHealthyLedgerLatency time.Duration) jrpc2.Handler {
	return handler.New(func(ctx context.Context) (HealthCheckResult, error) {
		txLedgerRange := txStore.GetLedgerRange()
		evLedgerRange := evStore.GetLedgerRange()
		if txLedgerRange.FirstLedger.Sequence < 1 || evLedgerRange.FirstLedger.Sequence < 1 {
			return HealthCheckResult{}, jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "data stores are not initialized",
			}
		}
		mergedRange := evLedgerRange
		if txLedgerRange.FirstLedger.Sequence < mergedRange.FirstLedger.Sequence {
			mergedRange.FirstLedger = txLedgerRange.FirstLedger
		}
		if txLedgerRange.LastLedger.Sequence > mergedRange.LastLedger.Sequence {
			mergedRange.LastLedger = txLedgerRange.LastLedger
		}

		lastKnownLedgerCloseTime := time.Unix(mergedRange.LastLedger.CloseTime, 0)
		lastKnownLedgerLatency := time.Since(lastKnownLedgerCloseTime)
		if lastKnownLedgerLatency > maxHealthyLedgerLatency {
			roundedLatency := lastKnownLedgerLatency.Round(time.Second)
			msg := fmt.Sprintf("latency (%s) since last known ledger closed is too high (>%s)", roundedLatency, maxHealthyLedgerLatency)
			return HealthCheckResult{}, jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: msg,
			}
		}
		result := HealthCheckResult{
			Status:       "healthy",
			LatestLedger: mergedRange.LastLedger.Sequence,
			FirstLedger:  mergedRange.FirstLedger.Sequence,
		}
		return result, nil
	})
}
