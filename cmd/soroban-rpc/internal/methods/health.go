package methods

import (
	"context"
	"fmt"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/ledgerbucketwindow"
)

type HealthCheckResult struct {
	Status                string `json:"status"`
	LatestLedger          uint32 `json:"latestLedger"`
	OldestLedger          uint32 `json:"oldestLedger"`
	LedgerRetentionWindow uint32 `json:"ledgerRetentionWindow"`
}

type LedgerRangeGetter interface {
	GetLedgerRange() ledgerbucketwindow.LedgerRange
}

// NewHealthCheck returns a health check json rpc handler
func NewHealthCheck(retentionWindow uint32, ledgerRangeGetter LedgerRangeGetter, maxHealthyLedgerLatency time.Duration) jrpc2.Handler {
	return handler.New(func(ctx context.Context) (HealthCheckResult, error) {
		ledgerRange := ledgerRangeGetter.GetLedgerRange()
		if ledgerRange.LastLedger.Sequence < 1 {
			return HealthCheckResult{}, jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "data stores are not initialized",
			}
		}

		lastKnownLedgerCloseTime := time.Unix(ledgerRange.LastLedger.CloseTime, 0)
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
			Status:                "healthy",
			LatestLedger:          ledgerRange.LastLedger.Sequence,
			OldestLedger:          ledgerRange.FirstLedger.Sequence,
			LedgerRetentionWindow: retentionWindow,
		}
		return result, nil
	})
}
