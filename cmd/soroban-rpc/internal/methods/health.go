package methods

import (
	"context"
	"fmt"
	"time"

	"github.com/creachadair/jrpc2"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/db"
)

type HealthCheckResult struct {
	Status                string `json:"status"`
	LatestLedger          uint32 `json:"latestLedger"`
	OldestLedger          uint32 `json:"oldestLedger"`
	LedgerRetentionWindow uint32 `json:"ledgerRetentionWindow"`
}

// NewHealthCheck returns a health check json rpc handler
func NewHealthCheck(
	retentionWindow uint32,
	reader db.LedgerRangeGetter,
	maxHealthyLedgerLatency time.Duration,
) jrpc2.Handler {
	return NewHandler(func(ctx context.Context) (HealthCheckResult, error) {
		ledgerRange, err := reader.GetLedgerRange(ctx)
		if err != nil || ledgerRange.LastLedger.Sequence < 1 {
			extra := ""
			if err != nil {
				extra = fmt.Sprintf(": %s", err.Error())
			}
			return HealthCheckResult{}, jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "data stores are not initialized" + extra,
			}
		}

		lastKnownLedgerCloseTime := time.Unix(ledgerRange.LastLedger.CloseTime, 0)
		lastKnownLedgerLatency := time.Since(lastKnownLedgerCloseTime)
		if lastKnownLedgerLatency > maxHealthyLedgerLatency {
			roundedLatency := lastKnownLedgerLatency.Round(time.Second)
			msg := fmt.Sprintf("latency (%s) since last known ledger closed is too high (>%s)",
				roundedLatency, maxHealthyLedgerLatency)
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
