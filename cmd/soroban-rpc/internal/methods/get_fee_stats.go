package methods

import (
	"context"

	"github.com/creachadair/jrpc2"

	"github.com/stellar/go/support/log"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/db"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/feewindow"
)

type FeeDistribution struct {
	Max              uint64 `json:"max,string"`
	Min              uint64 `json:"min,string"`
	Mode             uint64 `json:"mode,string"`
	P10              uint64 `json:"p10,string"`
	P20              uint64 `json:"p20,string"`
	P30              uint64 `json:"p30,string"`
	P40              uint64 `json:"p40,string"`
	P50              uint64 `json:"p50,string"`
	P60              uint64 `json:"p60,string"`
	P70              uint64 `json:"p70,string"`
	P80              uint64 `json:"p80,string"`
	P90              uint64 `json:"p90,string"`
	P95              uint64 `json:"p95,string"`
	P99              uint64 `json:"p99,string"`
	TransactionCount uint32 `json:"transactionCount,string"`
	LedgerCount      uint32 `json:"ledgerCount"`
}

func convertFeeDistribution(distribution feewindow.FeeDistribution) FeeDistribution {
	return FeeDistribution{
		Max:              distribution.Max,
		Min:              distribution.Min,
		Mode:             distribution.Mode,
		P10:              distribution.P10,
		P20:              distribution.P20,
		P30:              distribution.P30,
		P40:              distribution.P40,
		P50:              distribution.P50,
		P60:              distribution.P60,
		P70:              distribution.P70,
		P80:              distribution.P80,
		P90:              distribution.P90,
		P95:              distribution.P95,
		P99:              distribution.P99,
		TransactionCount: distribution.FeeCount,
		LedgerCount:      distribution.LedgerCount,
	}
}

type GetFeeStatsResult struct {
	SorobanInclusionFee FeeDistribution `json:"sorobanInclusionFee"`
	InclusionFee        FeeDistribution `json:"inclusionFee"`
	LatestLedger        uint32          `json:"latestLedger"`
}

// NewGetFeeStatsHandler returns a handler obtaining fee statistics
func NewGetFeeStatsHandler(windows *feewindow.FeeWindows, reader db.TransactionReader, logger *log.Entry) jrpc2.Handler {
	return NewHandler(func(ctx context.Context) (GetFeeStatsResult, error) {
		ledgerInfo, err := reader.GetLedgerRange(ctx)
		if err != nil { // still not fatal
			logger.WithError(err).
				Error("could not fetch ledger range")
		}

		result := GetFeeStatsResult{
			SorobanInclusionFee: convertFeeDistribution(windows.SorobanInclusionFeeWindow.GetFeeDistribution()),
			InclusionFee:        convertFeeDistribution(windows.ClassicFeeWindow.GetFeeDistribution()),
			LatestLedger:        ledgerInfo.LastLedger.Sequence,
		}
		return result, nil
	})
}
