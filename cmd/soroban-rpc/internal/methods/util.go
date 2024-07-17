package methods

import (
	"context"
	"fmt"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/db"
)

func getProtocolVersion(
	ctx context.Context,
	ledgerEntryReader db.LedgerEntryReader,
	ledgerReader db.LedgerReader,
) (uint32, error) {
	latestLedger, err := ledgerEntryReader.GetLatestLedgerSequence(ctx)
	if err != nil {
		return 0, err
	}

	// obtain bucket size
	closeMeta, ok, err := ledgerReader.GetLedger(ctx, latestLedger)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("missing meta for latest ledger (%d)", latestLedger)
	}
	if closeMeta.V != 1 {
		return 0, fmt.Errorf("latest ledger (%d) meta has unexpected verion (%d)", latestLedger, closeMeta.V)
	}
	return uint32(closeMeta.V1.LedgerHeader.Header.LedgerVersion), nil
}
