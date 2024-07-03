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
	var protocolVersion uint32
	readTx, err := ledgerEntryReader.NewCachedTx(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = readTx.Done()
	}()

	latestLedger, err := readTx.GetLatestLedgerSequence()
	if err != nil {
		return 0, err
	}

	_, protocolVersion, err = getBucketListSizeAndProtocolVersion(ctx, ledgerReader, latestLedger)
	if err != nil {
		return 0, err
	}
	return protocolVersion, nil
}

func getBucketListSizeAndProtocolVersion(
	ctx context.Context,
	ledgerReader db.LedgerReader,
	latestLedger uint32,
) (uint64, uint32, error) {
	// obtain bucket size
	closeMeta, ok, err := ledgerReader.GetLedger(ctx, latestLedger)
	if err != nil {
		return 0, 0, err
	}
	if !ok {
		return 0, 0, fmt.Errorf("missing meta for latest ledger (%d)", latestLedger)
	}
	if closeMeta.V != 1 {
		return 0, 0, fmt.Errorf("latest ledger (%d) meta has unexpected verion (%d)", latestLedger, closeMeta.V)
	}
	return uint64(closeMeta.V1.TotalByteSizeOfBucketList), uint32(closeMeta.V1.LedgerHeader.Header.LedgerVersion), nil
}
