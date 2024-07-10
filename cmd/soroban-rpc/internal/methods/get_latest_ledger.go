package methods

import (
	"context"
	"fmt"

	"github.com/creachadair/jrpc2"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/db"
)

type GetLatestLedgerResponse struct {
	// Hash of the latest ledger as a hex-encoded string
	Hash string `json:"id"`
	// Stellar Core protocol version associated with the ledger.
	ProtocolVersion uint32 `json:"protocolVersion"`
	// Sequence number of the latest ledger.
	Sequence uint32 `json:"sequence"`
}

// NewGetLatestLedgerHandler returns a JSON RPC handler to retrieve the latest ledger entry from Stellar core.
func NewGetLatestLedgerHandler(ledgerReader db.LedgerReader) jrpc2.Handler {
	return NewHandler(func(ctx context.Context) (GetLatestLedgerResponse, error) {
		ledgerRange, err := ledgerReader.GetLedgerRange(ctx)
		if err != nil {
			return GetLatestLedgerResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: fmt.Errorf("could not get ledger range: %w", err).Error(),
			}
		}
		latestLedger := ledgerRange.LastLedger

		response := GetLatestLedgerResponse{
			Hash:            latestLedger.Hash.HexString(),
			ProtocolVersion: latestLedger.ProtocolVersion,
			Sequence:        latestLedger.Sequence,
		}
		return response, nil
	})
}
