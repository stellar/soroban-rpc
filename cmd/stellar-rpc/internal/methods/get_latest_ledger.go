package methods

import (
	"context"

	"github.com/creachadair/jrpc2"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
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
func NewGetLatestLedgerHandler(ledgerEntryReader db.LedgerEntryReader, ledgerReader db.LedgerReader) jrpc2.Handler {
	return NewHandler(func(ctx context.Context) (GetLatestLedgerResponse, error) {
		latestSequence, err := ledgerEntryReader.GetLatestLedgerSequence(ctx)
		if err != nil {
			return GetLatestLedgerResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "could not get latest ledger sequence",
			}
		}

		latestLedger, found, err := ledgerReader.GetLedger(ctx, latestSequence)
		if (err != nil) || (!found) {
			return GetLatestLedgerResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "could not get latest ledger",
			}
		}

		response := GetLatestLedgerResponse{
			Hash:            latestLedger.LedgerHash().HexString(),
			ProtocolVersion: latestLedger.ProtocolVersion(),
			Sequence:        latestSequence,
		}
		return response, nil
	})
}
