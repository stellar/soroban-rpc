package methods

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/creachadair/jrpc2"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
)

type GetLatestLedgerRequest struct {
	Format string `json:"xdrFormat,omitempty"`
}

type GetLatestLedgerResponse struct {
	// Stellar Core protocol version associated with the ledger.
	ProtocolVersion uint32 `json:"protocolVersion"`
	LedgerInfo
}

type latestLedgerHandler struct {
	ledgerEntryReader db.LedgerEntryReader
	ledgerReader      db.LedgerReader
}

func (h latestLedgerHandler) getLatestLedger(ctx context.Context, request GetLatestLedgerRequest) (GetLatestLedgerResponse, error) {
	latestSequence, err := h.ledgerEntryReader.GetLatestLedgerSequence(ctx)
	if err != nil {
		return GetLatestLedgerResponse{}, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: "could not get latest ledger sequence",
		}
	}

	latestLedger, found, err := h.ledgerReader.GetLedger(ctx, latestSequence)
	if (err != nil) || (!found) {
		return GetLatestLedgerResponse{}, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: "could not get latest ledger",
		}
	}

	response := GetLatestLedgerResponse{
		ProtocolVersion: latestLedger.ProtocolVersion(),
	}
	response.Hash = latestLedger.LedgerHash().HexString()
	response.Sequence = latestSequence
	response.LedgerCloseTime = latestLedger.LedgerCloseTime()

	// Format the data according to the requested format (JSON or XDR)
	switch request.Format {
	case FormatJSON:
		var convErr error
		response.LedgerMetadataJSON, response.LedgerHeaderJSON, convErr = ledgerToJSON(&latestLedger)
		if convErr != nil {
			return GetLatestLedgerResponse{}, convErr
		}
	default:
		closeMetaB, err := latestLedger.MarshalBinary()
		if err != nil {
			return GetLatestLedgerResponse{}, fmt.Errorf("error marshaling ledger close meta: %w", err)
		}

		headerB, err := latestLedger.LedgerHeaderHistoryEntry().MarshalBinary()
		if err != nil {
			return GetLatestLedgerResponse{}, fmt.Errorf("error marshaling ledger header: %w", err)
		}

		response.LedgerMetadata = base64.StdEncoding.EncodeToString(closeMetaB)
		response.LedgerHeader = base64.StdEncoding.EncodeToString(headerB)
	}

	return response, nil
}

// NewGetLatestLedgerHandler returns a JSON RPC handler to retrieve the latest ledger entry from Stellar core.
func NewGetLatestLedgerHandler(ledgerEntryReader db.LedgerEntryReader, ledgerReader db.LedgerReader) jrpc2.Handler {
	return NewHandler((&latestLedgerHandler{
		ledgerEntryReader: ledgerEntryReader,
		ledgerReader:      ledgerReader,
	}).getLatestLedger)
}
