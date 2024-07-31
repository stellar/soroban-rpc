package methods

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/creachadair/jrpc2"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/db"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/xdr2json"
)

// Deprecated. Use GetLedgerEntriesRequest instead.
// TODO(https://github.com/stellar/soroban-tools/issues/374) remove after getLedgerEntries is deployed.
type GetLedgerEntryRequest struct {
	Key    string `json:"key"`
	Format string `json:"xdrFormat"`
}

// Deprecated. Use GetLedgerEntriesResponse instead.
// TODO(https://github.com/stellar/soroban-tools/issues/374) remove after getLedgerEntries is deployed.
type GetLedgerEntryResponse struct {
	EntryXDR  string          `json:"xdr"`
	EntryJSON json.RawMessage `json:"entryJson"`

	LastModifiedLedger uint32 `json:"lastModifiedLedgerSeq"`
	LatestLedger       uint32 `json:"latestLedger"`
	// The ledger sequence until the entry is live, available for entries that have associated ttl ledger entries.
	// TODO: it should had been `liveUntilLedgerSeq` :(
	LiveUntilLedgerSeq *uint32 `json:"LiveUntilLedgerSeq,omitempty"` //nolint:tagliatelle
}

// NewGetLedgerEntryHandler returns a json rpc handler to retrieve the specified ledger entry from stellar core
// Deprecated. use NewGetLedgerEntriesHandler instead.
// TODO(https://github.com/stellar/soroban-tools/issues/374) remove after getLedgerEntries is deployed.
func NewGetLedgerEntryHandler(logger *log.Entry, ledgerEntryReader db.LedgerEntryReader) jrpc2.Handler {
	return NewHandler(func(ctx context.Context, request GetLedgerEntryRequest) (GetLedgerEntryResponse, error) {
		if err := xdr2json.IsValidConversion(request.Format); err != nil {
			return GetLedgerEntryResponse{}, &jrpc2.Error{
				Code:    jrpc2.InvalidParams,
				Message: err.Error(),
			}
		}

		var key xdr.LedgerKey
		if err := xdr.SafeUnmarshalBase64(request.Key, &key); err != nil {
			logger.WithError(err).WithField("request", request).
				Info("could not unmarshal ledgerKey from getLedgerEntry request")
			return GetLedgerEntryResponse{}, &jrpc2.Error{
				Code:    jrpc2.InvalidParams,
				Message: "cannot unmarshal key value",
			}
		}

		if key.Type == xdr.LedgerEntryTypeTtl {
			return GetLedgerEntryResponse{}, &jrpc2.Error{
				Code:    jrpc2.InvalidParams,
				Message: ErrLedgerTTLEntriesCannotBeQueriedDirectly,
			}
		}

		tx, err := ledgerEntryReader.NewTx(ctx)
		if err != nil {
			return GetLedgerEntryResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "could not create read transaction",
			}
		}
		defer func() {
			_ = tx.Done()
		}()

		latestLedger, err := tx.GetLatestLedgerSequence()
		if err != nil {
			return GetLedgerEntryResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "could not get latest ledger",
			}
		}

		present, ledgerEntry, liveUntilLedgerSeq, err := db.GetLedgerEntry(tx, key)
		if err != nil {
			logger.WithError(err).WithField("request", request).
				Info("could not obtain ledger entry from storage")
			return GetLedgerEntryResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "could not obtain ledger entry from storage",
			}
		}

		if !present {
			return GetLedgerEntryResponse{}, &jrpc2.Error{
				Code:    jrpc2.InvalidRequest,
				Message: fmt.Sprintf("not found (at ledger %d)", latestLedger),
			}
		}

		response := GetLedgerEntryResponse{
			LastModifiedLedger: uint32(ledgerEntry.LastModifiedLedgerSeq),
			LatestLedger:       latestLedger,
			LiveUntilLedgerSeq: liveUntilLedgerSeq,
		}

		switch request.Format {
		case "":
			fallthrough
		case xdr2json.FormatBase64:
			if response.EntryXDR, err = xdr.MarshalBase64(ledgerEntry.Data); err != nil {
				logger.WithError(err).WithField("request", request).
					Info("could not serialize ledger entry data")
				return GetLedgerEntryResponse{}, &jrpc2.Error{
					Code:    jrpc2.InternalError,
					Message: "could not serialize ledger entry data",
				}
			}
		case xdr2json.FormatJSON:
			response.EntryJSON, err = xdr2json.ConvertInterface(ledgerEntry.Data)
			logger.WithError(err).WithField("request", request).
				Info("could not JSONify ledger entry data")
			return GetLedgerEntryResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "could not serialize ledger entry data",
			}
		}

		return response, nil
	})
}
