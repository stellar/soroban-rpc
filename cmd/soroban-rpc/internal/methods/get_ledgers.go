package methods

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/creachadair/jrpc2"
	"github.com/stellar/go/xdr"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/db"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/ledgerbucketwindow"
)

// GetLedgersRequest represents the request parameters for fetching ledgers.
type GetLedgersRequest struct {
	StartLedger uint32                         `json:"startLedger"`
	Pagination  *TransactionsPaginationOptions `json:"pagination,omitempty"`
	Format      string                         `json:"xdrFormat,omitempty"`
}

// validate checks the validity of the request parameters.
func (req *GetLedgersRequest) validate(maxLimit uint, ledgerRange ledgerbucketwindow.LedgerRange) error {
	switch {
	case req.Pagination != nil && req.Pagination.Cursor != "":
		if req.StartLedger != 0 {
			return errors.New("startLedger and cursor cannot both be set")
		}
	case req.StartLedger < ledgerRange.FirstLedger.Sequence || req.StartLedger > ledgerRange.LastLedger.Sequence:
		return fmt.Errorf(
			"start ledger must be between the oldest ledger: %d and the latest ledger: %d for this rpc instance",
			ledgerRange.FirstLedger.Sequence,
			ledgerRange.LastLedger.Sequence,
		)
	}

	if req.Pagination != nil && req.Pagination.Limit > maxLimit {
		return fmt.Errorf("limit must not exceed %d", maxLimit)
	}

	return IsValidFormat(req.Format)
}

// LedgerResponse represents a single ledger in the response.
type LedgerResponse struct {
	Hash            string `json:"hash"`
	Sequence        uint32 `json:"sequence"`
	LedgerCloseTime int64  `json:"ledgerCloseTime"`

	LedgerHeader     string          `json:"headerXdr"`
	LedgerHeaderJSON json.RawMessage `json:"headerJson,omitempty"`

	LedgerCloseMeta     string          `json:"closeMetaXdr"`
	LedgerCloseMetaJSON json.RawMessage `json:"closeMetaJson,omitempty"`
}

// GetLedgersResponse encapsulates the response structure for getLedgers queries.
type GetLedgersResponse struct {
	Ledgers               []LedgerResponse `json:"ledgers"`
	LatestLedger          uint32           `json:"latestLedger"`
	LatestLedgerCloseTime int64            `json:"latestLedgerCloseTime"`
	OldestLedger          uint32           `json:"oldestLedger"`
	OldestLedgerCloseTime int64            `json:"oldestLedgerCloseTime"`
	Cursor                string           `json:"cursor"`
}

type ledgersHandler struct {
	ledgerReader db.LedgerReader
	maxLimit     uint
	defaultLimit uint
}

// NewGetLedgersHandler returns a jrpc2.Handler for the getLedgers method.
func NewGetLedgersHandler(ledgerReader db.LedgerReader, maxLimit, defaultLimit uint) jrpc2.Handler {
	return NewHandler((&ledgersHandler{
		ledgerReader: ledgerReader,
		maxLimit:     maxLimit,
		defaultLimit: defaultLimit,
	}).getLedgers)
}

// getLedgers fetch ledgers and relevant metadata from DB.
func (h ledgersHandler) getLedgers(ctx context.Context, request GetLedgersRequest) (GetLedgersResponse, error) {
	ledgerRange, err := h.ledgerReader.GetLedgerRange(ctx)
	if err != nil {
		return GetLedgersResponse{}, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: err.Error(),
		}
	}

	if err := request.validate(h.maxLimit, ledgerRange); err != nil {
		return GetLedgersResponse{}, &jrpc2.Error{
			Code:    jrpc2.InvalidRequest,
			Message: err.Error(),
		}
	}

	start, limit, err := h.initializePagination(request)
	if err != nil {
		return GetLedgersResponse{}, &jrpc2.Error{
			Code:    jrpc2.InvalidParams,
			Message: err.Error(),
		}
	}

	ledgers, err := h.fetchLedgers(ctx, start, limit, request.Format)
	if err != nil {
		return GetLedgersResponse{}, err
	}
	cursor := strconv.FormatUint(uint64(ledgers[len(ledgers)-1].Sequence), 10)

	return GetLedgersResponse{
		Ledgers:               ledgers,
		LatestLedger:          ledgerRange.LastLedger.Sequence,
		LatestLedgerCloseTime: ledgerRange.LastLedger.CloseTime,
		OldestLedger:          ledgerRange.FirstLedger.Sequence,
		OldestLedgerCloseTime: ledgerRange.FirstLedger.CloseTime,
		Cursor:                cursor,
	}, nil
}

// initializePagination parses the request pagination details initializes the cursor.
func (h ledgersHandler) initializePagination(request GetLedgersRequest) (uint32, uint, error) {
	start := request.StartLedger
	limit := h.defaultLimit
	if request.Pagination != nil {
		if request.Pagination.Cursor != "" {
			cursorInt, err := strconv.ParseUint(request.Pagination.Cursor, 10, 32)
			if err != nil {
				return 0, 0, err
			}
			start = uint32(cursorInt) + 1
		}
		if request.Pagination.Limit > 0 {
			limit = request.Pagination.Limit
		}
	}
	return start, limit, nil
}

// fetchLedgers fetches ledgers from the DB beginning with the start request ledger until the max request limit.
func (h ledgersHandler) fetchLedgers(ctx context.Context, start uint32,
	limit uint, format string,
) ([]LedgerResponse, error) {
	ledgers := make([]LedgerResponse, 0, limit)
	for ledgerSeq := start; uint(len(ledgers)) < limit; ledgerSeq++ {
		ledger, found, err := h.ledgerReader.GetLedger(ctx, ledgerSeq)
		if err != nil {
			return nil, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: err.Error(),
			}
		}
		if !found {
			return nil, &jrpc2.Error{
				Code:    jrpc2.InvalidParams,
				Message: fmt.Sprintf("database does not contain metadata for ledger: %d", ledgerSeq),
			}
		}

		ledgerResponse, err := h.getMetaAndHeaderInfo(ledger, format)
		if err != nil {
			return nil, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: err.Error(),
			}
		}
		ledgers = append(ledgers, ledgerResponse)
	}
	return ledgers, nil
}

// getMetaAndHeaderInfo extracts and formats the ledger metadata and header information.
func (h ledgersHandler) getMetaAndHeaderInfo(ledger xdr.LedgerCloseMeta, format string) (LedgerResponse, error) {
	ledgerResponse := LedgerResponse{
		Hash:            ledger.LedgerHash().HexString(),
		Sequence:        ledger.LedgerSequence(),
		LedgerCloseTime: ledger.LedgerCloseTime(),
	}

	closeMetaB, err := ledger.MarshalBinary()
	if err != nil {
		return LedgerResponse{}, fmt.Errorf("error marshaling ledger close meta: %w", err)
	}

	headerB, err := ledger.LedgerHeaderHistoryEntry().MarshalBinary()
	if err != nil {
		return LedgerResponse{}, fmt.Errorf("error marshaling ledger header: %w", err)
	}

	// Format the data according to the requested format (JSON or XDR)
	switch format {
	case FormatJSON:
		closeMetaJSON, headerJSON, convErr := ledgerToJSON(closeMetaB, headerB)
		if convErr != nil {
			return LedgerResponse{}, fmt.Errorf("could not convert ledger metadata and "+
				"header to JSON: %v", convErr)
		}
		ledgerResponse.LedgerCloseMetaJSON = closeMetaJSON
		ledgerResponse.LedgerHeaderJSON = headerJSON
	default:
		ledgerResponse.LedgerCloseMeta = base64.StdEncoding.EncodeToString(closeMetaB)
		ledgerResponse.LedgerHeader = base64.StdEncoding.EncodeToString(headerB)
	}
	return ledgerResponse, nil
}
