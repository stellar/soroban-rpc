package methods

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/toid"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/db"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/ledgerbucketwindow"
)

// TransactionsPaginationOptions defines the available options for paginating through transactions.
type TransactionsPaginationOptions struct {
	Cursor string `json:"cursor,omitempty"`
	Limit  uint   `json:"limit,omitempty"`
}

// GetTransactionsRequest represents the request parameters for fetching transactions within a range of ledgers.
type GetTransactionsRequest struct {
	StartLedger uint32                         `json:"startLedger"`
	Pagination  *TransactionsPaginationOptions `json:"pagination,omitempty"`
}

// isValid checks the validity of the request parameters.
func (req GetTransactionsRequest) isValid(maxLimit uint, ledgerRange ledgerbucketwindow.LedgerRange) error {
	if req.Pagination != nil && req.Pagination.Cursor != "" {
		if req.StartLedger != 0 {
			return errors.New("startLedger and cursor cannot both be set")
		}
	} else if req.StartLedger < ledgerRange.FirstLedger.Sequence || req.StartLedger > ledgerRange.LastLedger.Sequence {
		return fmt.Errorf(
			"start ledger must be between the oldest ledger: %d and the latest ledger: %d for this rpc instance",
			ledgerRange.FirstLedger.Sequence,
			ledgerRange.LastLedger.Sequence,
		)
	}

	if req.Pagination != nil && req.Pagination.Limit > maxLimit {
		return fmt.Errorf("limit must not exceed %d", maxLimit)
	}

	return nil
}

type TransactionInfo struct {
	// Status is one of: TransactionSuccess, TransactionFailed.
	Status string `json:"status"`
	// ApplicationOrder is the index of the transaction among all the transactions
	// for that ledger.
	ApplicationOrder int32 `json:"applicationOrder"`
	// FeeBump indicates whether the transaction is a feebump transaction
	FeeBump bool `json:"feeBump"`
	// EnvelopeXdr is the TransactionEnvelope XDR value.
	EnvelopeXdr string `json:"envelopeXdr"`
	// ResultXdr is the TransactionResult XDR value.
	ResultXdr string `json:"resultXdr"`
	// ResultMetaXdr is the TransactionMeta XDR value.
	ResultMetaXdr string `json:"resultMetaXdr"`
	// DiagnosticEventsXDR is present only if transaction was not successful.
	// DiagnosticEventsXDR is a base64-encoded slice of xdr.DiagnosticEvent
	DiagnosticEventsXDR []string `json:"diagnosticEventsXdr,omitempty"`
	// Ledger is the sequence of the ledger which included the transaction.
	Ledger uint32 `json:"ledger"`
	// LedgerCloseTime is the unix timestamp of when the transaction was included in the ledger.
	LedgerCloseTime int64 `json:"createdAt"`
}

// GetTransactionsResponse encapsulates the response structure for getTransactions queries.
type GetTransactionsResponse struct {
	Transactions          []TransactionInfo `json:"transactions"`
	LatestLedger          uint32            `json:"latestLedger"`
	LatestLedgerCloseTime int64             `json:"latestLedgerCloseTimestamp"`
	OldestLedger          uint32            `json:"oldestLedger"`
	OldestLedgerCloseTime int64             `json:"oldestLedgerCloseTimestamp"`
	Cursor                string            `json:"cursor"`
}

type transactionsRPCHandler struct {
	ledgerReader      db.LedgerReader
	dbReader          db.TransactionReader
	maxLimit          uint
	defaultLimit      uint
	logger            *log.Entry
	networkPassphrase string
}

// getTransactionsByLedgerSequence fetches transactions between the start and end ledgers, inclusive of both.
// The number of ledgers returned can be tuned using the pagination options - cursor and limit.
func (h transactionsRPCHandler) getTransactionsByLedgerSequence(ctx context.Context, request GetTransactionsRequest) (GetTransactionsResponse, error) {
	ledgerRange, err := h.dbReader.GetLedgerRange(ctx)
	if err != nil {
		return GetTransactionsResponse{}, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: err.Error(),
		}
	}

	err = request.isValid(h.maxLimit, ledgerRange)
	if err != nil {
		return GetTransactionsResponse{}, &jrpc2.Error{
			Code:    jrpc2.InvalidRequest,
			Message: err.Error(),
		}
	}

	// Move start to pagination cursor
	start := toid.New(int32(request.StartLedger), 1, 1)
	limit := h.defaultLimit
	if request.Pagination != nil {
		if request.Pagination.Cursor != "" {
			cursorInt, err := strconv.ParseInt(request.Pagination.Cursor, 10, 64)
			if err != nil {
				return GetTransactionsResponse{}, &jrpc2.Error{
					Code:    jrpc2.InvalidParams,
					Message: err.Error(),
				}
			}

			*start = toid.Parse(cursorInt)
			// increment tx index because, when paginating,
			// we start with the item right after the cursor
			start.TransactionOrder++
		}
		if request.Pagination.Limit > 0 {
			limit = request.Pagination.Limit
		}
	}

	// Iterate through each ledger and its transactions until limit or end range is reached.
	// The latest ledger acts as the end ledger range for the request.
	var txns []TransactionInfo
	var cursor *toid.ID
LedgerLoop:
	for ledgerSeq := start.LedgerSequence; ledgerSeq <= int32(ledgerRange.LastLedger.Sequence); ledgerSeq++ {
		// Get ledger close meta from db
		ledger, found, err := h.ledgerReader.GetLedger(ctx, uint32(ledgerSeq))
		if err != nil {
			return GetTransactionsResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: err.Error(),
			}
		} else if !found {
			return GetTransactionsResponse{}, &jrpc2.Error{
				Code:    jrpc2.InvalidParams,
				Message: fmt.Sprintf("ledger close meta not found: %d", ledgerSeq),
			}
		}

		// Initialize tx reader.
		reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(h.networkPassphrase, ledger)
		if err != nil {
			return GetTransactionsResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: err.Error(),
			}
		}

		// Move the reader to specific tx idx
		startTxIdx := 1
		if ledgerSeq == start.LedgerSequence {
			startTxIdx = int(start.TransactionOrder)
			if ierr := reader.Seek(startTxIdx - 1); ierr != nil && ierr != io.EOF {
				return GetTransactionsResponse{}, &jrpc2.Error{
					Code:    jrpc2.InternalError,
					Message: err.Error(),
				}
			}
		}

		// Decode transaction info from ledger meta
		txCount := ledger.CountTransactions()
		for i := startTxIdx; i <= txCount; i++ {
			cursor = toid.New(int32(ledger.LedgerSequence()), int32(i), 1)

			ingestTx, err := reader.Read()
			if err != nil {
				if err == io.EOF {
					// No more transactions to read. Start from next ledger
					break
				}
				return GetTransactionsResponse{}, &jrpc2.Error{
					Code:    jrpc2.InvalidParams,
					Message: err.Error(),
				}
			}

			tx, err := db.ParseTransaction(ledger, ingestTx)
			if err != nil {
				return GetTransactionsResponse{}, &jrpc2.Error{
					Code:    jrpc2.InternalError,
					Message: err.Error(),
				}
			}

			txInfo := TransactionInfo{
				ApplicationOrder:    tx.ApplicationOrder,
				FeeBump:             tx.FeeBump,
				ResultXdr:           base64.StdEncoding.EncodeToString(tx.Result),
				ResultMetaXdr:       base64.StdEncoding.EncodeToString(tx.Meta),
				EnvelopeXdr:         base64.StdEncoding.EncodeToString(tx.Envelope),
				DiagnosticEventsXDR: base64EncodeSlice(tx.Events),
				Ledger:              tx.Ledger.Sequence,
				LedgerCloseTime:     tx.Ledger.CloseTime,
			}
			txInfo.Status = TransactionStatusFailed
			if tx.Successful {
				txInfo.Status = TransactionStatusSuccess
			}

			txns = append(txns, txInfo)
			if len(txns) >= int(limit) {
				break LedgerLoop
			}
		}
	}

	return GetTransactionsResponse{
		Transactions:          txns,
		LatestLedger:          ledgerRange.LastLedger.Sequence,
		LatestLedgerCloseTime: ledgerRange.LastLedger.CloseTime,
		OldestLedger:          ledgerRange.FirstLedger.Sequence,
		OldestLedgerCloseTime: ledgerRange.FirstLedger.CloseTime,
		Cursor:                cursor.String(),
	}, nil
}

func NewGetTransactionsHandler(logger *log.Entry, ledgerReader db.LedgerReader, dbReader db.TransactionReader, maxLimit, defaultLimit uint, networkPassphrase string) jrpc2.Handler {
	transactionsHandler := transactionsRPCHandler{
		ledgerReader:      ledgerReader,
		dbReader:          dbReader,
		maxLimit:          maxLimit,
		defaultLimit:      defaultLimit,
		logger:            logger,
		networkPassphrase: networkPassphrase,
	}

	return handler.New(func(context context.Context, request GetTransactionsRequest) (GetTransactionsResponse, error) {
		return transactionsHandler.getTransactionsByLedgerSequence(context, request)
	})
}
