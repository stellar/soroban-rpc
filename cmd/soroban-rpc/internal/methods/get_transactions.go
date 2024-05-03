package methods

import (
	"context"
	"fmt"
	"io"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/toid"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/db"
)

// TransactionsPaginationOptions defines the available options for paginating through transactions.
type TransactionsPaginationOptions struct {
	Cursor *toid.ID `json:"cursor,omitempty"`
	Limit  uint     `json:"limit,omitempty"`
}

// GetTransactionsRequest represents the request parameters for fetching transactions within a range of ledgers.
type GetTransactionsRequest struct {
	StartLedger uint32                         `json:"startLedger"`
	Pagination  *TransactionsPaginationOptions `json:"pagination,omitempty"`
}

// isValid checks the validity of the request parameters.
// It returns an error if any parameter is out of the expected range or combination.
func (req GetTransactionsRequest) isValid(maxLimit uint) error {
	// Validate pagination and start ledger sequence
	if req.Pagination != nil && req.Pagination.Cursor != nil {
		if req.StartLedger != 0 {
			return errors.New("startLedger and cursor cannot both be set")
		}
	} else if req.StartLedger < 1 {
		return errors.New("start ledger cannot be negative")
	}
	if req.Pagination != nil && req.Pagination.Limit > maxLimit {
		return fmt.Errorf("limit must not exceed %d", maxLimit)
	}

	return nil
}

// transactionInfo represents the decoded transaction information from the ledger close meta.
type transactionInfo struct {
	Result           []byte `json:"result"`
	Meta             []byte `json:"meta"`
	Envelope         []byte `json:"envelope"`
	FeeBump          bool   `json:"feeBump"`
	ApplicationOrder int32  `json:"applicationOrder"`
	Successful       bool   `json:"successful"`
	LedgerSequence   uint32 `json:"ledgerSequence"`
}

// GetTransactionsResponse encapsulates the response structure for getTransactions queries.
type GetTransactionsResponse struct {
	Transactions          []transactionInfo `json:"transactions"`
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
	err := request.isValid(h.maxLimit)
	if err != nil {
		return GetTransactionsResponse{}, &jrpc2.Error{
			Code:    jrpc2.InvalidParams,
			Message: err.Error(),
		}
	}

	// Move start to pagination cursor
	start := toid.New(int32(request.StartLedger), 0, 0)
	limit := h.defaultLimit
	if request.Pagination != nil {
		if request.Pagination.Cursor != nil {
			start = request.Pagination.Cursor
			// increment tx index because, when paginating,
			// we start with the item right after the cursor
			start.TransactionOrder++
		}
		if request.Pagination.Limit > 0 {
			limit = request.Pagination.Limit
		}
	}

	ledgerRange, err := h.dbReader.GetLedgerRange(ctx)
	if err != nil {
		return GetTransactionsResponse{}, &jrpc2.Error{
			Code:    jrpc2.InvalidParams,
			Message: err.Error(),
		}
	}

	// Iterate through each ledger and its transactions until limit or end range is reached.
	// The latest ledger acts as the end ledger range for the request.
	var txns []transactionInfo
	var cursor *toid.ID
LedgerLoop:
	for ledgerSeq := start.LedgerSequence; ledgerSeq <= int32(ledgerRange.LastLedger.Sequence); ledgerSeq++ {
		// Get ledger close meta from db
		ledger, found, err := h.ledgerReader.GetLedger(ctx, uint32(ledgerSeq))
		if (err != nil) || (!found) {
			if err == nil {
				err = errors.Errorf("ledger close meta not found: %d", ledgerSeq)
			}
			return GetTransactionsResponse{}, &jrpc2.Error{
				Code:    jrpc2.InvalidParams,
				Message: err.Error(),
			}
		}

		// Initialise tx reader.
		reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(h.networkPassphrase, ledger)
		if err != nil {
			return GetTransactionsResponse{}, &jrpc2.Error{
				Code:    jrpc2.InvalidParams,
				Message: err.Error(),
			}
		}

		// Move the reader to specific tx idx
		startTxIdx := 0
		if ledgerSeq == start.LedgerSequence {
			startTxIdx = int(start.TransactionOrder)
			if ierr := reader.Seek(startTxIdx); ierr != nil && ierr != io.EOF {
				return GetTransactionsResponse{}, &jrpc2.Error{
					Code:    jrpc2.InvalidParams,
					Message: err.Error(),
				}
			}
		}

		// Decode transaction info from ledger meta
		txCount := ledger.CountTransactions()
		for i := startTxIdx; i < txCount; i++ {
			cursor = toid.New(int32(ledger.LedgerSequence()), int32(i), 0)

			tx, err := reader.Read()
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

			txResult, txMeta, txEnvelope, err := h.getTransactionDetails(tx)
			if err != nil {
				return GetTransactionsResponse{}, &jrpc2.Error{
					Code:    jrpc2.InvalidParams,
					Message: err.Error(),
				}
			}

			txInfo := transactionInfo{
				Result:           txResult,
				Meta:             txMeta,
				Envelope:         txEnvelope,
				FeeBump:          tx.Envelope.IsFeeBump(),
				ApplicationOrder: int32(tx.Index),
				Successful:       tx.Result.Result.Successful(),
				LedgerSequence:   ledger.LedgerSequence(),
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

// getTransactionDetails fetches XDR for the following transaction details - result, meta and envelope.
func (h transactionsRPCHandler) getTransactionDetails(tx ingest.LedgerTransaction) ([]byte, []byte, []byte, error) {
	txResult, err := tx.Result.Result.MarshalBinary()
	if err != nil {
		return nil, nil, nil, err
	}
	txMeta, err := tx.UnsafeMeta.MarshalBinary()
	if err != nil {
		return nil, nil, nil, err
	}
	txEnvelope, err := tx.Envelope.MarshalBinary()
	if err != nil {
		return nil, nil, nil, err
	}

	return txResult, txMeta, txEnvelope, nil
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
