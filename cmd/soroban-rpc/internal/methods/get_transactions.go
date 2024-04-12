package methods

import (
	"context"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/support/log"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/db"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/transactions"
)

type TransactionsPaginationOptions struct {
	Cursor *transactions.Cursor `json:"cursor,omitempty"`
	Limit  uint                 `json:"limit,omitempty"`
}

type GetTransactionsRequest struct {
	StartLedger uint32
	EndLedger   uint32
	Pagination  *TransactionsPaginationOptions
}

func (req GetTransactionsRequest) valid() error {
	// Validate the start and end ledger sequence
	if req.StartLedger < 0 {
		return errors.New("start ledger cannot be negative")
	}
	if req.EndLedger < req.StartLedger {
		return errors.New("end ledger cannot be less than start ledger")
	}

	return nil
}

type GetTransactionsResponse struct {
	Transactions               []transactions.Transaction `json:"transactions"`
	LatestLedger               int64                      `json:"latestLedger"`
	LatestLedgerCloseTimestamp int64                      `json:"latestLedgerCloseTimestamp"`
}

type transactionsRPCHandler struct {
	ledgerReader      db.LedgerReader
	transactionReader db.TransactionReader
	maxLimit          uint
	defaultLimit      uint
	logger            *log.Entry
}

//func (h transactionsRPCHandler) getTransactionsByLedgerSequence(ctx context.Context, request GetTransactionsRequest) (GetTransactionsResponse, error) {
//	err := request.valid()
//	if err != nil {
//		return GetTransactionsResponse{}, &jrpc2.Error{
//			Code:    jrpc2.InvalidParams,
//			Message: err.Error(),
//		}
//	}

//start := transactions.Cursor{LedgerSequence: request.StartLedger}
//end := transactions.Cursor{LedgerSequence: request.EndLedger}
//limit := h.defaultLimit
//if request.Pagination != nil {
//	if request.Pagination.Cursor != nil {
//		start = *request.Pagination.Cursor
//		// increment event index because, when paginating,
//		// we start with the item right after the cursor
//		start.TxIdx++
//	}
//	if request.Pagination.Limit > 0 {
//		limit = request.Pagination.Limit
//	}
//}
//transactions := make([]transactions.Transaction, h.defaultLimit)
//for ledgerSeq := request.StartLedger; ledgerSeq <= request.EndLedger; ledgerSeq++ {
//	ledger, found, err := h.ledgerReader.GetLedger(ctx, ledgerSeq)
//	if (err != nil) || (!found) {
//		return GetTransactionsResponse{}, &jrpc2.Error{
//			Code:    jrpc2.InternalError,
//			Message: "could not get ledger sequence",
//		}
//	}
//
//	txCount := ledger.CountTransactions()
//	for i := 0; i < txCount; i++ {
//
//	}
//
//}
//}

func NewGetTransactionsHandler(logger *log.Entry, ledgerReader db.LedgerReader, transactionReader db.TransactionReader, maxLimit, defaultLimit uint) jrpc2.Handler {
	transactionsHandler := transactionsRPCHandler{
		ledgerReader:      ledgerReader,
		transactionReader: transactionReader,
		maxLimit:          maxLimit,
		defaultLimit:      defaultLimit,
		logger:            logger,
	}

	return handler.New(func(context context.Context, request GetTransactionsRequest) (GetTransactionsResponse, error) {
		return transactionsHandler.getTransactionsByLedgerSequence(context, request)
	})
}
