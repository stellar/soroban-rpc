package methods

import (
	"context"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
	"github.com/stellar/go/ingest"
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
	StartLedger uint32                         `json:"startLedger"`
	EndLedger   uint32                         `json:"endLedger"`
	Pagination  *TransactionsPaginationOptions `json:"pagination,omitempty"`
}

func (req GetTransactionsRequest) isValid() error {
	// Validate the start and end ledger sequence
	if req.StartLedger < 0 {
		return errors.New("start ledger cannot be negative")
	}
	if req.EndLedger < req.StartLedger {
		return errors.New("end ledger cannot be less than start ledger")
	}

	return nil
}

type TransactionInfo struct {
	Result           []byte `json:"result"`
	Meta             []byte `json:"meta"`
	Envelope         []byte `json:"envelope"`
	FeeBump          bool   `json:"feeBump"`
	ApplicationOrder int32  `json:"applicationOrder"`
	Successful       bool   `json:"successful"`
	LedgerSequence   uint   `json:"ledgerSequence"`
}

type GetTransactionsResponse struct {
	Transactions               []TransactionInfo              `json:"transactions"`
	LatestLedger               int64                          `json:"latestLedger"`
	LatestLedgerCloseTimestamp int64                          `json:"latestLedgerCloseTimestamp"`
	Pagination                 *TransactionsPaginationOptions `json:"pagination"`
}

type transactionsRPCHandler struct {
	ledgerReader      db.LedgerReader
	ledgerEntryReader db.LedgerEntryReader
	maxLimit          uint
	defaultLimit      uint
	logger            *log.Entry
	networkPassphrase string
}

func (h transactionsRPCHandler) getTransactionsByLedgerSequence(ctx context.Context, request GetTransactionsRequest) (GetTransactionsResponse, error) {
	err := request.isValid()
	if err != nil {
		return GetTransactionsResponse{}, &jrpc2.Error{
			Code:    jrpc2.InvalidParams,
			Message: err.Error(),
		}
	}

	// Move start to pagination cursor
	start := transactions.NewCursor(request.StartLedger, 0, 0)
	limit := h.defaultLimit
	if request.Pagination != nil {
		if request.Pagination.Cursor != nil {
			start = *request.Pagination.Cursor
			// increment tx index because, when paginating,
			// we start with the item right after the cursor
			start.TxIdx++
		}
		if request.Pagination.Limit > 0 {
			limit = request.Pagination.Limit
		}
	}

	// Iterate through each ledger and its transactions until limit or end range is reached
	var txns []TransactionInfo
	var cursor transactions.Cursor
LedgerLoop:
	for ledgerSeq := start.LedgerSequence; ledgerSeq <= request.EndLedger; ledgerSeq++ {
		// Get ledger close meta from db
		ledger, found, err := h.ledgerReader.GetLedger(ctx, ledgerSeq)
		if (err != nil) || (!found) {
			if err == nil {
				err = errors.New("ledger close meta not found")
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
			startTxIdx = int(start.TxIdx)
		}
		err = reader.Seek(startTxIdx)
		if err != nil {
			// Seek returns EOF so we can move onto next ledger
			continue
		}

		// Decode transaction info from ledger meta
		txCount := ledger.CountTransactions()
		for i := startTxIdx; i < txCount; i++ {
			cursor = transactions.NewCursor(ledger.LedgerSequence(), uint32(i), 0)

			tx, err := reader.Read()
			if err != nil {
				return GetTransactionsResponse{}, &jrpc2.Error{
					Code:    jrpc2.InvalidParams,
					Message: err.Error(),
				}
			}

			txInfo := TransactionInfo{
				FeeBump:          tx.Envelope.IsFeeBump(),
				ApplicationOrder: int32(tx.Index),
				Successful:       tx.Result.Result.Successful(),
				LedgerSequence:   uint(ledger.LedgerSequence()),
			}
			if txInfo.Result, err = tx.Result.Result.MarshalBinary(); err != nil {
				return GetTransactionsResponse{}, &jrpc2.Error{
					Code:    jrpc2.InvalidParams,
					Message: err.Error(),
				}
			}
			if txInfo.Meta, err = tx.UnsafeMeta.MarshalBinary(); err != nil {
				return GetTransactionsResponse{}, &jrpc2.Error{
					Code:    jrpc2.InvalidParams,
					Message: err.Error(),
				}
			}
			if txInfo.Envelope, err = tx.Envelope.MarshalBinary(); err != nil {
				return GetTransactionsResponse{}, &jrpc2.Error{
					Code:    jrpc2.InvalidParams,
					Message: err.Error(),
				}
			}

			txns = append(txns, txInfo)
			if len(txns) >= int(limit) {
				break LedgerLoop
			}
		}
	}

	tx, err := h.ledgerEntryReader.NewTx(ctx)
	if err != nil {
		return GetTransactionsResponse{}, &jrpc2.Error{
			Code:    jrpc2.InvalidParams,
			Message: err.Error(),
		}
	}
	defer func() {
		_ = tx.Done()
	}()

	latestSequence, err := tx.GetLatestLedgerSequence()
	if err != nil {
		return GetTransactionsResponse{}, &jrpc2.Error{
			Code:    jrpc2.InvalidParams,
			Message: err.Error(),
		}
	}

	latestLedger, found, err := h.ledgerReader.GetLedger(ctx, latestSequence)
	if (err != nil) || (!found) {
		if err == nil {
			err = errors.New("ledger close meta not found")
		}
		return GetTransactionsResponse{}, &jrpc2.Error{
			Code:    jrpc2.InvalidParams,
			Message: err.Error(),
		}
	}

	return GetTransactionsResponse{
		Transactions:               txns,
		LatestLedger:               int64(latestLedger.LedgerSequence()),
		LatestLedgerCloseTimestamp: int64(latestLedger.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime),
		Pagination: &TransactionsPaginationOptions{
			Cursor: &cursor,
			Limit:  limit,
		},
	}, nil

}

func NewGetTransactionsHandler(logger *log.Entry, ledgerReader db.LedgerReader, ledgerEntryReader db.LedgerEntryReader, maxLimit, defaultLimit uint, networkPassphrase string) jrpc2.Handler {
	transactionsHandler := transactionsRPCHandler{
		ledgerReader:      ledgerReader,
		ledgerEntryReader: ledgerEntryReader,
		maxLimit:          maxLimit,
		defaultLimit:      defaultLimit,
		logger:            logger,
		networkPassphrase: networkPassphrase,
	}

	return handler.New(func(context context.Context, request GetTransactionsRequest) (GetTransactionsResponse, error) {
		return transactionsHandler.getTransactionsByLedgerSequence(context, request)
	})
}
