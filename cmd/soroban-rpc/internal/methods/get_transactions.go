package methods

import (
	"context"
	"encoding/hex"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/db"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/transactions"
)

type TransactionsPaginationOptions struct {
	Cursor *transactions.Cursor `json:"cursor,omitempty"`
	Limit  uint                 `json:"limit,omitempty"`
}

// Range defines a [Start, End] interval of ledgers.
type Range struct {
	// Start defines the (inclusive) start of the range.
	Start transactions.Cursor
	// End defines the (inclusive) end of the range.
	End transactions.Cursor
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

		// Build transaction envelopes in the ledger
		byHash := map[xdr.Hash]xdr.TransactionEnvelope{}
		for _, tx := range ledger.TransactionEnvelopes() {
			hash, err := network.HashTransactionInEnvelope(tx, h.networkPassphrase)
			if err != nil {
				return GetTransactionsResponse{}, &jrpc2.Error{
					Code:    jrpc2.InvalidParams,
					Message: err.Error(),
				}
			}
			byHash[hash] = tx
		}

		// Decode transaction info from ledger meta
		txCount := ledger.CountTransactions()
		for i := start.TxIdx; i < uint32(txCount); i++ {
			cursor = transactions.NewCursor(ledger.LedgerSequence(), i, 0)

			hash := ledger.TransactionHash(int(i))
			envelope, ok := byHash[hash]
			if !ok {
				hexHash := hex.EncodeToString(hash[:])
				err = errors.Errorf("unknown tx hash in LedgerCloseMeta: %v", hexHash)
				return GetTransactionsResponse{}, &jrpc2.Error{
					Code:    jrpc2.InvalidParams,
					Message: err.Error(),
				}
			}
			txResult := ledger.TransactionResultPair(int(i))
			unsafeMeta := ledger.TxApplyProcessing(int(i))

			txInfo := TransactionInfo{
				FeeBump:          envelope.IsFeeBump(),
				ApplicationOrder: int32(i + 1),
				Successful:       txResult.Result.Successful(),
				LedgerSequence:   uint(ledger.LedgerSequence()),
			}
			if txInfo.Result, err = txResult.Result.MarshalBinary(); err != nil {
				return GetTransactionsResponse{}, &jrpc2.Error{
					Code:    jrpc2.InvalidParams,
					Message: err.Error(),
				}
			}
			if txInfo.Meta, err = unsafeMeta.MarshalBinary(); err != nil {
				return GetTransactionsResponse{}, &jrpc2.Error{
					Code:    jrpc2.InvalidParams,
					Message: err.Error(),
				}
			}
			if txInfo.Envelope, err = envelope.MarshalBinary(); err != nil {
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
