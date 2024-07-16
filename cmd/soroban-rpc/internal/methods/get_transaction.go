package methods

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/creachadair/jrpc2"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/db"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/xdr2json"
)

const (
	// TransactionStatusSuccess indicates the transaction was included in the ledger and
	// it was executed without errors.
	TransactionStatusSuccess = "SUCCESS"
	// TransactionStatusNotFound indicates the transaction was not found in Soroban-RPC's
	// transaction store.
	TransactionStatusNotFound = "NOT_FOUND"
	// TransactionStatusFailed indicates the transaction was included in the ledger and
	// it was executed with an error.
	TransactionStatusFailed = "FAILED"
)

// GetTransactionResponse is the response for the Soroban-RPC getTransaction() endpoint
type GetTransactionResponse struct {
	// Status is one of: TransactionSuccess, TransactionNotFound, or TransactionFailed.
	Status string `json:"status"`
	// LatestLedger is the latest ledger stored in Soroban-RPC.
	LatestLedger uint32 `json:"latestLedger"`
	// LatestLedgerCloseTime is the unix timestamp of when the latest ledger was closed.
	LatestLedgerCloseTime int64 `json:"latestLedgerCloseTime,string"`
	// LatestLedger is the oldest ledger stored in Soroban-RPC.
	OldestLedger uint32 `json:"oldestLedger"`
	// LatestLedgerCloseTime is the unix timestamp of when the oldest ledger was closed.
	OldestLedgerCloseTime int64 `json:"oldestLedgerCloseTime,string"`

	// The fields below are only present if Status is not TransactionNotFound.

	// ApplicationOrder is the index of the transaction among all the transactions
	// for that ledger.
	ApplicationOrder int32 `json:"applicationOrder,omitempty"`
	// FeeBump indicates whether the transaction is a feebump transaction
	FeeBump bool `json:"feeBump,omitempty"`
	// EnvelopeXdr is the TransactionEnvelope XDR value.
	EnvelopeXdr string                 `json:"envelopeXdr,omitempty"`
	Envelope    map[string]interface{} `json:"envelope,omitempty"`
	// ResultXdr is the TransactionResult XDR value.
	ResultXdr string                 `json:"resultXdr,omitempty"`
	Result    map[string]interface{} `json:"result,omitempty"`
	// ResultMetaXdr is the TransactionMeta XDR value.
	ResultMetaXdr string                 `json:"resultMetaXdr,omitempty"`
	ResultMeta    map[string]interface{} `json:"resultMeta,omitempty"`

	// Ledger is the sequence of the ledger which included the transaction.
	Ledger uint32 `json:"ledger,omitempty"`
	// LedgerCloseTime is the unix timestamp of when the transaction was included in the ledger.
	LedgerCloseTime int64 `json:"createdAt,string,omitempty"`

	// DiagnosticEventsXDR is present only if Status is equal to TransactionFailed.
	// DiagnosticEventsXDR is a base64-encoded slice of xdr.DiagnosticEvent
	DiagnosticEventsXDR []string                 `json:"diagnosticEventsXdr,omitempty"`
	DiagnosticEvents    []map[string]interface{} `json:"diagnosticEvents,omitempty"`
}

const (
	XdrFormatBase64 = "base64"
	XdrFormatJSON   = "json"
)

type GetTransactionRequest struct {
	Hash   string `json:"hash"`
	Format string `json:"xdrFormat,omitempty"`
}

func GetTransaction(
	ctx context.Context,
	log *log.Entry,
	reader db.TransactionReader,
	ledgerReader db.LedgerReader,
	request GetTransactionRequest,
) (GetTransactionResponse, error) {
	// parse XDR format expectations
	switch request.Format {
	case "":
		fallthrough
	case XdrFormatJSON:
		fallthrough
	case XdrFormatBase64:
		break
	default:
		return GetTransactionResponse{}, &jrpc2.Error{
			Code: jrpc2.InvalidParams,
			Message: fmt.Sprintf(
				"expected %s for 'xdr_format', got %s",
				strings.Join([]string{XdrFormatBase64, XdrFormatJSON}, ", "),
				request.Format),
		}
	}

	// parse hash
	if hex.DecodedLen(len(request.Hash)) != len(xdr.Hash{}) {
		return GetTransactionResponse{}, &jrpc2.Error{
			Code:    jrpc2.InvalidParams,
			Message: fmt.Sprintf("unexpected hash length (%d)", len(request.Hash)),
		}
	}

	var txHash xdr.Hash
	_, err := hex.Decode(txHash[:], []byte(request.Hash))
	if err != nil {
		return GetTransactionResponse{}, &jrpc2.Error{
			Code:    jrpc2.InvalidParams,
			Message: fmt.Sprintf("incorrect hash: %v", err),
		}
	}

	storeRange, err := ledgerReader.GetLedgerRange(ctx)
	if err != nil {
		return GetTransactionResponse{}, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: fmt.Sprintf("unable to get ledger range: %v", err),
		}
	}

	tx, err := reader.GetTransaction(ctx, txHash)

	response := GetTransactionResponse{
		LatestLedger:          storeRange.LastLedger.Sequence,
		LatestLedgerCloseTime: storeRange.LastLedger.CloseTime,
		OldestLedger:          storeRange.FirstLedger.Sequence,
		OldestLedgerCloseTime: storeRange.FirstLedger.CloseTime,
	}
	if errors.Is(err, db.ErrNoTransaction) {
		response.Status = TransactionStatusNotFound
		return response, nil
	} else if err != nil {
		log.WithError(err).
			WithField("hash", txHash).
			Errorf("failed to fetch transaction")
		return response, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: err.Error(),
		}
	}

	response.ApplicationOrder = tx.ApplicationOrder
	response.FeeBump = tx.FeeBump
	response.Ledger = tx.Ledger.Sequence
	response.LedgerCloseTime = tx.Ledger.CloseTime

	switch request.Format {
	case "json":
		result, envelope, meta, diagEvents, convErr := transactionToJSON(tx)
		if convErr != nil {
			return response, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: convErr.Error(),
			}
		}

		response.Result = result
		response.Envelope = envelope
		response.ResultMeta = meta
		response.DiagnosticEvents = diagEvents

	default:
		response.ResultXdr = base64.StdEncoding.EncodeToString(tx.Result)
		response.EnvelopeXdr = base64.StdEncoding.EncodeToString(tx.Envelope)
		response.ResultMetaXdr = base64.StdEncoding.EncodeToString(tx.Meta)
		response.DiagnosticEventsXDR = base64EncodeSlice(tx.Events)
	}

	response.Status = TransactionStatusFailed
	if tx.Successful {
		response.Status = TransactionStatusSuccess
	}
	return response, nil
}

// NewGetTransactionHandler returns a get transaction json rpc handler

func NewGetTransactionHandler(logger *log.Entry, getter db.TransactionReader,
	ledgerReader db.LedgerReader,
) jrpc2.Handler {
	return NewHandler(func(ctx context.Context, request GetTransactionRequest) (GetTransactionResponse, error) {
		return GetTransaction(ctx, logger, getter, ledgerReader, request)
	})
}

func transactionToJSON(tx db.Transaction) (
	map[string]interface{},
	map[string]interface{},
	map[string]interface{},
	[]map[string]interface{},
	error,
) {
	var err error
	var result, envelope, resultMeta map[string]interface{}
	var diagEvents []map[string]interface{}

	result, err = xdr2json.Convert(xdr.TransactionResult{}, tx.Result)
	if err != nil {
		return result, envelope, resultMeta, diagEvents, err
	}

	envelope, err = xdr2json.Convert(xdr.TransactionEnvelope{}, tx.Envelope)
	if err != nil {
		return result, envelope, resultMeta, diagEvents, err
	}

	resultMeta, err = xdr2json.Convert(xdr.TransactionMeta{}, tx.Meta)
	if err != nil {
		return result, envelope, resultMeta, diagEvents, err
	}

	diagEvents = make([]map[string]interface{}, len(tx.Events))
	for i, event := range tx.Events {
		diagEvents[i], err = xdr2json.Convert(xdr.DiagnosticEvent{}, event)
		if err != nil {
			return result, envelope, resultMeta, diagEvents, err
		}
	}

	return result, envelope, resultMeta, diagEvents, nil
}
