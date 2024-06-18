package methods

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/creachadair/jrpc2"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/daemon/interfaces"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/db"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/preflight"
)

type SimulateTransactionRequest struct {
	Transaction    string                    `json:"transaction"`
	ResourceConfig *preflight.ResourceConfig `json:"resourceConfig,omitempty"`
}

type SimulateTransactionCost struct {
	CPUInstructions uint64 `json:"cpuInsns,string"`
	MemoryBytes     uint64 `json:"memBytes,string"`
}

// SimulateHostFunctionResult contains the simulation result of each HostFunction within the single InvokeHostFunctionOp allowed in a Transaction
type SimulateHostFunctionResult struct {
	Auth []string `json:"auth"`
	XDR  string   `json:"xdr"`
}

type RestorePreamble struct {
	TransactionData string `json:"transactionData"` // SorobanTransactionData XDR in base64
	MinResourceFee  int64  `json:"minResourceFee,string"`
}
type LedgerEntryChangeType int

const (
	LedgerEntryChangeTypeCreated LedgerEntryChangeType = iota + 1
	LedgerEntryChangeTypeUpdated
	LedgerEntryChangeTypeDeleted
)

var (
	LedgerEntryChangeTypeName = map[LedgerEntryChangeType]string{
		LedgerEntryChangeTypeCreated: "created",
		LedgerEntryChangeTypeUpdated: "updated",
		LedgerEntryChangeTypeDeleted: "deleted",
	}
	LedgerEntryChangeTypeValue = map[string]LedgerEntryChangeType{
		"created": LedgerEntryChangeTypeCreated,
		"updated": LedgerEntryChangeTypeUpdated,
		"deleted": LedgerEntryChangeTypeDeleted,
	}
)

func (l LedgerEntryChangeType) String() string {
	return LedgerEntryChangeTypeName[l]
}

func (l LedgerEntryChangeType) MarshalJSON() ([]byte, error) {
	return json.Marshal(l.String())
}

func (l *LedgerEntryChangeType) Parse(s string) error {
	s = strings.TrimSpace(strings.ToLower(s))
	value, ok := LedgerEntryChangeTypeValue[s]
	if !ok {
		return fmt.Errorf("%q is not a valid ledger entry change type", s)
	}
	*l = value
	return nil
}

func (l *LedgerEntryChangeType) UnmarshalJSON(data []byte) (err error) {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	return l.Parse(s)
}

func (l *LedgerEntryChange) FromXDRDiff(diff preflight.XDRDiff) error {
	beforePresent := len(diff.Before) > 0
	afterPresent := len(diff.After) > 0
	var (
		entryXDR   []byte
		changeType LedgerEntryChangeType
	)
	switch {
	case beforePresent:
		entryXDR = diff.Before
		if afterPresent {
			changeType = LedgerEntryChangeTypeUpdated
		} else {
			changeType = LedgerEntryChangeTypeDeleted
		}
	case afterPresent:
		entryXDR = diff.After
		changeType = LedgerEntryChangeTypeCreated
	default:
		return errors.New("missing before and after")
	}
	var entry xdr.LedgerEntry

	if err := xdr.SafeUnmarshal(entryXDR, &entry); err != nil {
		return err
	}
	key, err := entry.LedgerKey()
	if err != nil {
		return err
	}
	keyB64, err := xdr.MarshalBase64(key)
	if err != nil {
		return err
	}
	l.Type = changeType
	l.Key = keyB64
	if beforePresent {
		before := base64.StdEncoding.EncodeToString(diff.Before)
		l.Before = &before
	}
	if afterPresent {
		after := base64.StdEncoding.EncodeToString(diff.After)
		l.After = &after
	}
	return nil
}

// LedgerEntryChange designates a change in a ledger entry. Before and After cannot be omitted at the same time.
// If Before is omitted, it constitutes a creation, if After is omitted, it constitutes a deletion.
type LedgerEntryChange struct {
	Type   LedgerEntryChangeType `json:"type"`
	Key    string                `json:"key"`    // LedgerEntryKey in base64
	Before *string               `json:"before"` // LedgerEntry XDR in base64
	After  *string               `json:"after"`  // LedgerEntry XDR in base64
}

type SimulateTransactionResponse struct {
	Error           string                       `json:"error,omitempty"`
	TransactionData string                       `json:"transactionData,omitempty"` // SorobanTransactionData XDR in base64
	MinResourceFee  int64                        `json:"minResourceFee,string,omitempty"`
	Events          []string                     `json:"events,omitempty"`          // DiagnosticEvent XDR in base64
	Results         []SimulateHostFunctionResult `json:"results,omitempty"`         // an array of the individual host function call results
	Cost            SimulateTransactionCost      `json:"cost,omitempty"`            // the effective cpu and memory cost of the invoked transaction execution.
	RestorePreamble *RestorePreamble             `json:"restorePreamble,omitempty"` // If present, it indicates that a prior RestoreFootprint is required
	StateChanges    []LedgerEntryChange          `json:"stateChanges,omitempty"`    // If present, it indicates how the state (ledger entries) will change as a result of the transaction execution.
	LatestLedger    uint32                       `json:"latestLedger"`
}

type PreflightGetter interface {
	GetPreflight(ctx context.Context, params preflight.PreflightGetterParameters) (preflight.Preflight, error)
}

// NewSimulateTransactionHandler returns a json rpc handler to run preflight simulations
func NewSimulateTransactionHandler(logger *log.Entry, ledgerEntryReader db.LedgerEntryReader, ledgerReader db.LedgerReader, daemon interfaces.Daemon, getter PreflightGetter) jrpc2.Handler {

	return NewHandler(func(ctx context.Context, request SimulateTransactionRequest) SimulateTransactionResponse {
		var txEnvelope xdr.TransactionEnvelope
		if err := xdr.SafeUnmarshalBase64(request.Transaction, &txEnvelope); err != nil {
			logger.WithError(err).WithField("request", request).
				Info("could not unmarshal simulate transaction envelope")
			return SimulateTransactionResponse{
				Error: "Could not unmarshal transaction",
			}
		}
		if len(txEnvelope.Operations()) != 1 {
			return SimulateTransactionResponse{
				Error: "Transaction contains more than one operation",
			}
		}
		op := txEnvelope.Operations()[0]

		var sourceAccount xdr.AccountId
		if opSourceAccount := op.SourceAccount; opSourceAccount != nil {
			sourceAccount = opSourceAccount.ToAccountId()
		} else {
			sourceAccount = txEnvelope.SourceAccount().ToAccountId()
		}

		footprint := xdr.LedgerFootprint{}
		switch op.Body.Type {
		case xdr.OperationTypeInvokeHostFunction:
		case xdr.OperationTypeExtendFootprintTtl, xdr.OperationTypeRestoreFootprint:
			if txEnvelope.Type != xdr.EnvelopeTypeEnvelopeTypeTx && txEnvelope.V1.Tx.Ext.V != 1 {
				return SimulateTransactionResponse{
					Error: "To perform a SimulateTransaction for ExtendFootprintTtl or RestoreFootprint operations, SorobanTransactionData must be provided",
				}
			}
			footprint = txEnvelope.V1.Tx.Ext.SorobanData.Resources.Footprint
		default:
			return SimulateTransactionResponse{
				Error: "Transaction contains unsupported operation type: " + op.Body.Type.String(),
			}
		}

		readTx, err := ledgerEntryReader.NewCachedTx(ctx)
		if err != nil {
			return SimulateTransactionResponse{
				Error: "Cannot create read transaction",
			}
		}
		defer func() {
			_ = readTx.Done()
		}()
		latestLedger, err := readTx.GetLatestLedgerSequence()
		if err != nil {
			return SimulateTransactionResponse{
				Error: err.Error(),
			}
		}
		bucketListSize, protocolVersion, err := getBucketListSizeAndProtocolVersion(ctx, ledgerReader, latestLedger)
		if err != nil {
			return SimulateTransactionResponse{
				Error: err.Error(),
			}
		}

		resource_config := preflight.DefaultResourceConfig()
		if request.ResourceConfig != nil {
			resource_config = *request.ResourceConfig
		}
		params := preflight.PreflightGetterParameters{
			LedgerEntryReadTx: readTx,
			BucketListSize:    bucketListSize,
			SourceAccount:     sourceAccount,
			OperationBody:     op.Body,
			Footprint:         footprint,
			ResourceConfig:    resource_config,
			ProtocolVersion:   protocolVersion,
		}
		result, err := getter.GetPreflight(ctx, params)
		if err != nil {
			return SimulateTransactionResponse{
				Error:        err.Error(),
				LatestLedger: latestLedger,
			}
		}

		var results []SimulateHostFunctionResult
		if len(result.Result) != 0 {
			results = append(results, SimulateHostFunctionResult{
				XDR:  base64.StdEncoding.EncodeToString(result.Result),
				Auth: base64EncodeSlice(result.Auth),
			})
		}
		var restorePreamble *RestorePreamble = nil
		if len(result.PreRestoreTransactionData) != 0 {
			restorePreamble = &RestorePreamble{
				TransactionData: base64.StdEncoding.EncodeToString(result.PreRestoreTransactionData),
				MinResourceFee:  result.PreRestoreMinFee,
			}
		}

		stateChanges := make([]LedgerEntryChange, len(result.LedgerEntryDiff))
		for i := 0; i < len(stateChanges); i++ {
			stateChanges[i].FromXDRDiff(result.LedgerEntryDiff[i])
		}

		return SimulateTransactionResponse{
			Error:           result.Error,
			Results:         results,
			Events:          base64EncodeSlice(result.Events),
			TransactionData: base64.StdEncoding.EncodeToString(result.TransactionData),
			MinResourceFee:  result.MinFee,
			Cost: SimulateTransactionCost{
				CPUInstructions: result.CPUInstructions,
				MemoryBytes:     result.MemoryBytes,
			},
			LatestLedger:    latestLedger,
			RestorePreamble: restorePreamble,
			StateChanges:    stateChanges,
		}
	})
}

func base64EncodeSlice(in [][]byte) []string {
	result := make([]string, len(in))
	for i, v := range in {
		result[i] = base64.StdEncoding.EncodeToString(v)
	}
	return result
}

func getBucketListSizeAndProtocolVersion(ctx context.Context, ledgerReader db.LedgerReader, latestLedger uint32) (uint64, uint32, error) {
	// obtain bucket size
	var closeMeta, ok, err = ledgerReader.GetLedger(ctx, latestLedger)
	if err != nil {
		return 0, 0, err
	}
	if !ok {
		return 0, 0, fmt.Errorf("missing meta for latest ledger (%d)", latestLedger)
	}
	if closeMeta.V != 1 {
		return 0, 0, fmt.Errorf("latest ledger (%d) meta has unexpected verion (%d)", latestLedger, closeMeta.V)
	}
	return uint64(closeMeta.V1.TotalByteSizeOfBucketList), uint32(closeMeta.V1.LedgerHeader.Header.LedgerVersion), nil
}
