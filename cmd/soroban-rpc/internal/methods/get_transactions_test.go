package methods

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go/toid"
	"github.com/stellar/go/xdr"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/db"
)

const (
	NetworkPassphrase string = "passphrase"
)

// createTestLedger Creates a test ledger with 2 transactions
func createTestLedger(sequence uint32) xdr.LedgerCloseMeta {
	sequence -= 100
	meta := txMeta(sequence, true)
	meta.V1.TxProcessing = append(meta.V1.TxProcessing, xdr.TransactionResultMeta{
		TxApplyProcessing: xdr.TransactionMeta{
			V:          3,
			Operations: &[]xdr.OperationMeta{},
			V3:         &xdr.TransactionMetaV3{},
		},
		Result: xdr.TransactionResultPair{
			TransactionHash: txHash(sequence),
			Result:          transactionResult(false),
		},
	})
	return meta
}

func TestGetTransactions_DefaultLimit(t *testing.T) {
	mockDBReader := db.NewMockTransactionStore(NetworkPassphrase)
	mockLedgerReader := db.NewMockLedgerReader(mockDBReader)
	for i := 1; i <= 10; i++ {
		meta := createTestLedger(uint32(i))
		err := mockDBReader.InsertTransactions(meta)
		require.NoError(t, err)
	}

	handler := transactionsRPCHandler{
		ledgerReader:      mockLedgerReader,
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := GetTransactionsRequest{
		StartLedger: 1,
	}

	response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	require.NoError(t, err)

	// assert latest ledger details
	assert.Equal(t, uint32(10), response.LatestLedger)
	assert.Equal(t, int64(350), response.LatestLedgerCloseTime)

	// assert pagination
	assert.Equal(t, toid.New(5, 2, 1).String(), response.Cursor)

	// assert transactions result
	assert.Equal(t, 10, len(response.Transactions))
}

func TestGetTransactions_DefaultLimitExceedsLatestLedger(t *testing.T) {
	mockDBReader := db.NewMockTransactionStore(NetworkPassphrase)
	mockLedgerReader := db.NewMockLedgerReader(mockDBReader)
	for i := 1; i <= 3; i++ {
		meta := createTestLedger(uint32(i))
		err := mockDBReader.InsertTransactions(meta)
		require.NoError(t, err)
	}

	handler := transactionsRPCHandler{
		ledgerReader:      mockLedgerReader,
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := GetTransactionsRequest{
		StartLedger: 1,
	}

	response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	require.NoError(t, err)

	// assert latest ledger details
	assert.Equal(t, uint32(3), response.LatestLedger)
	assert.Equal(t, int64(175), response.LatestLedgerCloseTime)

	// assert pagination
	assert.Equal(t, toid.New(3, 2, 1).String(), response.Cursor)

	// assert transactions result
	assert.Len(t, response.Transactions, 6)
}

func TestGetTransactions_CustomLimit(t *testing.T) {
	mockDBReader := db.NewMockTransactionStore(NetworkPassphrase)
	mockLedgerReader := db.NewMockLedgerReader(mockDBReader)
	for i := 1; i <= 10; i++ {
		meta := createTestLedger(uint32(i))
		err := mockDBReader.InsertTransactions(meta)
		require.NoError(t, err)
	}

	handler := transactionsRPCHandler{
		ledgerReader:      mockLedgerReader,
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := GetTransactionsRequest{
		StartLedger: 1,
		Pagination: &TransactionsPaginationOptions{
			Limit: 2,
		},
	}

	response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	require.NoError(t, err)

	// assert latest ledger details
	assert.Equal(t, uint32(10), response.LatestLedger)
	assert.Equal(t, int64(350), response.LatestLedgerCloseTime)

	// assert pagination
	assert.Equal(t, toid.New(1, 2, 1).String(), response.Cursor)

	// assert transactions result
	assert.Len(t, response.Transactions, 2)
	assert.Equal(t, uint32(1), response.Transactions[0].Ledger)
	assert.Equal(t, uint32(1), response.Transactions[1].Ledger)
}

func TestGetTransactions_CustomLimitAndCursor(t *testing.T) {
	mockDBReader := db.NewMockTransactionStore(NetworkPassphrase)
	mockLedgerReader := db.NewMockLedgerReader(mockDBReader)
	for i := 1; i <= 10; i++ {
		meta := createTestLedger(uint32(i))
		err := mockDBReader.InsertTransactions(meta)
		require.NoError(t, err)
	}

	handler := transactionsRPCHandler{
		ledgerReader:      mockLedgerReader,
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := GetTransactionsRequest{
		Pagination: &TransactionsPaginationOptions{
			Cursor: toid.New(1, 2, 1).String(),
			Limit:  3,
		},
	}

	response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	require.NoError(t, err)

	// assert latest ledger details
	assert.Equal(t, uint32(10), response.LatestLedger)
	assert.Equal(t, int64(350), response.LatestLedgerCloseTime)

	// assert pagination
	assert.Equal(t, toid.New(3, 1, 1).String(), response.Cursor)

	// assert transactions result
	assert.Equal(t, 3, len(response.Transactions))
	assert.Equal(t, uint32(2), response.Transactions[0].Ledger)
	assert.Equal(t, uint32(2), response.Transactions[1].Ledger)
	assert.Equal(t, uint32(3), response.Transactions[2].Ledger)
}

func TestGetTransactions_InvalidStartLedger(t *testing.T) {
	mockDBReader := db.NewMockTransactionStore(NetworkPassphrase)
	mockLedgerReader := db.NewMockLedgerReader(mockDBReader)
	for i := 1; i <= 3; i++ {
		meta := createTestLedger(uint32(i))
		err := mockDBReader.InsertTransactions(meta)
		require.NoError(t, err)
	}

	handler := transactionsRPCHandler{
		ledgerReader:      mockLedgerReader,
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := GetTransactionsRequest{
		StartLedger: 4,
	}

	response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	expectedErr := fmt.Errorf(
		"[%d] start ledger must be between the oldest ledger: 1 and the latest ledger: 3 for this rpc instance",
		jrpc2.InvalidRequest,
	)
	assert.Equal(t, expectedErr.Error(), err.Error())
	assert.Nil(t, response.Transactions)
}

func TestGetTransactions_LedgerNotFound(t *testing.T) {
	mockDBReader := db.NewMockTransactionStore(NetworkPassphrase)
	mockLedgerReader := db.NewMockLedgerReader(mockDBReader)
	for i := 1; i <= 3; i++ {
		// Skip creation of ledger 2
		if i == 2 {
			continue
		}
		meta := createTestLedger(uint32(i))
		err := mockDBReader.InsertTransactions(meta)
		require.NoError(t, err)
	}

	handler := transactionsRPCHandler{
		ledgerReader:      mockLedgerReader,
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := GetTransactionsRequest{
		StartLedger: 1,
	}

	response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	expectedErr := fmt.Errorf("[%d] database does not contain metadata for ledger: 2", jrpc2.InvalidParams)
	assert.Equal(t, expectedErr.Error(), err.Error())
	assert.Nil(t, response.Transactions)
}

func TestGetTransactions_LimitGreaterThanMaxLimit(t *testing.T) {
	mockDBReader := db.NewMockTransactionStore(NetworkPassphrase)
	mockLedgerReader := db.NewMockLedgerReader(mockDBReader)
	for i := 1; i <= 3; i++ {
		meta := createTestLedger(uint32(i))
		err := mockDBReader.InsertTransactions(meta)
		require.NoError(t, err)
	}

	handler := transactionsRPCHandler{
		ledgerReader:      mockLedgerReader,
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := GetTransactionsRequest{
		StartLedger: 1,
		Pagination: &TransactionsPaginationOptions{
			Limit: 200,
		},
	}

	_, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	expectedErr := fmt.Errorf("[%d] limit must not exceed 100", jrpc2.InvalidRequest)
	assert.Equal(t, expectedErr.Error(), err.Error())
}

func TestGetTransactions_InvalidCursorString(t *testing.T) {
	mockDBReader := db.NewMockTransactionStore(NetworkPassphrase)
	mockLedgerReader := db.NewMockLedgerReader(mockDBReader)
	for i := 1; i <= 3; i++ {
		meta := createTestLedger(uint32(i))
		err := mockDBReader.InsertTransactions(meta)
		require.NoError(t, err)
	}

	handler := transactionsRPCHandler{
		ledgerReader:      mockLedgerReader,
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := GetTransactionsRequest{
		Pagination: &TransactionsPaginationOptions{
			Cursor: "abc",
		},
	}

	_, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	expectedErr := fmt.Errorf("[%d] strconv.ParseInt: parsing \"abc\": invalid syntax", jrpc2.InvalidParams)
	assert.Equal(t, expectedErr.Error(), err.Error())
}

func TestGetTransactions_JSONFormat(t *testing.T) {
	mockDBReader := db.NewMockTransactionStore(NetworkPassphrase)
	mockLedgerReader := db.NewMockLedgerReader(mockDBReader)
	for i := 1; i <= 3; i++ {
		meta := createTestLedger(uint32(i))
		err := mockDBReader.InsertTransactions(meta)
		require.NoError(t, err)
	}

	handler := transactionsRPCHandler{
		ledgerReader:      mockLedgerReader,
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := GetTransactionsRequest{
		Format:      FormatJSON,
		StartLedger: 1,
	}

	js, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	require.NoError(t, err)

	// Do a marshaling round-trip on a transaction so we can check that the
	// fields are encoded correctly as JSON.
	txResp := js.Transactions[0]
	jsBytes, err := json.Marshal(txResp)
	require.NoError(t, err)

	var tx map[string]interface{}
	require.NoError(t, json.Unmarshal(jsBytes, &tx))

	require.Nilf(t, tx["envelopeXdr"], "field: 'envelopeXdr'")
	require.NotNilf(t, tx["envelopeJson"], "field: 'envelopeJson'")
	require.Nilf(t, tx["resultXdr"], "field: 'resultXdr'")
	require.NotNilf(t, tx["resultJson"], "field: 'resultJson'")
	require.Nilf(t, tx["resultMetaXdr"], "field: 'resultMetaXdr'")
	require.NotNilf(t, tx["resultMetaJson"], "field: 'resultMetaJson'")
}
