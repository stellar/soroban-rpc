package methods

import (
	"context"
	"testing"

	"github.com/stellar/go/toid"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/db"
)

const (
	NetworkPassphrase string = "passphrase"
)

// createTestLedger Creates a test ledger with 2 transactions
func createTestLedger(sequence uint32) xdr.LedgerCloseMeta {
	sequence = sequence - 100
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
	mockDbReader := db.NewMockTransactionStore(NetworkPassphrase)
	for i := 1; i <= 10; i++ {
		meta := createTestLedger(uint32(i))
		err := mockDbReader.InsertTransactions(meta)
		assert.NoError(t, err)
	}
	mockLedgerReader := db.NewMockLedgerReader(mockDbReader)

	handler := transactionsRPCHandler{
		ledgerReader:      mockLedgerReader,
		dbReader:          mockDbReader,
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := GetTransactionsRequest{
		StartLedger: 1,
	}

	response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	assert.NoError(t, err)

	// assert latest ledger details
	assert.Equal(t, response.LatestLedger, uint32(10))
	assert.Equal(t, response.LatestLedgerCloseTime, int64(350))

	// assert pagination
	assert.Equal(t, response.Cursor, toid.New(5, 1, 0).String())

	// assert transactions result
	assert.Equal(t, len(response.Transactions), 10)
}

func TestGetTransactions_DefaultLimitExceedsLatestLedger(t *testing.T) {
	mockDbReader := db.NewMockTransactionStore(NetworkPassphrase)
	for i := 1; i <= 3; i++ {
		meta := createTestLedger(uint32(i))
		err := mockDbReader.InsertTransactions(meta)
		assert.NoError(t, err)
	}
	mockLedgerReader := db.NewMockLedgerReader(mockDbReader)

	handler := transactionsRPCHandler{
		ledgerReader:      mockLedgerReader,
		dbReader:          mockDbReader,
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := GetTransactionsRequest{
		StartLedger: 1,
	}

	response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	assert.NoError(t, err)

	// assert latest ledger details
	assert.Equal(t, response.LatestLedger, uint32(3))
	assert.Equal(t, response.LatestLedgerCloseTime, int64(175))

	// assert pagination
	assert.Equal(t, response.Cursor, toid.New(3, 1, 0).String())

	// assert transactions result
	assert.Equal(t, len(response.Transactions), 6)
}

//func TestGetTransactions_CustomLimit(t *testing.T) {
//	ctrl := gomock.NewController(t)
//	defer ctrl.Finish()
//
//	mockLedgerEntryReader, mockLedgerReader := getMockReaders(ctrl, true, 10, nil)
//
//	handler := transactionsRPCHandler{
//		ledgerReader:      mockLedgerReader,
//		ledgerEntryReader: mockLedgerEntryReader,
//		maxLimit:          100,
//		defaultLimit:      10,
//		networkPassphrase: NetworkPassphrase,
//	}
//
//	request := GetTransactionsRequest{
//		StartLedger: 1,
//		Pagination: &TransactionsPaginationOptions{
//			Limit: 2,
//		},
//	}
//
//	response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
//	assert.NoError(t, err)
//
//	// assert latest ledger details
//	assert.Equal(t, response.LatestLedger, int64(10))
//	assert.Equal(t, response.LatestLedgerCloseTimestamp, int64(350))
//
//	// assert pagination
//	assert.Equal(t, response.Cursor, toid.New(1, 1, 0).String())
//
//	// assert transactions result
//	assert.Equal(t, len(response.Transactions), 2)
//	assert.Equal(t, int(response.Transactions[0].LedgerSequence), 1)
//	assert.Equal(t, int(response.Transactions[1].LedgerSequence), 1)
//}
//
//func TestGetTransactions_CustomLimitAndCursor(t *testing.T) {
//	ctrl := gomock.NewController(t)
//	defer ctrl.Finish()
//
//	mockLedgerEntryReader, mockLedgerReader := getMockReaders(ctrl, true, 10, nil)
//
//	handler := transactionsRPCHandler{
//		ledgerReader:      mockLedgerReader,
//		ledgerEntryReader: mockLedgerEntryReader,
//		maxLimit:          100,
//		defaultLimit:      10,
//		networkPassphrase: NetworkPassphrase,
//	}
//
//	request := GetTransactionsRequest{
//		Pagination: &TransactionsPaginationOptions{
//			Cursor: &toid.ID{
//				LedgerSequence:   1,
//				TransactionOrder: 1,
//				OperationOrder:   0,
//			},
//			Limit: 3,
//		},
//	}
//
//	response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
//	assert.NoError(t, err)
//
//	// assert latest ledger details
//	assert.Equal(t, response.LatestLedger, int64(10))
//	assert.Equal(t, response.LatestLedgerCloseTimestamp, int64(350))
//
//	// assert pagination
//	assert.Equal(t, response.Cursor, toid.New(3, 0, 0).String())
//
//	// assert transactions result
//	assert.Equal(t, len(response.Transactions), 3)
//	assert.Equal(t, int(response.Transactions[0].LedgerSequence), 2)
//	assert.Equal(t, int(response.Transactions[1].LedgerSequence), 2)
//	assert.Equal(t, int(response.Transactions[2].LedgerSequence), 3)
//}
//
//func TestGetTransactions_LedgerNotFound(t *testing.T) {
//	ctrl := gomock.NewController(t)
//	defer ctrl.Finish()
//
//	mockLedgerEntryReader, mockLedgerReader := getMockReaders(ctrl, false, 10, nil)
//
//	handler := transactionsRPCHandler{
//		ledgerReader:      mockLedgerReader,
//		ledgerEntryReader: mockLedgerEntryReader,
//		maxLimit:          100,
//		defaultLimit:      10,
//		networkPassphrase: NetworkPassphrase,
//	}
//
//	request := GetTransactionsRequest{
//		StartLedger: 1,
//	}
//
//	response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
//	assert.Equal(t, err.Error(), "[-32602] ledger close meta not found: 10")
//	assert.Nil(t, response.Transactions)
//}
//
//func TestGetTransactions_LedgerDbError(t *testing.T) {
//	ctrl := gomock.NewController(t)
//	defer ctrl.Finish()
//
//	err := errors.New("error reading from db")
//	mockLedgerEntryReader, mockLedgerReader := getMockReaders(ctrl, false, 10, err)
//
//	handler := transactionsRPCHandler{
//		ledgerReader:      mockLedgerReader,
//		ledgerEntryReader: mockLedgerEntryReader,
//		maxLimit:          100,
//		defaultLimit:      10,
//		networkPassphrase: NetworkPassphrase,
//	}
//
//	request := GetTransactionsRequest{
//		StartLedger: 1,
//	}
//
//	response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
//	assert.Equal(t, err.Error(), "[-32602] error reading from db")
//	assert.Nil(t, response.Transactions)
//}
//
//func TestGetTransactions_LimitGreaterThanMaxLimit(t *testing.T) {
//	ctrl := gomock.NewController(t)
//	defer ctrl.Finish()
//
//	mockLedgerEntryReader, mockLedgerReader := getMockReaders(ctrl, true, 10, nil)
//
//	handler := transactionsRPCHandler{
//		ledgerReader:      mockLedgerReader,
//		ledgerEntryReader: mockLedgerEntryReader,
//		maxLimit:          100,
//		defaultLimit:      10,
//		networkPassphrase: NetworkPassphrase,
//	}
//
//	request := GetTransactionsRequest{
//		StartLedger: 1,
//		Pagination: &TransactionsPaginationOptions{
//			Limit: 200,
//		},
//	}
//
//	_, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
//	assert.Equal(t, err.Error(), "[-32602] limit must not exceed 100")
//}
