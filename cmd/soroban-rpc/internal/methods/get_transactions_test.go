package methods

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/transactions"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/util"
)

const (
	NetworkPassphrase          string = "passphrase"
	LatestLedger               int64  = 1136657
	LatestLedgerCloseTimestamp int64  = 28419025
)

// createTestLedger Create a ledger with 2 transactions
func createTestLedger(sequence uint32) xdr.LedgerCloseMeta {
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

func getMockReaders(ctrl *gomock.Controller, ledgerFound bool, err error) (*util.MockLedgerEntryReader, *util.MockLedgerReader) {
	mockLedgerReader := util.NewMockLedgerReader(ctrl)
	mockLedgerReader.EXPECT().
		GetLedger(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, bool, error) {
			var meta xdr.LedgerCloseMeta
			if ledgerFound {
				meta = createTestLedger(sequence)
			}
			return meta, ledgerFound, err
		}).AnyTimes()

	mockLedgerEntryReader := util.NewMockLedgerEntryReader(ctrl)
	mockLedgerEntryReader.EXPECT().
		GetLatestLedgerSequence(gomock.Any()).
		Return(uint32(LatestLedger), nil).AnyTimes()

	return mockLedgerEntryReader, mockLedgerReader
}

func TestGetTransactions_DefaultLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockLedgerEntryReader, mockLedgerReader := getMockReaders(ctrl, true, nil)

	handler := transactionsRPCHandler{
		ledgerReader:      mockLedgerReader,
		ledgerEntryReader: mockLedgerEntryReader,
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := GetTransactionsRequest{
		StartLedger: 1,
		EndLedger:   2,
	}

	response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	assert.NoError(t, err)

	// assert latest ledger details
	assert.Equal(t, response.LatestLedger, LatestLedger)
	assert.Equal(t, response.LatestLedgerCloseTimestamp, LatestLedgerCloseTimestamp)

	// assert pagination
	assert.Equal(t, int(response.Pagination.Limit), 10)
	cursor := transactions.NewCursor(102, 1, 0)
	assert.Equal(t, response.Pagination.Cursor, &cursor)

	// assert transactions result
	assert.Equal(t, len(response.Transactions), 4)
}

func TestGetTransactions_CustomLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockLedgerEntryReader, mockLedgerReader := getMockReaders(ctrl, true, nil)

	handler := transactionsRPCHandler{
		ledgerReader:      mockLedgerReader,
		ledgerEntryReader: mockLedgerEntryReader,
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := GetTransactionsRequest{
		StartLedger: 1,
		EndLedger:   2,
		Pagination: &TransactionsPaginationOptions{
			Limit: 2,
		},
	}

	response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	assert.NoError(t, err)

	// assert latest ledger details
	assert.Equal(t, response.LatestLedger, LatestLedger)
	assert.Equal(t, response.LatestLedgerCloseTimestamp, LatestLedgerCloseTimestamp)

	// assert pagination
	assert.Equal(t, int(response.Pagination.Limit), 2)
	cursor := transactions.NewCursor(101, 1, 0)
	assert.Equal(t, response.Pagination.Cursor, &cursor)

	// assert transactions result
	assert.Equal(t, len(response.Transactions), 2)
	assert.Equal(t, int(response.Transactions[0].LedgerSequence), 101)
	assert.Equal(t, int(response.Transactions[1].LedgerSequence), 101)

}

func TestGetTransactions_CustomLimitAndCursor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockLedgerEntryReader, mockLedgerReader := getMockReaders(ctrl, true, nil)

	handler := transactionsRPCHandler{
		ledgerReader:      mockLedgerReader,
		ledgerEntryReader: mockLedgerEntryReader,
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := GetTransactionsRequest{
		EndLedger: 2,
		Pagination: &TransactionsPaginationOptions{
			Cursor: &transactions.Cursor{
				LedgerSequence: 1,
				TxIdx:          0,
				OpIdx:          0,
			},
			Limit: 2,
		},
	}

	response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	assert.NoError(t, err)

	// assert latest ledger details
	assert.Equal(t, response.LatestLedger, LatestLedger)
	assert.Equal(t, response.LatestLedgerCloseTimestamp, LatestLedgerCloseTimestamp)

	// assert pagination
	assert.Equal(t, int(response.Pagination.Limit), 2)
	cursor := transactions.NewCursor(102, 0, 0)
	assert.Equal(t, response.Pagination.Cursor, &cursor)

	// assert transactions result
	assert.Equal(t, len(response.Transactions), 2)
	assert.Equal(t, int(response.Transactions[0].LedgerSequence), 101)
	assert.Equal(t, int(response.Transactions[1].LedgerSequence), 102)
}

func TestGetTransactions_LedgerNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockLedgerEntryReader, mockLedgerReader := getMockReaders(ctrl, false, nil)

	handler := transactionsRPCHandler{
		ledgerReader:      mockLedgerReader,
		ledgerEntryReader: mockLedgerEntryReader,
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := GetTransactionsRequest{
		StartLedger: 1,
		EndLedger:   2,
	}

	response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	assert.Equal(t, err.Error(), "[-32602] ledger close meta not found: 1")
	assert.Nil(t, response.Transactions)
}

func TestGetTransactions_LedgerDbError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	err := errors.New("error reading from db")
	mockLedgerEntryReader, mockLedgerReader := getMockReaders(ctrl, false, err)

	handler := transactionsRPCHandler{
		ledgerReader:      mockLedgerReader,
		ledgerEntryReader: mockLedgerEntryReader,
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := GetTransactionsRequest{
		StartLedger: 1,
		EndLedger:   2,
	}

	response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	assert.Equal(t, err.Error(), "[-32602] error reading from db")
	assert.Nil(t, response.Transactions)
}

func TestGetTransactions_InvalidLedgerRange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockLedgerEntryReader, mockLedgerReader := getMockReaders(ctrl, true, nil)

	handler := transactionsRPCHandler{
		ledgerReader:      mockLedgerReader,
		ledgerEntryReader: mockLedgerEntryReader,
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := GetTransactionsRequest{
		StartLedger: 3,
		EndLedger:   2,
	}

	_, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	assert.Equal(t, err.Error(), "[-32602] end ledger cannot be less than start ledger")
}

func TestGetTransactions_LimitGreaterThanMaxLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockLedgerEntryReader, mockLedgerReader := getMockReaders(ctrl, true, nil)

	handler := transactionsRPCHandler{
		ledgerReader:      mockLedgerReader,
		ledgerEntryReader: mockLedgerEntryReader,
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := GetTransactionsRequest{
		StartLedger: 1,
		EndLedger:   2,
		Pagination: &TransactionsPaginationOptions{
			Limit: 200,
		},
	}

	_, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	assert.Equal(t, err.Error(), "[-32602] limit must not exceed 100")
}
