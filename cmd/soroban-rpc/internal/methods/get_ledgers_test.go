package methods

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stellar/go/support/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/daemon/interfaces"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/db"
)

var expectedLedgerInfo = LedgerInfo{
	Hash:            "0000000000000000000000000000000000000000000000000000000000000000",
	Sequence:        1,
	LedgerCloseTime: 125,
	LedgerHeader:    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB9AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",                                                                                                                                                                                                                                                                                                                                                         //nolint:lll
	LedgerCloseMeta: "AAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAH0AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAEAAAACAAABAIAAAAAAAAAAPww0v5OtDZlx0EzMkPcFURyDiq2XNKSi+w16A/x/6JoAAAABAAAAAP///50AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGw0LNdyu0BUtYvu6oo7T+kmRyH5+FpqPyiaHsX7ibKLQAAAAAAAABkAAAAAAAAAAAAAAAAAAAAAAAAAAMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==", //nolint:lll
}

func setupTestDB(t *testing.T, numLedgers int) *db.DB {
	testDB := NewTestDB(t)
	daemon := interfaces.MakeNoOpDeamon()
	for sequence := range numLedgers {
		ledgerCloseMeta := txMeta(uint32(sequence)-100, true)
		tx, err := db.NewReadWriter(log.DefaultLogger, testDB, daemon, 150, 15, passphrase).NewTx(context.Background())
		require.NoError(t, err)
		require.NoError(t, tx.LedgerWriter().InsertLedger(ledgerCloseMeta))
		require.NoError(t, tx.Commit(ledgerCloseMeta))
	}
	return testDB
}

func TestGetLedgers_DefaultLimit(t *testing.T) {
	testDB := setupTestDB(t, 11)
	handler := ledgersHandler{
		ledgerReader: db.NewLedgerReader(testDB),
		maxLimit:     100,
		defaultLimit: 5,
	}

	request := GetLedgersRequest{
		StartLedger: 1,
	}

	response, err := handler.getLedgers(context.TODO(), request)
	require.NoError(t, err)

	assert.Equal(t, uint32(10), response.LatestLedger)
	assert.Equal(t, ledgerCloseTime(10), response.LatestLedgerCloseTime)
	assert.Equal(t, "5", response.Cursor)
	assert.Len(t, response.Ledgers, 5)
	assert.Equal(t, uint32(1), response.Ledgers[0].Sequence)
	assert.Equal(t, uint32(5), response.Ledgers[4].Sequence)

	// assert a single ledger info structure for sanity purposes
	assert.Equal(t, expectedLedgerInfo, response.Ledgers[0])
}

func TestGetLedgers_CustomLimit(t *testing.T) {
	testDB := setupTestDB(t, 11)
	handler := ledgersHandler{
		ledgerReader: db.NewLedgerReader(testDB),
		maxLimit:     100,
		defaultLimit: 5,
	}

	request := GetLedgersRequest{
		StartLedger: 1,
		Pagination: &PaginationOptions{
			Limit: 3,
		},
	}

	response, err := handler.getLedgers(context.TODO(), request)
	require.NoError(t, err)

	assert.Equal(t, uint32(10), response.LatestLedger)
	assert.Equal(t, "3", response.Cursor)
	assert.Len(t, response.Ledgers, 3)
	assert.Equal(t, uint32(1), response.Ledgers[0].Sequence)
	assert.Equal(t, uint32(3), response.Ledgers[2].Sequence)
}

func TestGetLedgers_WithCursor(t *testing.T) {
	testDB := setupTestDB(t, 11)
	handler := ledgersHandler{
		ledgerReader: db.NewLedgerReader(testDB),
		maxLimit:     100,
		defaultLimit: 5,
	}

	request := GetLedgersRequest{
		Pagination: &PaginationOptions{
			Cursor: "5",
			Limit:  3,
		},
	}

	response, err := handler.getLedgers(context.TODO(), request)
	require.NoError(t, err)

	assert.Equal(t, uint32(10), response.LatestLedger)
	assert.Equal(t, "8", response.Cursor)
	assert.Len(t, response.Ledgers, 3)
	assert.Equal(t, uint32(6), response.Ledgers[0].Sequence)
	assert.Equal(t, uint32(8), response.Ledgers[2].Sequence)
}

func TestGetLedgers_InvalidStartLedger(t *testing.T) {
	testDB := setupTestDB(t, 11)
	handler := ledgersHandler{
		ledgerReader: db.NewLedgerReader(testDB),
		maxLimit:     100,
		defaultLimit: 5,
	}

	request := GetLedgersRequest{
		StartLedger: 12,
	}

	_, err := handler.getLedgers(context.TODO(), request)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "start ledger must be between")
}

func TestGetLedgers_LimitExceedsMaxLimit(t *testing.T) {
	testDB := setupTestDB(t, 11)
	handler := ledgersHandler{
		ledgerReader: db.NewLedgerReader(testDB),
		maxLimit:     100,
		defaultLimit: 5,
	}

	request := GetLedgersRequest{
		StartLedger: 1,
		Pagination: &PaginationOptions{
			Limit: 101,
		},
	}

	_, err := handler.getLedgers(context.TODO(), request)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "limit must not exceed 100")
}

func TestGetLedgers_InvalidCursor(t *testing.T) {
	testDB := setupTestDB(t, 11)
	handler := ledgersHandler{
		ledgerReader: db.NewLedgerReader(testDB),
		maxLimit:     100,
		defaultLimit: 5,
	}

	request := GetLedgersRequest{
		Pagination: &PaginationOptions{
			Cursor: "invalid",
		},
	}

	_, err := handler.getLedgers(context.TODO(), request)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid syntax")
}

func TestGetLedgers_JSONFormat(t *testing.T) {
	testDB := setupTestDB(t, 11)
	handler := ledgersHandler{
		ledgerReader: db.NewLedgerReader(testDB),
		maxLimit:     100,
		defaultLimit: 5,
	}

	request := GetLedgersRequest{
		StartLedger: 1,
		Format:      FormatJSON,
	}

	response, err := handler.getLedgers(context.TODO(), request)
	require.NoError(t, err)

	assert.NotEmpty(t, response.Ledgers)
	ledger := response.Ledgers[0]

	assert.NotEmpty(t, ledger.LedgerHeaderJSON)
	assert.Empty(t, ledger.LedgerHeader)
	assert.NotEmpty(t, ledger.LedgerCloseMetaJSON)
	assert.Empty(t, ledger.LedgerCloseMeta)

	var headerJSON map[string]interface{}
	err = json.Unmarshal(ledger.LedgerHeaderJSON, &headerJSON)
	require.NoError(t, err)
	assert.NotEmpty(t, headerJSON)

	var metaJSON map[string]interface{}
	err = json.Unmarshal(ledger.LedgerCloseMetaJSON, &metaJSON)
	require.NoError(t, err)
	assert.NotEmpty(t, metaJSON)
}

func TestGetLedgers_NoLedgers(t *testing.T) {
	testDB := setupTestDB(t, 0)
	handler := ledgersHandler{
		ledgerReader: db.NewLedgerReader(testDB),
		maxLimit:     100,
		defaultLimit: 5,
	}

	request := GetLedgersRequest{
		StartLedger: 1,
	}

	_, err := handler.getLedgers(context.TODO(), request)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "[-32603] DB is empty")
}

func TestGetLedgers_CursorGreaterThanLatestLedger(t *testing.T) {
	testDB := setupTestDB(t, 11)
	handler := ledgersHandler{
		ledgerReader: db.NewLedgerReader(testDB),
		maxLimit:     100,
		defaultLimit: 5,
	}

	request := GetLedgersRequest{
		Pagination: &PaginationOptions{
			Cursor: "15",
		},
	}

	_, err := handler.getLedgers(context.TODO(), request)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cursor must be between")
}
