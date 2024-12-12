package methods

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
)

var expectedResponse = GetLatestLedgerResponse{
	ProtocolVersion: uint32(0),
	LedgerInfo: LedgerInfo{
		Hash:            "0000000000000000000000000000000000000000000000000000000000000000",
		Sequence:        5,
		LedgerCloseTime: 225,
		LedgerHeader:    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADhAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",                                                                                                                                                                                                                                                                                                                                                         //nolint:lll
		LedgerMetadata:  "AAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAOEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAEAAAACAAABAIAAAAAAAAAAPww0v5OtDZlx0EzMkPcFURyDiq2XNKSi+w16A/x/6JoAAAABAAAAAP///6EAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAE/FqKJRRfCYUt0glxgPMqrA9VV2uKo7gsQDTTGLdLSYgAAAAAAAABkAAAAAAAAAAAAAAAAAAAAAAAAAAMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==", //nolint:lll
	},
}

func TestGetLatestLedger_DefaultRequest(t *testing.T) {
	testDB := setupTestDB(t, 5)
	request := GetLatestLedgerRequest{
		Format: FormatBase64,
	}

	handler := latestLedgerHandler{
		ledgerReader:      db.NewLedgerReader(testDB),
		ledgerEntryReader: db.NewLedgerEntryReader(testDB),
	}

	resp, err := handler.getLatestLedger(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, expectedResponse, resp)
}

func TestGetLatestLedger_JSONFormat(t *testing.T) {
	testDB := setupTestDB(t, 5)
	request := GetLatestLedgerRequest{
		Format: FormatJSON,
	}

	handler := latestLedgerHandler{
		ledgerReader:      db.NewLedgerReader(testDB),
		ledgerEntryReader: db.NewLedgerEntryReader(testDB),
	}

	resp, err := handler.getLatestLedger(context.Background(), request)
	require.NoError(t, err)

	assert.NotEmpty(t, resp.LedgerHeaderJSON)
	assert.Empty(t, resp.LedgerHeader)
	assert.NotEmpty(t, resp.LedgerMetadataJSON)
	assert.Empty(t, resp.LedgerMetadata)

	var headerJSON map[string]interface{}
	err = json.Unmarshal(resp.LedgerHeaderJSON, &headerJSON)
	require.NoError(t, err)
	assert.NotEmpty(t, headerJSON)

	var metaJSON map[string]interface{}
	err = json.Unmarshal(resp.LedgerMetadataJSON, &metaJSON)
	require.NoError(t, err)
	assert.NotEmpty(t, metaJSON)
}
