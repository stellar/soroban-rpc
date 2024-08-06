package integrationtest

import (
	"context"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go/xdr"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/integrationtest/infrastructure"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/methods"
)

func TestGetLedgerEntryNotFound(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	contractIDHash := xdr.Hash{0x1, 0x2}
	keyB64, err := xdr.MarshalBase64(xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeContractData,
		ContractData: &xdr.LedgerKeyContractData{
			Contract: xdr.ScAddress{
				Type:       xdr.ScAddressTypeScAddressTypeContract,
				ContractId: &contractIDHash,
			},
			Key: xdr.ScVal{
				Type: xdr.ScValTypeScvLedgerKeyContractInstance,
			},
			Durability: xdr.ContractDataDurabilityPersistent,
		},
	})
	require.NoError(t, err)
	request := methods.GetLedgerEntryRequest{
		Key: keyB64,
	}

	var result methods.GetLedgerEntryResponse
	client := test.GetRPCLient()
	jsonRPCErr := client.CallResult(context.Background(), "getLedgerEntry", request, &result).(*jrpc2.Error)
	require.Contains(t, jsonRPCErr.Message, "not found")
	require.Equal(t, jrpc2.InvalidRequest, jsonRPCErr.Code)
}

func TestGetLedgerEntryInvalidParams(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	client := test.GetRPCLient()

	request := methods.GetLedgerEntryRequest{
		Key: "<>@@#$",
	}

	var result methods.GetLedgerEntryResponse
	jsonRPCErr := client.CallResult(context.Background(), "getLedgerEntry", request, &result).(*jrpc2.Error)
	require.Equal(t, "cannot unmarshal key value", jsonRPCErr.Message)
	require.Equal(t, jrpc2.InvalidParams, jsonRPCErr.Code)
}

func TestGetLedgerEntrySucceeds(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	_, contractHash := test.UploadHelloWorldContract()

	keyB64, err := xdr.MarshalBase64(xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeContractCode,
		ContractCode: &xdr.LedgerKeyContractCode{
			Hash: contractHash,
		},
	})
	require.NoError(t, err)
	request := methods.GetLedgerEntryRequest{
		Key: keyB64,
	}

	var result methods.GetLedgerEntryResponse
	err = test.GetRPCLient().CallResult(context.Background(), "getLedgerEntry", request, &result)
	require.NoError(t, err)
	require.Greater(t, result.LatestLedger, uint32(0))
	require.GreaterOrEqual(t, result.LatestLedger, result.LastModifiedLedger)
	var entry xdr.LedgerEntryData
	require.NoError(t, xdr.SafeUnmarshalBase64(result.EntryXDR, &entry))
	require.Equal(t, infrastructure.GetHelloWorldContract(), entry.MustContractCode().Code)
}
