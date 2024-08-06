//nolint:lll
package integrationtest

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/integrationtest/infrastructure"
	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/methods"
)

func TestSimulateTransactionSucceeds(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	contractBinary := infrastructure.GetHelloWorldContract()
	params := infrastructure.CreateTransactionParams(
		test.MasterAccount(),
		infrastructure.CreateUploadWasmOperation(test.MasterAccount().GetAccountID(), contractBinary),
	)
	client := test.GetRPCLient()
	result := infrastructure.SimulateTransactionFromTxParams(t, client, params)

	contractHash := sha256.Sum256(contractBinary)
	contractHashBytes := xdr.ScBytes(contractHash[:])
	expectedXdr := xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &contractHashBytes}
	require.Greater(t, result.LatestLedger, uint32(0))
	require.Greater(t, result.Cost.CPUInstructions, uint64(0))
	require.Greater(t, result.Cost.MemoryBytes, uint64(0))

	expectedTransactionData := xdr.SorobanTransactionData{
		Resources: xdr.SorobanResources{
			Footprint: xdr.LedgerFootprint{
				ReadWrite: []xdr.LedgerKey{
					{
						Type: xdr.LedgerEntryTypeContractCode,
						ContractCode: &xdr.LedgerKeyContractCode{
							Hash: xdr.Hash(contractHash),
						},
					},
				},
			},
			Instructions: 4378462,
			ReadBytes:    0,
			WriteBytes:   7048,
		},
		// the resulting fee is derived from the compute factors and a default padding is applied to instructions by preflight
		// for test purposes, the most deterministic way to require the resulting fee is expected value in test scope, is to capture
		// the resulting fee from current preflight output and re-plug it in here, rather than try to re-implement the cost-model algo
		// in the test.
		ResourceFee: 149755,
	}

	// First, decode and compare the transaction data so we get a decent diff if it fails.
	var transactionData xdr.SorobanTransactionData
	err := xdr.SafeUnmarshalBase64(result.TransactionDataXDR, &transactionData)
	require.NoError(t, err)
	require.Equal(t, expectedTransactionData.Resources.Footprint, transactionData.Resources.Footprint)
	require.InDelta(t, uint32(expectedTransactionData.Resources.Instructions), uint32(transactionData.Resources.Instructions), 3200000)
	require.InDelta(t, uint32(expectedTransactionData.Resources.ReadBytes), uint32(transactionData.Resources.ReadBytes), 10)
	require.InDelta(t, uint32(expectedTransactionData.Resources.WriteBytes), uint32(transactionData.Resources.WriteBytes), 300)
	require.InDelta(t, int64(expectedTransactionData.ResourceFee), int64(transactionData.ResourceFee), 40000)

	// Then decode and check the result xdr, separately so we get a decent diff if it fails.
	require.Len(t, result.Results, 1)
	var resultXdr xdr.ScVal
	err = xdr.SafeUnmarshalBase64(result.Results[0].ReturnValueXDR, &resultXdr)
	require.NoError(t, err)
	require.Equal(t, expectedXdr, resultXdr)

	// Check state diff
	require.Len(t, result.StateChanges, 1)
	require.Nil(t, result.StateChanges[0].BeforeXDR)
	require.NotNil(t, result.StateChanges[0].AfterXDR)
	require.Equal(t, methods.LedgerEntryChangeTypeCreated, result.StateChanges[0].Type)
	var after xdr.LedgerEntry
	require.NoError(t, xdr.SafeUnmarshalBase64(*result.StateChanges[0].AfterXDR, &after))
	require.Equal(t, xdr.LedgerEntryTypeContractCode, after.Data.Type)
	entryKey, err := after.LedgerKey()
	require.NoError(t, err)
	entryKeyB64, err := xdr.MarshalBase64(entryKey)
	require.NoError(t, err)
	require.Equal(t, entryKeyB64, result.StateChanges[0].KeyXDR)

	// test operation which does not have a source account
	params = infrastructure.CreateTransactionParams(test.MasterAccount(),
		infrastructure.CreateUploadWasmOperation("", contractBinary),
	)
	require.NoError(t, err)

	resultForRequestWithoutOpSource := infrastructure.SimulateTransactionFromTxParams(t, client, params)
	// Let's not compare the latest ledger since it may change
	result.LatestLedger = resultForRequestWithoutOpSource.LatestLedger
	require.Equal(t, result, resultForRequestWithoutOpSource)

	// test that operation source account takes precedence over tx source account
	params = infrastructure.CreateTransactionParams(
		&txnbuild.SimpleAccount{
			AccountID: keypair.Root("test passphrase").Address(),
			Sequence:  0,
		},
		infrastructure.CreateUploadWasmOperation("", contractBinary),
	)

	resultForRequestWithDifferentTxSource := infrastructure.SimulateTransactionFromTxParams(t, client, params)
	require.GreaterOrEqual(t, resultForRequestWithDifferentTxSource.LatestLedger, result.LatestLedger)
	// apart from latest ledger the response should be the same
	resultForRequestWithDifferentTxSource.LatestLedger = result.LatestLedger
	require.Equal(t, result, resultForRequestWithDifferentTxSource)
}

func TestSimulateTransactionWithAuth(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	test.UploadHelloWorldContract()

	deployContractOp := infrastructure.CreateCreateHelloWorldContractOperation(test.MasterAccount().GetAccountID())
	deployContractParams := infrastructure.CreateTransactionParams(
		test.MasterAccount(),
		deployContractOp,
	)

	client := test.GetRPCLient()
	response := infrastructure.SimulateTransactionFromTxParams(t, client, deployContractParams)
	require.NotEmpty(t, response.Results)
	require.Len(t, response.Results[0].AuthXDR, 1)
	require.Empty(t, deployContractOp.Auth)

	var auth xdr.SorobanAuthorizationEntry
	require.NoError(t, xdr.SafeUnmarshalBase64(response.Results[0].AuthXDR[0], &auth))
	require.Equal(t, auth.Credentials.Type, xdr.SorobanCredentialsTypeSorobanCredentialsSourceAccount)
	deployContractOp.Auth = append(deployContractOp.Auth, auth)
	deployContractParams.Operations = []txnbuild.Operation{deployContractOp}

	// preflight deployContractOp with auth
	deployContractParams = infrastructure.PreflightTransactionParams(t, client, deployContractParams)
	tx, err := txnbuild.NewTransaction(deployContractParams)
	require.NoError(t, err)
	test.SendMasterTransaction(tx)
}

func TestSimulateInvokeContractTransactionSucceeds(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	_, contractID, contractHash := test.CreateHelloWorldContract()

	contractFnParameterSym := xdr.ScSymbol("world")
	authAddrArg := "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H"
	authAccountIDArg := xdr.MustAddress(authAddrArg)
	test.SendMasterOperation(&txnbuild.CreateAccount{
		Destination:   authAddrArg,
		Amount:        "100000",
		SourceAccount: test.MasterAccount().GetAccountID(),
	})
	params := infrastructure.CreateTransactionParams(
		test.MasterAccount(),
		infrastructure.CreateInvokeHostOperation(
			test.MasterAccount().GetAccountID(),
			contractID,
			"auth",
			xdr.ScVal{
				Type: xdr.ScValTypeScvAddress,
				Address: &xdr.ScAddress{
					Type:      xdr.ScAddressTypeScAddressTypeAccount,
					AccountId: &authAccountIDArg,
				},
			},
			xdr.ScVal{
				Type: xdr.ScValTypeScvSymbol,
				Sym:  &contractFnParameterSym,
			},
		),
	)
	tx, err := txnbuild.NewTransaction(params)
	require.NoError(t, err)

	txB64, err := tx.Base64()
	require.NoError(t, err)

	request := methods.SimulateTransactionRequest{Transaction: txB64}
	var response methods.SimulateTransactionResponse
	err = test.GetRPCLient().CallResult(context.Background(), "simulateTransaction", request, &response)
	require.NoError(t, err)
	require.Empty(t, response.Error)

	// check the result
	require.Len(t, response.Results, 1)
	var obtainedResult xdr.ScVal
	err = xdr.SafeUnmarshalBase64(response.Results[0].ReturnValueXDR, &obtainedResult)
	require.NoError(t, err)
	require.Equal(t, xdr.ScValTypeScvAddress, obtainedResult.Type)
	require.NotNil(t, obtainedResult.Address)
	require.Equal(t, authAccountIDArg, obtainedResult.Address.MustAccountId())

	// check the footprint
	var obtainedTransactionData xdr.SorobanTransactionData
	err = xdr.SafeUnmarshalBase64(response.TransactionDataXDR, &obtainedTransactionData)
	obtainedFootprint := obtainedTransactionData.Resources.Footprint
	require.NoError(t, err)
	require.Len(t, obtainedFootprint.ReadWrite, 1)
	require.Len(t, obtainedFootprint.ReadOnly, 3)
	ro0 := obtainedFootprint.ReadOnly[0]
	require.Equal(t, xdr.LedgerEntryTypeAccount, ro0.Type)
	require.Equal(t, authAddrArg, ro0.Account.AccountId.Address())
	ro1 := obtainedFootprint.ReadOnly[1]
	require.Equal(t, xdr.LedgerEntryTypeContractData, ro1.Type)
	require.Equal(t, xdr.ScAddressTypeScAddressTypeContract, ro1.ContractData.Contract.Type)
	require.Equal(t, xdr.Hash(contractID), *ro1.ContractData.Contract.ContractId)
	require.Equal(t, xdr.ScValTypeScvLedgerKeyContractInstance, ro1.ContractData.Key.Type)
	ro2 := obtainedFootprint.ReadOnly[2]
	require.Equal(t, xdr.LedgerEntryTypeContractCode, ro2.Type)
	require.Equal(t, contractHash, ro2.ContractCode.Hash)
	require.NoError(t, err)

	require.NotZero(t, obtainedTransactionData.ResourceFee)
	require.NotZero(t, obtainedTransactionData.Resources.Instructions)
	require.NotZero(t, obtainedTransactionData.Resources.ReadBytes)
	require.NotZero(t, obtainedTransactionData.Resources.WriteBytes)

	// check the auth
	require.Len(t, response.Results[0].AuthXDR, 1)
	var obtainedAuth xdr.SorobanAuthorizationEntry
	err = xdr.SafeUnmarshalBase64(response.Results[0].AuthXDR[0], &obtainedAuth)
	require.NoError(t, err)
	require.Equal(t, xdr.SorobanCredentialsTypeSorobanCredentialsAddress, obtainedAuth.Credentials.Type)
	require.Equal(t, xdr.ScValTypeScvVoid, obtainedAuth.Credentials.Address.Signature.Type)

	require.NotZero(t, obtainedAuth.Credentials.Address.Nonce)
	require.Equal(t, xdr.ScAddressTypeScAddressTypeAccount, obtainedAuth.Credentials.Address.Address.Type)
	require.Equal(t, authAddrArg, obtainedAuth.Credentials.Address.Address.AccountId.Address())

	require.Equal(t, xdr.SorobanCredentialsTypeSorobanCredentialsAddress, obtainedAuth.Credentials.Type)
	require.Equal(t, xdr.ScAddressTypeScAddressTypeAccount, obtainedAuth.Credentials.Address.Address.Type)
	require.Equal(t, authAddrArg, obtainedAuth.Credentials.Address.Address.AccountId.Address())
	require.Equal(t, xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn, obtainedAuth.RootInvocation.Function.Type)
	require.Equal(t, xdr.ScSymbol("auth"), obtainedAuth.RootInvocation.Function.ContractFn.FunctionName)
	require.Len(t, obtainedAuth.RootInvocation.Function.ContractFn.Args, 2)
	world := obtainedAuth.RootInvocation.Function.ContractFn.Args[1]
	require.Equal(t, xdr.ScValTypeScvSymbol, world.Type)
	require.Equal(t, xdr.ScSymbol("world"), *world.Sym)
	require.Nil(t, obtainedAuth.RootInvocation.SubInvocations)

	// check the events. There will be 2 debug events and the event emitted by the "auth" function
	// which is the one we are going to check.
	require.Len(t, response.EventsXDR, 3)
	var event xdr.DiagnosticEvent
	err = xdr.SafeUnmarshalBase64(response.EventsXDR[1], &event)
	require.NoError(t, err)
	require.True(t, event.InSuccessfulContractCall)
	require.NotNil(t, event.Event.ContractId)
	require.Equal(t, xdr.Hash(contractID), *event.Event.ContractId)
	require.Equal(t, xdr.ContractEventTypeContract, event.Event.Type)
	require.Equal(t, int32(0), event.Event.Body.V)
	require.Equal(t, xdr.ScValTypeScvSymbol, event.Event.Body.V0.Data.Type)
	require.Equal(t, xdr.ScSymbol("world"), *event.Event.Body.V0.Data.Sym)
	require.Len(t, event.Event.Body.V0.Topics, 1)
	require.Equal(t, xdr.ScValTypeScvString, event.Event.Body.V0.Topics[0].Type)
	require.Equal(t, xdr.ScString("auth"), *event.Event.Body.V0.Topics[0].Str)
}

func TestSimulateTransactionError(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	client := test.GetRPCLient()

	invokeHostOp := infrastructure.CreateInvokeHostOperation(
		test.MasterAccount().GetAccountID(),
		xdr.Hash{},
		"noMethod",
	)
	invokeHostOp.HostFunction = xdr.HostFunction{
		Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
		InvokeContract: &xdr.InvokeContractArgs{
			ContractAddress: xdr.ScAddress{
				Type:       xdr.ScAddressTypeScAddressTypeContract,
				ContractId: &xdr.Hash{0x1, 0x2},
			},
			FunctionName: "",
			Args:         nil,
		},
	}
	params := infrastructure.CreateTransactionParams(
		test.MasterAccount(),
		invokeHostOp,
	)
	result := infrastructure.SimulateTransactionFromTxParams(t, client, params)
	require.Greater(t, result.LatestLedger, uint32(0))
	require.Contains(t, result.Error, "MissingValue")
	require.GreaterOrEqual(t, len(result.EventsXDR), 1)
	var event xdr.DiagnosticEvent
	require.NoError(t, xdr.SafeUnmarshalBase64(result.EventsXDR[0], &event))
}

func TestSimulateTransactionMultipleOperations(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	account := test.MasterAccount()
	sourceAccount := account.GetAccountID()
	params := txnbuild.TransactionParams{
		SourceAccount:        account,
		IncrementSequenceNum: false,
		Operations: []txnbuild.Operation{
			infrastructure.CreateUploadHelloWorldOperation(sourceAccount),
			infrastructure.CreateCreateHelloWorldContractOperation(sourceAccount),
		},
		BaseFee: txnbuild.MinBaseFee,
		Memo:    nil,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	}

	client := test.GetRPCLient()
	result := infrastructure.SimulateTransactionFromTxParams(t, client, params)
	require.Equal(
		t,
		methods.SimulateTransactionResponse{
			Error: "Transaction contains more than one operation",
		},
		result,
	)
}

func TestSimulateTransactionWithoutInvokeHostFunction(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	params := infrastructure.CreateTransactionParams(
		test.MasterAccount(),
		&txnbuild.BumpSequence{BumpTo: 1},
	)

	client := test.GetRPCLient()
	result := infrastructure.SimulateTransactionFromTxParams(t, client, params)
	require.Equal(
		t,
		methods.SimulateTransactionResponse{
			Error: "Transaction contains unsupported operation type: OperationTypeBumpSequence",
		},
		result,
	)
}

func TestSimulateTransactionUnmarshalError(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	client := test.GetRPCLient()

	request := methods.SimulateTransactionRequest{Transaction: "invalid"}
	var result methods.SimulateTransactionResponse
	err := client.CallResult(context.Background(), "simulateTransaction", request, &result)
	require.NoError(t, err)
	require.Equal(
		t,
		"Could not unmarshal transaction",
		result.Error,
	)
}

func TestSimulateTransactionExtendAndRestoreFootprint(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	_, contractID, _ := test.CreateHelloWorldContract()
	test.InvokeHostFunc(
		contractID,
		"inc",
	)

	// get the counter ledger entry TTL
	key := getCounterLedgerKey(contractID)

	keyB64, err := xdr.MarshalBase64(key)
	require.NoError(t, err)
	getLedgerEntryrequest := methods.GetLedgerEntryRequest{
		Key: keyB64,
	}
	var getLedgerEntryResult methods.GetLedgerEntryResponse
	client := test.GetRPCLient()
	err = client.CallResult(context.Background(), "getLedgerEntry", getLedgerEntryrequest, &getLedgerEntryResult)
	require.NoError(t, err)

	var entry xdr.LedgerEntryData
	require.NoError(t, xdr.SafeUnmarshalBase64(getLedgerEntryResult.EntryXDR, &entry))
	require.Equal(t, xdr.LedgerEntryTypeContractData, entry.Type)
	require.NotNil(t, getLedgerEntryResult.LiveUntilLedgerSeq)

	initialLiveUntil := *getLedgerEntryResult.LiveUntilLedgerSeq

	// Extend the initial TTL
	test.PreflightAndSendMasterOperation(&txnbuild.ExtendFootprintTtl{
		ExtendTo: 20,
		Ext: xdr.TransactionExt{
			V: 1,
			SorobanData: &xdr.SorobanTransactionData{
				Resources: xdr.SorobanResources{
					Footprint: xdr.LedgerFootprint{
						ReadOnly: []xdr.LedgerKey{key},
					},
				},
			},
		},
	},
	)

	err = client.CallResult(context.Background(), "getLedgerEntry", getLedgerEntryrequest, &getLedgerEntryResult)
	require.NoError(t, err)
	require.NoError(t, xdr.SafeUnmarshalBase64(getLedgerEntryResult.EntryXDR, &entry))
	require.Equal(t, xdr.LedgerEntryTypeContractData, entry.Type)
	require.NotNil(t, getLedgerEntryResult.LiveUntilLedgerSeq)
	newLiveUntilSeq := *getLedgerEntryResult.LiveUntilLedgerSeq
	require.Greater(t, newLiveUntilSeq, initialLiveUntil)

	// Wait until it is not live anymore
	waitUntilLedgerEntryTTL(t, client, key)

	// and restore it
	test.PreflightAndSendMasterOperation(
		&txnbuild.RestoreFootprint{
			Ext: xdr.TransactionExt{
				V: 1,
				SorobanData: &xdr.SorobanTransactionData{
					Resources: xdr.SorobanResources{
						Footprint: xdr.LedgerFootprint{
							ReadWrite: []xdr.LedgerKey{key},
						},
					},
				},
			},
		},
	)

	// Wait for TTL again and check the pre-restore field when trying to exec the contract again
	waitUntilLedgerEntryTTL(t, client, key)

	invokeIncPresistentEntryParams := infrastructure.CreateTransactionParams(
		test.MasterAccount(),
		infrastructure.CreateInvokeHostOperation(test.MasterAccount().GetAccountID(), contractID, "inc"),
	)
	simulationResult := infrastructure.SimulateTransactionFromTxParams(t, client, invokeIncPresistentEntryParams)
	require.NotNil(t, simulationResult.RestorePreamble)
	require.NotZero(t, simulationResult.RestorePreamble)

	params := infrastructure.PreflightTransactionParamsLocally(
		t,
		infrastructure.CreateTransactionParams(
			test.MasterAccount(),
			&txnbuild.RestoreFootprint{},
		),
		methods.SimulateTransactionResponse{
			TransactionDataXDR: simulationResult.RestorePreamble.TransactionDataXDR,
			MinResourceFee:     simulationResult.RestorePreamble.MinResourceFee,
		},
	)
	tx, err := txnbuild.NewTransaction(params)
	require.NoError(t, err)
	test.SendMasterTransaction(tx)

	// Finally, we should be able to send the inc host function invocation now that we
	// have pre-restored the entries
	params = infrastructure.PreflightTransactionParamsLocally(t, invokeIncPresistentEntryParams, simulationResult)
	tx, err = txnbuild.NewTransaction(params)
	require.NoError(t, err)
	test.SendMasterTransaction(tx)
}

func getCounterLedgerKey(contractID [32]byte) xdr.LedgerKey {
	contractIDHash := xdr.Hash(contractID)
	counterSym := xdr.ScSymbol("COUNTER")
	key := xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeContractData,
		ContractData: &xdr.LedgerKeyContractData{
			Contract: xdr.ScAddress{
				Type:       xdr.ScAddressTypeScAddressTypeContract,
				ContractId: &contractIDHash,
			},
			Key: xdr.ScVal{
				Type: xdr.ScValTypeScvSymbol,
				Sym:  &counterSym,
			},
			Durability: xdr.ContractDataDurabilityPersistent,
		},
	}
	return key
}

func waitUntilLedgerEntryTTL(t *testing.T, client *infrastructure.Client, ledgerKey xdr.LedgerKey) {
	keyB64, err := xdr.MarshalBase64(ledgerKey)
	require.NoError(t, err)
	request := methods.GetLedgerEntriesRequest{
		Keys: []string{keyB64},
	}
	ttled := false
	for i := 0; i < 50; i++ {
		var result methods.GetLedgerEntriesResponse
		var entry xdr.LedgerEntryData
		err := client.CallResult(context.Background(), "getLedgerEntries", request, &result)
		require.NoError(t, err)
		require.NotEmpty(t, result.Entries)
		require.NoError(t, xdr.SafeUnmarshalBase64(result.Entries[0].DataXDR, &entry))
		require.NotEqual(t, xdr.LedgerEntryTypeTtl, entry.Type)
		liveUntilLedgerSeq := xdr.Uint32(*result.Entries[0].LiveUntilLedgerSeq)
		// See https://soroban.stellar.org/docs/fundamentals-and-concepts/state-expiration#expiration-ledger
		currentLedger := result.LatestLedger + 1
		if xdr.Uint32(currentLedger) > liveUntilLedgerSeq {
			ttled = true
			t.Logf("ledger entry ttl'ed")
			break
		}
		t.Log("waiting for ledger entry to ttl at ledger", liveUntilLedgerSeq)
		time.Sleep(time.Second)
	}
	require.True(t, ttled)
}

func TestSimulateInvokePrng_u64_in_range(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	_, contractID, _ := test.CreateHelloWorldContract()

	authAddrArg := "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H"
	test.SendMasterOperation(
		&txnbuild.CreateAccount{
			Destination:   authAddrArg,
			Amount:        "100000",
			SourceAccount: test.MasterAccount().GetAccountID(),
		},
	)
	low := xdr.Uint64(1500)
	high := xdr.Uint64(10000)
	params := infrastructure.CreateTransactionParams(
		test.MasterAccount(),
		infrastructure.CreateInvokeHostOperation(
			test.MasterAccount().GetAccountID(),
			contractID,
			"prng_u64_in_range",
			xdr.ScVal{
				Type: xdr.ScValTypeScvU64,
				U64:  &low,
			},
			xdr.ScVal{
				Type: xdr.ScValTypeScvU64,
				U64:  &high,
			},
		),
	)

	tx, err := txnbuild.NewTransaction(params)
	require.NoError(t, err)
	txB64, err := tx.Base64()
	require.NoError(t, err)

	request := methods.SimulateTransactionRequest{Transaction: txB64}
	var response methods.SimulateTransactionResponse
	err = test.GetRPCLient().CallResult(context.Background(), "simulateTransaction", request, &response)
	require.NoError(t, err)
	require.Empty(t, response.Error)

	// check the result
	require.Len(t, response.Results, 1)
	var obtainedResult xdr.ScVal
	err = xdr.SafeUnmarshalBase64(response.Results[0].ReturnValueXDR, &obtainedResult)
	require.NoError(t, err)
	require.Equal(t, xdr.ScValTypeScvU64, obtainedResult.Type)
	require.LessOrEqual(t, uint64(*obtainedResult.U64), uint64(high))
	require.GreaterOrEqual(t, uint64(*obtainedResult.U64), uint64(low))
}

func TestSimulateSystemEvent(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	_, contractID, contractHash := test.CreateHelloWorldContract()
	authAddrArg := "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H"
	test.SendMasterOperation(
		&txnbuild.CreateAccount{
			Destination:   authAddrArg,
			Amount:        "100000",
			SourceAccount: test.MasterAccount().GetAccountID(),
		},
	)

	byteSlice := xdr.ScBytes(contractHash[:])

	params := infrastructure.CreateTransactionParams(
		test.MasterAccount(),
		infrastructure.CreateInvokeHostOperation(
			test.MasterAccount().GetAccountID(),
			contractID,
			"upgrade_contract",
			xdr.ScVal{
				Type:  xdr.ScValTypeScvBytes,
				Bytes: &byteSlice,
			},
		),
	)
	tx, err := txnbuild.NewTransaction(params)
	require.NoError(t, err)
	txB64, err := tx.Base64()
	require.NoError(t, err)

	request := methods.SimulateTransactionRequest{Transaction: txB64}
	var response methods.SimulateTransactionResponse
	err = test.GetRPCLient().CallResult(context.Background(), "simulateTransaction", request, &response)
	require.NoError(t, err)
	require.Empty(t, response.Error)

	// check the result
	require.Len(t, response.Results, 1)
	var obtainedResult xdr.ScVal
	err = xdr.SafeUnmarshalBase64(response.Results[0].ReturnValueXDR, &obtainedResult)
	require.NoError(t, err)

	var transactionData xdr.SorobanTransactionData
	err = xdr.SafeUnmarshalBase64(response.TransactionDataXDR, &transactionData)
	require.NoError(t, err)
	require.InDelta(t, 6856, uint32(transactionData.Resources.ReadBytes), 200)

	// the resulting fee is derived from compute factors and a default padding is applied to instructions by preflight
	// for test purposes, the most deterministic way to require the resulting fee is expected value in test scope, is to capture
	// the resulting fee from current preflight output and re-plug it in here, rather than try to re-implement the cost-model algo
	// in the test.
	require.InDelta(t, 70668, int64(transactionData.ResourceFee), 20000)
	require.InDelta(t, 104, uint32(transactionData.Resources.WriteBytes), 15)
	require.GreaterOrEqual(t, len(response.EventsXDR), 3)
}
