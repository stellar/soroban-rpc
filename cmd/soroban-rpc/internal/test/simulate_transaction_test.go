package test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path"
	"runtime"
	"testing"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/jhttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/methods"
)

var (
	testSalt = sha256.Sum256([]byte("a1"))
)

func getHelloWorldContract(t *testing.T) []byte {
	_, filename, _, _ := runtime.Caller(0)
	testDirName := path.Dir(filename)
	contractFile := path.Join(testDirName, helloWorldContractPath)
	ret, err := os.ReadFile(contractFile)
	if err != nil {
		t.Fatalf("unable to read test_hello_world.wasm (%v) please run `make build-test-wasms` at the project root directory", err)
	}
	return ret
}

func createInvokeHostOperation(sourceAccount string, contractID xdr.Hash, method string, args ...xdr.ScVal) *txnbuild.InvokeHostFunction {
	return &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
			InvokeContract: &xdr.InvokeContractArgs{
				ContractAddress: xdr.ScAddress{
					Type:       xdr.ScAddressTypeScAddressTypeContract,
					ContractId: &contractID,
				},
				FunctionName: xdr.ScSymbol(method),
				Args:         args,
			},
		},
		Auth:          nil,
		SourceAccount: sourceAccount,
	}
}

func createInstallContractCodeOperation(sourceAccount string, contractCode []byte) *txnbuild.InvokeHostFunction {
	return &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm,
			Wasm: &contractCode,
		},
		SourceAccount: sourceAccount,
	}
}

func createCreateContractOperation(sourceAccount string, contractCode []byte) *txnbuild.InvokeHostFunction {
	saltParam := xdr.Uint256(testSalt)
	contractHash := xdr.Hash(sha256.Sum256(contractCode))

	sourceAccountID := xdr.MustAddress(sourceAccount)
	return &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeCreateContract,
			CreateContract: &xdr.CreateContractArgs{
				ContractIdPreimage: xdr.ContractIdPreimage{
					Type: xdr.ContractIdPreimageTypeContractIdPreimageFromAddress,
					FromAddress: &xdr.ContractIdPreimageFromAddress{
						Address: xdr.ScAddress{
							Type:      xdr.ScAddressTypeScAddressTypeAccount,
							AccountId: &sourceAccountID,
						},
						Salt: saltParam,
					},
				},
				Executable: xdr.ContractExecutable{
					Type:     xdr.ContractExecutableTypeContractExecutableWasm,
					WasmHash: &contractHash,
				},
			},
		},
		Auth:          []xdr.SorobanAuthorizationEntry{},
		SourceAccount: sourceAccount,
	}
}

func getContractID(t *testing.T, sourceAccount string, salt [32]byte, networkPassphrase string) [32]byte {
	sourceAccountID := xdr.MustAddress(sourceAccount)
	preImage := xdr.HashIdPreimage{
		Type: xdr.EnvelopeTypeEnvelopeTypeContractId,
		ContractId: &xdr.HashIdPreimageContractId{
			NetworkId: sha256.Sum256([]byte(networkPassphrase)),
			ContractIdPreimage: xdr.ContractIdPreimage{
				Type: xdr.ContractIdPreimageTypeContractIdPreimageFromAddress,
				FromAddress: &xdr.ContractIdPreimageFromAddress{
					Address: xdr.ScAddress{
						Type:      xdr.ScAddressTypeScAddressTypeAccount,
						AccountId: &sourceAccountID,
					},
					Salt: salt,
				},
			},
		},
	}

	xdrPreImageBytes, err := preImage.MarshalBinary()
	require.NoError(t, err)
	hashedContractID := sha256.Sum256(xdrPreImageBytes)
	return hashedContractID
}

func simulateTransactionFromTxParams(t *testing.T, client *jrpc2.Client, params txnbuild.TransactionParams) methods.SimulateTransactionResponse {
	savedAutoIncrement := params.IncrementSequenceNum
	params.IncrementSequenceNum = false
	tx, err := txnbuild.NewTransaction(params)
	assert.NoError(t, err)
	params.IncrementSequenceNum = savedAutoIncrement
	txB64, err := tx.Base64()
	assert.NoError(t, err)
	request := methods.SimulateTransactionRequest{Transaction: txB64}
	var response methods.SimulateTransactionResponse
	err = client.CallResult(context.Background(), "simulateTransaction", request, &response)
	assert.NoError(t, err)
	return response
}

func preflightTransactionParamsLocally(t *testing.T, params txnbuild.TransactionParams, response methods.SimulateTransactionResponse) txnbuild.TransactionParams {
	if !assert.Empty(t, response.Error) {
		fmt.Println(response.Error)
	}
	var transactionData xdr.SorobanTransactionData
	err := xdr.SafeUnmarshalBase64(response.TransactionData, &transactionData)
	require.NoError(t, err)

	op := params.Operations[0]
	switch v := op.(type) {
	case *txnbuild.InvokeHostFunction:
		require.Len(t, response.Results, 1)
		v.Ext = xdr.TransactionExt{
			V:           1,
			SorobanData: &transactionData,
		}
		var auth []xdr.SorobanAuthorizationEntry
		for _, b64 := range response.Results[0].Auth {
			var a xdr.SorobanAuthorizationEntry
			err := xdr.SafeUnmarshalBase64(b64, &a)
			assert.NoError(t, err)
			auth = append(auth, a)
		}
		v.Auth = auth
	case *txnbuild.ExtendFootprintTtl:
		require.Len(t, response.Results, 0)
		v.Ext = xdr.TransactionExt{
			V:           1,
			SorobanData: &transactionData,
		}
	case *txnbuild.RestoreFootprint:
		require.Len(t, response.Results, 0)
		v.Ext = xdr.TransactionExt{
			V:           1,
			SorobanData: &transactionData,
		}
	default:
		t.Fatalf("Wrong operation type %v", op)
	}

	params.Operations = []txnbuild.Operation{op}

	params.BaseFee += response.MinResourceFee
	return params
}

func preflightTransactionParams(t *testing.T, client *jrpc2.Client, params txnbuild.TransactionParams) txnbuild.TransactionParams {
	response := simulateTransactionFromTxParams(t, client, params)
	// The preamble should be zero except for the special restore case
	assert.Nil(t, response.RestorePreamble)
	return preflightTransactionParamsLocally(t, params, response)
}

func TestSimulateTransactionSucceeds(t *testing.T) {
	test := NewTest(t, nil)

	ch := jhttp.NewChannel(test.sorobanRPCURL(), nil)
	client := jrpc2.NewClient(ch, nil)

	sourceAccount := keypair.Root(StandaloneNetworkPassphrase).Address()
	contractBinary := getHelloWorldContract(t)
	params := txnbuild.TransactionParams{
		SourceAccount: &txnbuild.SimpleAccount{
			AccountID: sourceAccount,
			Sequence:  0,
		},
		IncrementSequenceNum: false,
		Operations: []txnbuild.Operation{
			createInstallContractCodeOperation(sourceAccount, contractBinary),
		},
		BaseFee: txnbuild.MinBaseFee,
		Memo:    nil,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	}
	result := simulateTransactionFromTxParams(t, client, params)

	contractHash := sha256.Sum256(contractBinary)
	contractHashBytes := xdr.ScBytes(contractHash[:])
	expectedXdr := xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &contractHashBytes}
	assert.Greater(t, result.LatestLedger, uint32(0))
	assert.Greater(t, result.Cost.CPUInstructions, uint64(0))
	assert.Greater(t, result.Cost.MemoryBytes, uint64(0))

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
		// for test purposes, the most deterministic way to assert the resulting fee is expected value in test scope, is to capture
		// the resulting fee from current preflight output and re-plug it in here, rather than try to re-implement the cost-model algo
		// in the test.
		ResourceFee: 118357,
	}

	// First, decode and compare the transaction data so we get a decent diff if it fails.
	var transactionData xdr.SorobanTransactionData
	err := xdr.SafeUnmarshalBase64(result.TransactionData, &transactionData)
	assert.NoError(t, err)
	assert.Equal(t, expectedTransactionData.Resources.Footprint, transactionData.Resources.Footprint)
	assert.InDelta(t, uint32(expectedTransactionData.Resources.Instructions), uint32(transactionData.Resources.Instructions), 3200000)
	assert.InDelta(t, uint32(expectedTransactionData.Resources.ReadBytes), uint32(transactionData.Resources.ReadBytes), 10)
	assert.InDelta(t, uint32(expectedTransactionData.Resources.WriteBytes), uint32(transactionData.Resources.WriteBytes), 300)
	assert.InDelta(t, int64(expectedTransactionData.ResourceFee), int64(transactionData.ResourceFee), 4000)

	// Then decode and check the result xdr, separately so we get a decent diff if it fails.
	assert.Len(t, result.Results, 1)
	var resultXdr xdr.ScVal
	err = xdr.SafeUnmarshalBase64(result.Results[0].XDR, &resultXdr)
	assert.NoError(t, err)
	assert.Equal(t, expectedXdr, resultXdr)

	// Check state diff
	assert.Len(t, result.StateChanges, 1)
	assert.Nil(t, result.StateChanges[0].Before)
	assert.NotNil(t, result.StateChanges[0].After)
	assert.Equal(t, methods.LedgerEntryChangeTypeCreated, result.StateChanges[0].Type)
	var after xdr.LedgerEntry
	assert.NoError(t, xdr.SafeUnmarshalBase64(*result.StateChanges[0].After, &after))
	assert.Equal(t, xdr.LedgerEntryTypeContractCode, after.Data.Type)
	entryKey, err := after.LedgerKey()
	assert.NoError(t, err)
	entryKeyB64, err := xdr.MarshalBase64(entryKey)
	assert.NoError(t, err)
	assert.Equal(t, entryKeyB64, result.StateChanges[0].Key)

	// test operation which does not have a source account
	withoutSourceAccountOp := createInstallContractCodeOperation("", contractBinary)
	params = txnbuild.TransactionParams{
		SourceAccount: &txnbuild.SimpleAccount{
			AccountID: sourceAccount,
			Sequence:  0,
		},
		IncrementSequenceNum: false,
		Operations:           []txnbuild.Operation{withoutSourceAccountOp},
		BaseFee:              txnbuild.MinBaseFee,
		Memo:                 nil,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	}
	require.NoError(t, err)

	resultForRequestWithoutOpSource := simulateTransactionFromTxParams(t, client, params)
	// Let's not compare the latest ledger since it may change
	result.LatestLedger = resultForRequestWithoutOpSource.LatestLedger
	assert.Equal(t, result, resultForRequestWithoutOpSource)

	// test that operation source account takes precedence over tx source account
	params = txnbuild.TransactionParams{
		SourceAccount: &txnbuild.SimpleAccount{
			AccountID: keypair.Root("test passphrase").Address(),
			Sequence:  0,
		},
		IncrementSequenceNum: false,
		Operations: []txnbuild.Operation{
			createInstallContractCodeOperation(sourceAccount, contractBinary),
		},
		BaseFee: txnbuild.MinBaseFee,
		Memo:    nil,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	}

	resultForRequestWithDifferentTxSource := simulateTransactionFromTxParams(t, client, params)
	assert.GreaterOrEqual(t, resultForRequestWithDifferentTxSource.LatestLedger, result.LatestLedger)
	// apart from latest ledger the response should be the same
	resultForRequestWithDifferentTxSource.LatestLedger = result.LatestLedger
	assert.Equal(t, result, resultForRequestWithDifferentTxSource)
}

func TestSimulateTransactionWithAuth(t *testing.T) {
	test := NewTest(t, nil)

	ch := jhttp.NewChannel(test.sorobanRPCURL(), nil)
	client := jrpc2.NewClient(ch, nil)

	sourceAccount := keypair.Root(StandaloneNetworkPassphrase)
	address := sourceAccount.Address()
	account := txnbuild.NewSimpleAccount(address, 0)

	helloWorldContract := getHelloWorldContract(t)

	params := preflightTransactionParams(t, client, txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{
			createInstallContractCodeOperation(account.AccountID, helloWorldContract),
		},
		BaseFee: txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	})

	tx, err := txnbuild.NewTransaction(params)
	assert.NoError(t, err)
	sendSuccessfulTransaction(t, client, sourceAccount, tx)

	deployContractOp := createCreateContractOperation(address, helloWorldContract)
	deployContractParams := txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{
			deployContractOp,
		},
		BaseFee: txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	}
	response := simulateTransactionFromTxParams(t, client, deployContractParams)
	require.NotEmpty(t, response.Results)
	require.Len(t, response.Results[0].Auth, 1)
	require.Empty(t, deployContractOp.Auth)

	var auth xdr.SorobanAuthorizationEntry
	assert.NoError(t, xdr.SafeUnmarshalBase64(response.Results[0].Auth[0], &auth))
	require.Equal(t, auth.Credentials.Type, xdr.SorobanCredentialsTypeSorobanCredentialsSourceAccount)
	deployContractOp.Auth = append(deployContractOp.Auth, auth)
	deployContractParams.Operations = []txnbuild.Operation{deployContractOp}

	// preflight deployContractOp with auth
	deployContractParams = preflightTransactionParams(t, client, deployContractParams)
	tx, err = txnbuild.NewTransaction(deployContractParams)
	assert.NoError(t, err)
	sendSuccessfulTransaction(t, client, sourceAccount, tx)
}

func TestSimulateInvokeContractTransactionSucceeds(t *testing.T) {
	test := NewTest(t, nil)

	ch := jhttp.NewChannel(test.sorobanRPCURL(), nil)
	client := jrpc2.NewClient(ch, nil)

	sourceAccount := keypair.Root(StandaloneNetworkPassphrase)
	address := sourceAccount.Address()
	account := txnbuild.NewSimpleAccount(address, 0)

	helloWorldContract := getHelloWorldContract(t)

	params := preflightTransactionParams(t, client, txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{
			createInstallContractCodeOperation(account.AccountID, helloWorldContract),
		},
		BaseFee: txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	})

	tx, err := txnbuild.NewTransaction(params)
	assert.NoError(t, err)
	sendSuccessfulTransaction(t, client, sourceAccount, tx)

	params = preflightTransactionParams(t, client, txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{
			createCreateContractOperation(address, helloWorldContract),
		},
		BaseFee: txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	})

	tx, err = txnbuild.NewTransaction(params)
	assert.NoError(t, err)
	sendSuccessfulTransaction(t, client, sourceAccount, tx)

	contractID := getContractID(t, address, testSalt, StandaloneNetworkPassphrase)
	contractFnParameterSym := xdr.ScSymbol("world")
	authAddrArg := "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H"
	authAccountIDArg := xdr.MustAddress(authAddrArg)
	tx, err = txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{
			&txnbuild.CreateAccount{
				Destination:   authAddrArg,
				Amount:        "100000",
				SourceAccount: address,
			},
		},
		BaseFee: txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	})
	assert.NoError(t, err)
	sendSuccessfulTransaction(t, client, sourceAccount, tx)
	params = txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: false,
		Operations: []txnbuild.Operation{
			createInvokeHostOperation(
				address,
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
		},
		BaseFee: txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	}
	tx, err = txnbuild.NewTransaction(params)

	assert.NoError(t, err)

	txB64, err := tx.Base64()
	assert.NoError(t, err)

	request := methods.SimulateTransactionRequest{Transaction: txB64}
	var response methods.SimulateTransactionResponse
	err = client.CallResult(context.Background(), "simulateTransaction", request, &response)
	assert.NoError(t, err)
	assert.Empty(t, response.Error)

	// check the result
	assert.Len(t, response.Results, 1)
	var obtainedResult xdr.ScVal
	err = xdr.SafeUnmarshalBase64(response.Results[0].XDR, &obtainedResult)
	assert.NoError(t, err)
	assert.Equal(t, xdr.ScValTypeScvAddress, obtainedResult.Type)
	require.NotNil(t, obtainedResult.Address)
	assert.Equal(t, authAccountIDArg, obtainedResult.Address.MustAccountId())

	// check the footprint
	var obtainedTransactionData xdr.SorobanTransactionData
	err = xdr.SafeUnmarshalBase64(response.TransactionData, &obtainedTransactionData)
	obtainedFootprint := obtainedTransactionData.Resources.Footprint
	assert.NoError(t, err)
	assert.Len(t, obtainedFootprint.ReadWrite, 1)
	assert.Len(t, obtainedFootprint.ReadOnly, 3)
	ro0 := obtainedFootprint.ReadOnly[0]
	assert.Equal(t, xdr.LedgerEntryTypeAccount, ro0.Type)
	assert.Equal(t, authAddrArg, ro0.Account.AccountId.Address())
	ro1 := obtainedFootprint.ReadOnly[1]
	assert.Equal(t, xdr.LedgerEntryTypeContractData, ro1.Type)
	assert.Equal(t, xdr.ScAddressTypeScAddressTypeContract, ro1.ContractData.Contract.Type)
	assert.Equal(t, xdr.Hash(contractID), *ro1.ContractData.Contract.ContractId)
	assert.Equal(t, xdr.ScValTypeScvLedgerKeyContractInstance, ro1.ContractData.Key.Type)
	ro2 := obtainedFootprint.ReadOnly[2]
	assert.Equal(t, xdr.LedgerEntryTypeContractCode, ro2.Type)
	contractHash := sha256.Sum256(helloWorldContract)
	assert.Equal(t, xdr.Hash(contractHash), ro2.ContractCode.Hash)
	assert.NoError(t, err)

	assert.NotZero(t, obtainedTransactionData.ResourceFee)
	assert.NotZero(t, obtainedTransactionData.Resources.Instructions)
	assert.NotZero(t, obtainedTransactionData.Resources.ReadBytes)
	assert.NotZero(t, obtainedTransactionData.Resources.WriteBytes)

	// check the auth
	assert.Len(t, response.Results[0].Auth, 1)
	var obtainedAuth xdr.SorobanAuthorizationEntry
	err = xdr.SafeUnmarshalBase64(response.Results[0].Auth[0], &obtainedAuth)
	assert.NoError(t, err)
	assert.Equal(t, obtainedAuth.Credentials.Type, xdr.SorobanCredentialsTypeSorobanCredentialsAddress)
	assert.Equal(t, obtainedAuth.Credentials.Address.Signature.Type, xdr.ScValTypeScvVoid)

	assert.NotZero(t, obtainedAuth.Credentials.Address.Nonce)
	assert.Equal(t, xdr.ScAddressTypeScAddressTypeAccount, obtainedAuth.Credentials.Address.Address.Type)
	assert.Equal(t, authAddrArg, obtainedAuth.Credentials.Address.Address.AccountId.Address())

	assert.Equal(t, xdr.SorobanCredentialsTypeSorobanCredentialsAddress, obtainedAuth.Credentials.Type)
	assert.Equal(t, xdr.ScAddressTypeScAddressTypeAccount, obtainedAuth.Credentials.Address.Address.Type)
	assert.Equal(t, authAddrArg, obtainedAuth.Credentials.Address.Address.AccountId.Address())
	assert.Equal(t, xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn, obtainedAuth.RootInvocation.Function.Type)
	assert.Equal(t, xdr.ScSymbol("auth"), obtainedAuth.RootInvocation.Function.ContractFn.FunctionName)
	assert.Len(t, obtainedAuth.RootInvocation.Function.ContractFn.Args, 2)
	world := obtainedAuth.RootInvocation.Function.ContractFn.Args[1]
	assert.Equal(t, xdr.ScValTypeScvSymbol, world.Type)
	assert.Equal(t, xdr.ScSymbol("world"), *world.Sym)
	assert.Nil(t, obtainedAuth.RootInvocation.SubInvocations)

	// check the events. There will be 2 debug events and the event emitted by the "auth" function
	// which is the one we are going to check.
	assert.Len(t, response.Events, 3)
	var event xdr.DiagnosticEvent
	err = xdr.SafeUnmarshalBase64(response.Events[1], &event)
	assert.NoError(t, err)
	assert.True(t, event.InSuccessfulContractCall)
	assert.NotNil(t, event.Event.ContractId)
	assert.Equal(t, xdr.Hash(contractID), *event.Event.ContractId)
	assert.Equal(t, xdr.ContractEventTypeContract, event.Event.Type)
	assert.Equal(t, int32(0), event.Event.Body.V)
	assert.Equal(t, xdr.ScValTypeScvSymbol, event.Event.Body.V0.Data.Type)
	assert.Equal(t, xdr.ScSymbol("world"), *event.Event.Body.V0.Data.Sym)
	assert.Len(t, event.Event.Body.V0.Topics, 1)
	assert.Equal(t, xdr.ScValTypeScvString, event.Event.Body.V0.Topics[0].Type)
	assert.Equal(t, xdr.ScString("auth"), *event.Event.Body.V0.Topics[0].Str)
	metrics := getMetrics(test)
	require.Contains(t, metrics, "soroban_rpc_json_rpc_request_duration_seconds_count{endpoint=\"simulateTransaction\",status=\"ok\"} 3")
	require.Contains(t, metrics, "soroban_rpc_preflight_pool_request_ledger_get_duration_seconds_count{status=\"ok\",type=\"db\"} 3")
	require.Contains(t, metrics, "soroban_rpc_preflight_pool_request_ledger_get_duration_seconds_count{status=\"ok\",type=\"all\"} 3")
	require.Contains(t, metrics, "soroban_rpc_preflight_pool_request_ledger_entries_fetched_sum 60")
}

func TestSimulateTransactionError(t *testing.T) {
	test := NewTest(t, nil)

	ch := jhttp.NewChannel(test.sorobanRPCURL(), nil)
	client := jrpc2.NewClient(ch, nil)

	sourceAccount := keypair.Root(StandaloneNetworkPassphrase).Address()
	invokeHostOp := createInvokeHostOperation(sourceAccount, xdr.Hash{}, "noMethod")
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
	params := txnbuild.TransactionParams{
		SourceAccount: &txnbuild.SimpleAccount{
			AccountID: keypair.Root(StandaloneNetworkPassphrase).Address(),
			Sequence:  0,
		},
		IncrementSequenceNum: false,
		Operations:           []txnbuild.Operation{invokeHostOp},
		BaseFee:              txnbuild.MinBaseFee,
		Memo:                 nil,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	}
	result := simulateTransactionFromTxParams(t, client, params)
	assert.Greater(t, result.LatestLedger, uint32(0))
	assert.Contains(t, result.Error, "MissingValue")
	require.Len(t, result.Events, 1)
	var event xdr.DiagnosticEvent
	require.NoError(t, xdr.SafeUnmarshalBase64(result.Events[0], &event))
}

func TestSimulateTransactionMultipleOperations(t *testing.T) {
	test := NewTest(t, nil)

	ch := jhttp.NewChannel(test.sorobanRPCURL(), nil)
	client := jrpc2.NewClient(ch, nil)

	sourceAccount := keypair.Root(StandaloneNetworkPassphrase).Address()
	contractBinary := getHelloWorldContract(t)
	params := txnbuild.TransactionParams{
		SourceAccount: &txnbuild.SimpleAccount{
			AccountID: keypair.Root(StandaloneNetworkPassphrase).Address(),
			Sequence:  0,
		},
		IncrementSequenceNum: false,
		Operations: []txnbuild.Operation{
			createInstallContractCodeOperation(sourceAccount, contractBinary),
			createCreateContractOperation(sourceAccount, contractBinary),
		},
		BaseFee: txnbuild.MinBaseFee,
		Memo:    nil,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	}

	result := simulateTransactionFromTxParams(t, client, params)
	assert.Equal(
		t,
		methods.SimulateTransactionResponse{
			Error: "Transaction contains more than one operation",
		},
		result,
	)
}

func TestSimulateTransactionWithoutInvokeHostFunction(t *testing.T) {
	test := NewTest(t, nil)

	ch := jhttp.NewChannel(test.sorobanRPCURL(), nil)
	client := jrpc2.NewClient(ch, nil)

	params := txnbuild.TransactionParams{
		SourceAccount: &txnbuild.SimpleAccount{
			AccountID: keypair.Root(StandaloneNetworkPassphrase).Address(),
			Sequence:  0,
		},
		IncrementSequenceNum: false,
		Operations: []txnbuild.Operation{
			&txnbuild.BumpSequence{BumpTo: 1},
		},
		BaseFee: txnbuild.MinBaseFee,
		Memo:    nil,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	}
	result := simulateTransactionFromTxParams(t, client, params)
	assert.Equal(
		t,
		methods.SimulateTransactionResponse{
			Error: "Transaction contains unsupported operation type: OperationTypeBumpSequence",
		},
		result,
	)
}

func TestSimulateTransactionUnmarshalError(t *testing.T) {
	test := NewTest(t, nil)

	ch := jhttp.NewChannel(test.sorobanRPCURL(), nil)
	client := jrpc2.NewClient(ch, nil)

	request := methods.SimulateTransactionRequest{Transaction: "invalid"}
	var result methods.SimulateTransactionResponse
	err := client.CallResult(context.Background(), "simulateTransaction", request, &result)
	assert.NoError(t, err)
	assert.Equal(
		t,
		"Could not unmarshal transaction",
		result.Error,
	)
}

func TestSimulateTransactionExtendAndRestoreFootprint(t *testing.T) {
	test := NewTest(t, nil)

	ch := jhttp.NewChannel(test.sorobanRPCURL(), nil)
	client := jrpc2.NewClient(ch, nil)

	sourceAccount := keypair.Root(StandaloneNetworkPassphrase)
	address := sourceAccount.Address()
	account := txnbuild.NewSimpleAccount(address, 0)

	helloWorldContract := getHelloWorldContract(t)

	params := preflightTransactionParams(t, client, txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{
			createInstallContractCodeOperation(account.AccountID, helloWorldContract),
		},
		BaseFee: txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	})
	tx, err := txnbuild.NewTransaction(params)
	assert.NoError(t, err)
	sendSuccessfulTransaction(t, client, sourceAccount, tx)

	params = preflightTransactionParams(t, client, txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{
			createCreateContractOperation(address, helloWorldContract),
		},
		BaseFee: txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	})
	tx, err = txnbuild.NewTransaction(params)
	assert.NoError(t, err)
	sendSuccessfulTransaction(t, client, sourceAccount, tx)

	contractID := getContractID(t, address, testSalt, StandaloneNetworkPassphrase)
	invokeIncPresistentEntryParams := txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{
			createInvokeHostOperation(
				address,
				contractID,
				"inc",
			),
		},
		BaseFee: txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	}
	params = preflightTransactionParams(t, client, invokeIncPresistentEntryParams)
	tx, err = txnbuild.NewTransaction(params)
	assert.NoError(t, err)
	sendSuccessfulTransaction(t, client, sourceAccount, tx)

	// get the counter ledger entry TTL
	key := getCounterLedgerKey(contractID)

	keyB64, err := xdr.MarshalBase64(key)
	require.NoError(t, err)
	getLedgerEntryrequest := methods.GetLedgerEntryRequest{
		Key: keyB64,
	}
	var getLedgerEntryResult methods.GetLedgerEntryResponse
	err = client.CallResult(context.Background(), "getLedgerEntry", getLedgerEntryrequest, &getLedgerEntryResult)
	assert.NoError(t, err)

	var entry xdr.LedgerEntryData
	assert.NoError(t, xdr.SafeUnmarshalBase64(getLedgerEntryResult.XDR, &entry))
	assert.Equal(t, xdr.LedgerEntryTypeContractData, entry.Type)
	require.NotNil(t, getLedgerEntryResult.LiveUntilLedgerSeq)

	initialLiveUntil := *getLedgerEntryResult.LiveUntilLedgerSeq

	// Extend the initial TTL
	params = preflightTransactionParams(t, client, txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{
			&txnbuild.ExtendFootprintTtl{
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
		},
		BaseFee: txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	})
	tx, err = txnbuild.NewTransaction(params)
	assert.NoError(t, err)
	sendSuccessfulTransaction(t, client, sourceAccount, tx)

	err = client.CallResult(context.Background(), "getLedgerEntry", getLedgerEntryrequest, &getLedgerEntryResult)
	assert.NoError(t, err)
	assert.NoError(t, xdr.SafeUnmarshalBase64(getLedgerEntryResult.XDR, &entry))
	assert.Equal(t, xdr.LedgerEntryTypeContractData, entry.Type)
	require.NotNil(t, getLedgerEntryResult.LiveUntilLedgerSeq)
	newLiveUntilSeq := *getLedgerEntryResult.LiveUntilLedgerSeq
	assert.Greater(t, newLiveUntilSeq, initialLiveUntil)

	// Wait until it is not live anymore
	waitUntilLedgerEntryTTL(t, client, key)

	// and restore it
	params = preflightTransactionParams(t, client, txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{
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
		},
		BaseFee: txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	})
	tx, err = txnbuild.NewTransaction(params)
	assert.NoError(t, err)
	sendSuccessfulTransaction(t, client, sourceAccount, tx)

	// Wait for TTL again and check the pre-restore field when trying to exec the contract again
	waitUntilLedgerEntryTTL(t, client, key)

	simulationResult := simulateTransactionFromTxParams(t, client, invokeIncPresistentEntryParams)
	require.NotNil(t, simulationResult.RestorePreamble)
	assert.NotZero(t, simulationResult.RestorePreamble)

	params = preflightTransactionParamsLocally(t,
		txnbuild.TransactionParams{
			SourceAccount:        &account,
			IncrementSequenceNum: true,
			Operations: []txnbuild.Operation{
				&txnbuild.RestoreFootprint{},
			},
			BaseFee: txnbuild.MinBaseFee,
			Preconditions: txnbuild.Preconditions{
				TimeBounds: txnbuild.NewInfiniteTimeout(),
			},
		},
		methods.SimulateTransactionResponse{
			TransactionData: simulationResult.RestorePreamble.TransactionData,
			MinResourceFee:  simulationResult.RestorePreamble.MinResourceFee,
		},
	)
	tx, err = txnbuild.NewTransaction(params)
	assert.NoError(t, err)
	sendSuccessfulTransaction(t, client, sourceAccount, tx)

	// Finally, we should be able to send the inc host function invocation now that we
	// have pre-restored the entries
	params = preflightTransactionParamsLocally(t, invokeIncPresistentEntryParams, simulationResult)
	tx, err = txnbuild.NewTransaction(params)
	assert.NoError(t, err)
	sendSuccessfulTransaction(t, client, sourceAccount, tx)
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

func waitUntilLedgerEntryTTL(t *testing.T, client *jrpc2.Client, ledgerKey xdr.LedgerKey) {
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
		require.NoError(t, xdr.SafeUnmarshalBase64(result.Entries[0].XDR, &entry))
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
	test := NewTest(t, nil)

	ch := jhttp.NewChannel(test.sorobanRPCURL(), nil)
	client := jrpc2.NewClient(ch, nil)

	sourceAccount := keypair.Root(StandaloneNetworkPassphrase)
	address := sourceAccount.Address()
	account := txnbuild.NewSimpleAccount(address, 0)

	helloWorldContract := getHelloWorldContract(t)

	params := preflightTransactionParams(t, client, txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{
			createInstallContractCodeOperation(account.AccountID, helloWorldContract),
		},
		BaseFee: txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	})

	tx, err := txnbuild.NewTransaction(params)
	require.NoError(t, err)
	sendSuccessfulTransaction(t, client, sourceAccount, tx)

	params = preflightTransactionParams(t, client, txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{
			createCreateContractOperation(address, helloWorldContract),
		},
		BaseFee: txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	})

	tx, err = txnbuild.NewTransaction(params)
	require.NoError(t, err)
	sendSuccessfulTransaction(t, client, sourceAccount, tx)

	contractID := getContractID(t, address, testSalt, StandaloneNetworkPassphrase)
	authAddrArg := "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H"
	tx, err = txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{
			&txnbuild.CreateAccount{
				Destination:   authAddrArg,
				Amount:        "100000",
				SourceAccount: address,
			},
		},
		BaseFee: txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	})
	require.NoError(t, err)
	sendSuccessfulTransaction(t, client, sourceAccount, tx)
	low := xdr.Uint64(1500)
	high := xdr.Uint64(10000)
	params = txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: false,
		Operations: []txnbuild.Operation{
			createInvokeHostOperation(
				address,
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
		},
		BaseFee: txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	}
	tx, err = txnbuild.NewTransaction(params)

	require.NoError(t, err)

	txB64, err := tx.Base64()
	require.NoError(t, err)

	request := methods.SimulateTransactionRequest{Transaction: txB64}
	var response methods.SimulateTransactionResponse
	err = client.CallResult(context.Background(), "simulateTransaction", request, &response)
	require.NoError(t, err)
	require.Empty(t, response.Error)

	// check the result
	require.Len(t, response.Results, 1)
	var obtainedResult xdr.ScVal
	err = xdr.SafeUnmarshalBase64(response.Results[0].XDR, &obtainedResult)
	require.NoError(t, err)
	require.Equal(t, xdr.ScValTypeScvU64, obtainedResult.Type)
	require.LessOrEqual(t, uint64(*obtainedResult.U64), uint64(high))
	require.GreaterOrEqual(t, uint64(*obtainedResult.U64), uint64(low))
}

func TestSimulateSystemEvent(t *testing.T) {
	test := NewTest(t, nil)

	ch := jhttp.NewChannel(test.sorobanRPCURL(), nil)
	client := jrpc2.NewClient(ch, nil)

	sourceAccount := keypair.Root(StandaloneNetworkPassphrase)
	address := sourceAccount.Address()
	account := txnbuild.NewSimpleAccount(address, 0)

	helloWorldContract := getHelloWorldContract(t)

	params := preflightTransactionParams(t, client, txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{
			createInstallContractCodeOperation(account.AccountID, helloWorldContract),
		},
		BaseFee: txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	})

	tx, err := txnbuild.NewTransaction(params)
	require.NoError(t, err)
	sendSuccessfulTransaction(t, client, sourceAccount, tx)

	params = preflightTransactionParams(t, client, txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{
			createCreateContractOperation(address, helloWorldContract),
		},
		BaseFee: txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	})

	tx, err = txnbuild.NewTransaction(params)
	require.NoError(t, err)
	sendSuccessfulTransaction(t, client, sourceAccount, tx)

	contractID := getContractID(t, address, testSalt, StandaloneNetworkPassphrase)
	authAddrArg := "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H"
	tx, err = txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{
			&txnbuild.CreateAccount{
				Destination:   authAddrArg,
				Amount:        "100000",
				SourceAccount: address,
			},
		},
		BaseFee: txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	})
	require.NoError(t, err)
	sendSuccessfulTransaction(t, client, sourceAccount, tx)

	contractHash := sha256.Sum256(helloWorldContract)
	byteSlice := xdr.ScBytes(contractHash[:])

	params = txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: false,
		Operations: []txnbuild.Operation{
			createInvokeHostOperation(
				address,
				contractID,
				"upgrade_contract",
				xdr.ScVal{
					Type:  xdr.ScValTypeScvBytes,
					Bytes: &byteSlice,
				},
			),
		},
		BaseFee: txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	}
	tx, err = txnbuild.NewTransaction(params)

	require.NoError(t, err)

	txB64, err := tx.Base64()
	require.NoError(t, err)

	request := methods.SimulateTransactionRequest{Transaction: txB64}
	var response methods.SimulateTransactionResponse
	err = client.CallResult(context.Background(), "simulateTransaction", request, &response)
	require.NoError(t, err)
	require.Empty(t, response.Error)

	// check the result
	require.Len(t, response.Results, 1)
	var obtainedResult xdr.ScVal
	err = xdr.SafeUnmarshalBase64(response.Results[0].XDR, &obtainedResult)
	require.NoError(t, err)

	var transactionData xdr.SorobanTransactionData
	err = xdr.SafeUnmarshalBase64(response.TransactionData, &transactionData)
	require.NoError(t, err)
	assert.InDelta(t, 6856, uint32(transactionData.Resources.ReadBytes), 200)

	// the resulting fee is derived from compute factors and a default padding is applied to instructions by preflight
	// for test purposes, the most deterministic way to assert the resulting fee is expected value in test scope, is to capture
	// the resulting fee from current preflight output and re-plug it in here, rather than try to re-implement the cost-model algo
	// in the test.
	assert.InDelta(t, 85360, int64(transactionData.ResourceFee), 5000)
	assert.InDelta(t, 104, uint32(transactionData.Resources.WriteBytes), 15)
	require.GreaterOrEqual(t, len(response.Events), 3)
}
